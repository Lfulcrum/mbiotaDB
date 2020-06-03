# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 14:46:31 2019

@author: William
"""

# Standard library imports
import os
import os.path
import re
import csv
import time
from collections import defaultdict, OrderedDict
from contextlib import contextmanager

# Third-party imports
import biom
from Bio import Phylo
from sqlalchemy import create_engine, exists, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.engine.url import URL

# Local application imports
import model
from model import Experiment, Sample, Preparation
from model import Processing, Lineage, Count, SequencingVariant
from . import session_scope


# TODO: Better to implement as a namedtuple? Are we going to add more
# functionality? Perhaps we will need to use __slots__ to make this a
# lightweight object? If we attach many thousands of these objects to
# a Workflow, memory might become an important consideration!
class CountElement:
    """Class representing a single element in a BIOM count table."""

    __slots__ = 'count', 'lineage', 'seq_var'
    
    def __init__(self, count, lineage, seq_var=None):
        self.count = count
        self.lineage = lineage
        self.seq_var = seq_var


def get_dirs(path):
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_dir():
                yield entry       


# TODO Reimplement using os.scandir()
# As the experiment identifier is often NOT recorded in the biom file, we shall
# have to use the parent directory of the file to link the counts to a sample.
# TODO We need to be certain about the context in which the sample identifier
# is unique. Are sample identifiers unique to an experiment? Are they unique
# to a sequencing run? i.e. can we find the same sample identifier for different
# sequencing runs?
def get_prep_filenames(path, exclude=[r'qiime', r'prep_data']):
    prep_re = re.compile(r'prep')
    all_prep_files = (file for file in os.listdir(path)
                      if prep_re.search(file))
    filtered_files = filter(lambda x: not any(re.search(regex, x) 
                                              for regex in exclude),
                            all_prep_files)
    return list(filtered_files)


# TODO Write some code to first check whether the biom file contains the 
# experiment identifier as metadata.
def get_biom_filenames(path, exclude=[r'.*all.biom']):
    all_biom_files = (file for file in os.listdir(path) 
                     if file.endswith('.biom'))
    filtered_files = filter(lambda x: not any(re.search(regex, x)
                                              for regex in exclude),
                            all_biom_files)
    return list(filtered_files)


def get_proc_id_from_biom(biom_path):
    biom_file = os.path.basename(biom_path)
    match = re.match('(.*?)_', biom_file)
    if match:
        return match.group(1)
    return None


# Returns a dictionary of non-zero CountElements indexed on sample_id. 
# Then we will connect this dictionary to the appropriate workflow (that 
# produced the BIOM file that we are parsing), so that we can easily connect
# a sample to a count during the formation of a CountFact.
# TODO Remove session argument from all calls! I am not convinced that session 
# should be used in this way to search for existing lineages.
def get_counts(biom_path, session):
    """Parse counts, lineages and seq variants into CountElements.
    
    The function will first attempt to read lineages from the given BIOM file.
    If the lineages are not available in the metadata of this file, it will
    parse a corresponding tree file (in the same directory) into lineages
    before connecting the CountElement to the appropriate lineage. 
    
    Parameters
    ----------
    biom_path : str
        Path to the BIOM file from which CountElements will be parsed.
    session : creator.Session
        Session used to search database for a matching lineages.
    
    Returns
    -------
    dict of CountElement
        Keys are sample ids, values are CountElement objects.
    """
    table = biom.load_table(biom_path)
    biom_file = os.path.basename(biom_path)
    path = os.path.dirname(biom_path)
    tree_file = get_tree_filename(path, biom_file)
    tree = None
    taxa = None
    if tree_file:
        tree_path = os.path.join(path, tree_file)
        tree = Phylo.read(tree_path, 'newick')
        taxa = get_tree_taxa(tree)
    lineage_map = get_lineage_map(table, session, tree, taxa)
    counts = defaultdict(list)
    for obs_id, samp_id in table.nonzero():
        lineage = lineage_map[obs_id]
        seq_var = SequencingVariant(sequencing_variant=samp_id)\
                    if re.match('[ATGCN]+', samp_id) else None
        count = table.get_value_by_ids(obs_id, samp_id)
        count_elem = CountElement(count=count, lineage=lineage, seq_var=seq_var)
        counts[samp_id].append(count_elem)
    return counts


def get_sample(samp_id, study_id, session):
    try:
        sample, experiment = session.query(Sample, Experiment)\
                                .filter(Sample.sample_id == samp_id)\
                                .filter(Experiment.study_id == study_id)\
                                .one()
        return sample
    except NoResultFound:
        # TODO A sample should always be present in database, before this 
        # method is called, but for testing purposes, we create an empty
        # Sample object.
        return Sample()


def get_lineage(table, obs_id, session, tree=None, taxa=None):
    try:
        lineage = table.metadata(obs_id, axis='observation')['taxonomy']
    except TypeError:
        lineage = get_lineage_from_tree(obs_id, tree, taxa)
    lineage = dict(zip(['kingdom_', 'phylum_', 'class_', 
                        'order_', 'family_', 'genus_', 
                        'species_'], lineage))
    lineage = Lineage(**lineage)
    return lineage


def get_lineage_map(table, session, tree, taxa):
    lineage_map = {}
    for obs_id in table.ids(axis='observation'):
        lineage = get_lineage(table, obs_id, session, tree, taxa)
        lineage_map[obs_id] = lineage
    return lineage_map


def get_tree_taxa(tree):
    taxa = tree.find_elements(name=r'.*__.*')
    taxa = list(taxa)
    return taxa


# TODO Check whether all taxon names contain the chars [\w\d_-]. I notice that
# some of the lineages extracted as metadata from biom files have a form x__[name].
# What is the significance of the brakets in this name? Should we include [] in
# the regular expression for searching tree taxa names.
# Note: We are currently, discarding any distance/confidence information 
# available in tree - could this be useful?
# TODO: Any way to speed up this function?
taxon_name_re = re.compile(r'([\d.]+:)?(?P<name>\w__[\w\d_-]*)')
def get_lineage_from_tree(obs_id, tree, taxa):
    seq_var = tree.find_any(name=obs_id)
    taxa_prefix = ['k__', 'p__', 'c__', 'o__', 'f__', 'g__', 's__']
    lineage = OrderedDict(zip(taxa_prefix, taxa_prefix))
    # If sequence variant is not found in tree:
    if seq_var is None:
        return [None]*len(taxa_prefix)
    for taxon in taxa:
        if taxon.is_parent_of(seq_var):
            taxon_name_matches = taxon_name_re.finditer(taxon.name)
            for taxon_name_match in taxon_name_matches:
                try:
                    taxon_name = taxon_name_match.group('name')
                except AttributeError:
                    raise Exception('Encountered unrecognizable taxon name.')    
                key = taxon_name[:3]
                lineage[key] = taxon_name
    return list(lineage.values())


# TODO implement filter for exclude parameter
def get_tree_filename(path, biom_file, exclude=[]):
    biom_id_re = re.compile(r'(.*?)_')
    biom_id_match = biom_id_re.match(biom_file)
    if biom_id_match:
        biom_id = biom_id_match.group(1)
    else:
        raise Exception('No biom_id could be detected in the given biom filename.')
    with os.scandir(path) as entries:
        for entry in entries:
            if (entry.is_file() and 
                entry.name.startswith(biom_id) and 
                entry.name.endswith('.tre')):
                return entry.name
    return None


# TODO don't think it's worth having this function
def read_tree(tree, tree_format='newick'):
    tree = Phylo.read(tree, 'newick')
    return tree

if __name__ == '__main__':
    pass
