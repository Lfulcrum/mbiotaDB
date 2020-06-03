# -*- coding: utf-8 -*-
"""
Created on Mon May 18 13:47:00 2020

The user would like to parse a BIOM file (possibly with the assistance of a
phylogenetic tree file to extract lineage information) into a number of
objects that can then be serialized into (or substituted for SQLAlchemy objects).

@author: William
"""

# Standard library imports
import re
import time
from collections import OrderedDict, namedtuple

# Third-party imports
import biom
from Bio import Phylo

# Local application imports
import model.Lineage, model.SequencingVariant, model.Count

# class Count:
#     def __init__(self, sample_id, observation_id, count, seq_var=None, lineage=None):
#         self.sample_id = sample_id
#         self.sample_id = observation_id
#         self.count = count
#         self.seq_var = seq_var
#         self.lineage = lineage

Count = namedtuple('Count',
                   ('samp_id', 'obs_id', 'count', 'seq_var', 'lineage'))

taxon_name_re = re.compile(r'([\d.]+:)?'  # branch length
                           r'(?P<name>(?P<prefix>\w__)\[?[\w\d_-]*\]?)')
seq_var_re = re.compile('[AGCTN]+')


def get_tree(file, tree_format='newick'):
    tree = Phylo.read(file, 'newick')
    return tree

def find_clades_by_name(tree, terminal=True, contains=[], contains_all=False,
                        pattern=None):
    """Find clades by their name and whether or not they are terminal clades.

    Parameters
    ----------
    terminal : bool
        If True, only the terminal nodes of the `tree` are searched.
        If False, only the non-terminal nodes are searched.
        If None, all nodes are searched.
    contains : iterable of str
        Collection of strings whose presence will be checked in a clade name.
        If `contains` is given, then `pattern` should not be given.
    contains_all: bool
        If True, all str specified in `contains` must be present in the clade
        name.
        If False, any str specified in `contains` gives a name of interest.
    pattern : str
        Regular expression pattern to match the clade name.
        If `pattern` is given then `contains` should not be given.

    Returns
    -------
    generator expression
        Iterating over the returned generator will yield clades whose names
        match the given search criteria.

    Notes
    -----
    Slightly faster implementation than BioPython's Phylo package find_clades
    search functionality.
    """
    if contains:
        logic = all if contains_all else any
        return (clade for clade in tree.find_clades(terminal=terminal) if
                clade.name and
                logic(x in clade.name for x in contains))
    elif pattern:
        pattern = re.compile(pattern)
        return (clade for clade in tree.find_clades(terminal=terminal) if
                clade.name and
                pattern.match(clade.name))
    else:
        return (clade for clade in tree.find_clades(terminal=terminal))


def all_parents(tree):
    parents = {}
    for clade in tree.find_clades(order='level'):
        for child in clade:
            parents[child] = clade
    return parents

def get_path(clade, parents):
    path = []
    while True:
        try:
            path.append(clade)
            clade = parents[clade]
        except KeyError:
            break
    return path

# TODO: I had thought about implementing the lineage with an OrderedDict
# mapping prefixes to taxonomic names. Initialize this dict with the supplied
# prefixes (if given, otherwise use empty list, as currently used). Then only
# insert names as values in this OrderedDict were the extracted prefix (using
# pattern) matches a key in the OrderedDict.
# TODO: Could generalize this function for a path of strings too (rather than
# just Clade objects).
def get_lineage_from_path(path, pattern, prefixes=None, all_levels=True):
    """Construct a lineage from a collection of clades (path).

    Parameters
    ----------
    path : list of Phylo Clade objects
        An ordered collection of clades, such that the first element is a clade
        whose lineage is of interest to us and the last element is the root of
        the tree in which this clade is found (or some arbitrary clade between
        the clade of interest and root).
    pattern : str or re Pattern object
        The pattern can contain no groups or two groups. If it contains no
        groups or one group, the whole pattern will be used to search clade
        names for taxonomic names of interest. If it contains two groups, the
        first group is interpreted to be a prefix and the second as the taxon
        name of interest. If the pattern contains named groups (using Python's
        named group notation: (?P<name>...)), with names 'prefix' and 'index',
        then any number of other groups are valid in the pattern and will be
        ignored.
    prefixes : list of str
        The prefixes that will be used to fill in levels not found if the
        `all_levels` is True.
    all_levels : bool
        If True and if `prefixes` is given, then all taxonomic levels not
        found in the `path` will be filled in with those found in prefixes.

    Returns
    -------
    lineage : list of str
    """
    lineage = []
    if not path:
        return None
    for clade in path:
        if not clade.name:
            continue
        try:
            matches = pattern.finditer(clade.name)
        except AttributeError:
            matches = re.finditer(pattern, clade.name)
        for match in matches:
            try:
                prefix = match.group('prefix')
                taxon = match.group('name')
            except IndexError:
                pass
            else:
                lineage.insert(0, taxon)
                continue
            if match.re.groups > 2:
                raise ValueError('Given `pattern` contains more than 2 groups')
            try:
                prefix = match.group(1)
                taxon = match.group(2)
            except IndexError:
                pass
            else:
                lineage.insert(0, taxon)
                continue
            taxon = match.group()
            lineage.insert(0, taxon)
    prefixes = list(prefixes)
    if prefixes and all_levels:
        full_lineage = prefixes.copy()
        full_lineage[:len(lineage)] = lineage
        return full_lineage
    return lineage


class ParsedTree:
    def __init__(self, file, tree_format, taxon_pattern=None,
                 lineage_prefixes=None):
        self.tree = get_tree(file, tree_format)
        self.parents = all_parents(self.tree)
        self.taxon_pattern = taxon_pattern
        self.lineage_prefixes = lineage_prefixes
        self.index_clades = {}

    def get_lineage(self, clade, index_only=False):
        clade = self.get_clade(clade, index_only)
        if not clade:
            raise ValueError('The given clade cannot be found in this '
                             'ParsedTree.')
        path = get_path(clade, self.parents)
        return get_lineage_from_path(path, pattern=self.taxon_pattern,
                                     prefixes=self.lineage_prefixes,
                                     all_levels=True)

    def set_index_clades(self, terminal=True, contains=[], contains_all=False,
                         pattern=None):
        clades = find_clades_by_name(self.tree, terminal, contains,
                                     contains_all, pattern)
        self.index_clades = {clade.name: clade for clade in clades}

    def get_clade(self, name, index_only=False):
        if isinstance(name, Phylo.BaseTree.Clade):
            return name
        name = str(name)
        try:
            return self.index_clades[name]
        except KeyError:
            if index_only:
                return None
        return self.tree.find_any(name=name)


def get_table(file):
    return biom.load_table(file)

def parse_lineage(table, otu_id, parsed_tree=None, index_only=True):
    try:
        return table.metadata(otu_id, axis='observation')['taxonomy']
    except TypeError:
        pass
    if parsed_tree:
        try:
            return parsed_tree.get_lineage(otu_id, index_only=index_only)
        except ValueError:
            return None
    return None

def has_lineage_data(table):
    obs_metadata = table.metadata(axis='observation')
    if obs_metadata:
        return any(obs.get('taxonomy') for obs in obs_metadata)
    return False

def parse_sequencing_variant(string):
    try:
        return seq_var_re.match(obs_id).group()
    except AttributeError:
        return None

def get_non_zero_counts(table):
    return table.nonzero()

def parse_counts(biom_file, tree_file=None):
    counts = []
    table = biom.load_table(biom_file)
    if tree_file:
        tree = ParsedTree(tree_file, 'newick', taxon_name_re,
                          lineage_prefixes=['k__', 'p__', 'c__', 'o__', 'f__',
                                            'g__', 's__'])
        tree.set_index_clades(contains=['A','T','G','C'])
    for obs_id, samp_id in table.nonzero():
        lineage = parse_lineage(table, obs_id)
        if not lineage and tree:
            lineage = parse_lineage(table, obs_id, parsed_tree=tree,
                                    index_only=True)
        seq_var = parse_sequencing_variant(obs_id)
        count = table.get_value_by_ids(obs_id, samp_id)
        counts.append(Count(samp_id=samp_id,
                            obs_id=obs_id,
                            count=count,
                            seq_var=seq_var,
                            lineage=lineage))
    return counts


def get_proc_id_from_biom(biom_path):
    biom_file = os.path.basename(biom_path)
    match = re.match('(.*?)_', biom_file)
    if match:
        return match.group(1)
    return None


# The following code becomes SQLAlchemy/RDBMS-specific:

def get_lineage_from_count(count):
    return Lineage(*count.lineage)

def get_seq_var_from_count(count):
    return SequencingVariant(count.seq_var)

# TODO Should proc_id be included in the Count namedtuple/object?
# More flexibility about where proc_id comes from if we don't. Otherwise, the
# proc_id must be obtained from the BIOM file, which is what we intend to do
# for BIOM files obtained from Qiita. Also all counts would have the same
# proc_id so it seems a bit pointless to have it as an attribute.
def get_count_fact(count, proc_id, workflows, samples):
    workflow = workflows[proc_id]
    sample = samples[count.samp_id]
    attributes = {
        'lineage': get_lineage_from_count(count),
        'seq_var': get_seq_var_from_count(count),
        'workflow': workflow,
        'prep': workflow.prep,
        'sample': sample,
        'sample_site': sample.sample_site,
        'sample_time': sample.sample_time,
        'subject': sample.subject,
        'experiment': subject.experiment
    }
    return model.Count(**attributes)


# This function demostrates how we might be able to check the database for
# an existing tuple before we insert the object into the database.
def get_lineage_from_db(session, lineage):
    return session.query(Lineage)\
                  .filter_by(kingdom_=lineage.kingdom_,
                             phylum_=lineage.phylum_,
                             class_=lineage.class_,
                             order_=lineage.order_,
                             family_=lineage.family_,
                             genus_=lineage.genus_,
                             species_=lineage.species_)\
                  .one_or_none()


def get_seq_var_from_db(session, seq_var):
    return session.query(SequencingVariant)\
                  .filter_by(sequencing_variant=seq_var.sequencing_variant)\
                  .one_or_none()


# TODO: More general database query function, should work for all the
# cases (specific queries no longer required, but must supply list of
# relevant attrs each time - annoying for user, so we should create
# wrappers)...
# We should also specify the attrs in only one place.
# TODO: I feel like there should be an automatic way to get the names of
# all columns except the identity column (we will always be querying by this
# for the purpose of checking for a duplicate).
def get_from_db(session, obj, attrs):
    attr_dict = dict((attr, getattr(obj, attr)) for attr in attrs)
    return session.query(obj.__class__)\
                  .filter_by(**attr_dict)\
                  .one_or_none()

sample_site_attrs = ['uberon_habitat_term', 'uberon_site_term',
                     'uberon_product_term', 'env_biom_term',
                     'env_feature_term']

def get_sample_site_from_db(session, sample_site):
    return get_from_db(session, sample_site, sample_site_attrs)

seq_instr_attrs = ['platform', 'model', 'name']

def get_seq_instr_from_db(session, seq_instr):
    return get_from_db(session, seq_instr, seq_instr_attrs)

# TODO: Should we put all checks for existing tuples in this one function?
# Makes sense to put all similar functionality in one place and avoids any
# potential complication of objects entering detached state.
def add_count_facts(session, counts, proc_id, workflows, samples):
    for count in counts:
        count_fact = get_count_fact(count, proc_id, workflows, samples)
        lineage = get_lineage_from_db(session, count_fact.lineage)
        seq_var = get_lineage_from_db(session, count_fact.seq_var)
        sample_site = get_sample_site_from_db(session, count_fact.sample_site)
        sample_time = get_sample_time_from_db(session, count_fact.sample_time)
        seq_instr = get_seq_instr_from_db(session,
                                          count_fact.prep.seq_instrument)
        # Add more checks here...
        if lineage:
            count_fact.lineage = lineage
        if seq_var:
            count_fact.seq_var = seq_var
        if sample_site:
            count_fact.sample_site = sample_site
        if sample_time:
            count_fact.sample_time = sample_time
        if seq_instr:
            count_fact.seq_instrument = seq_instr
        session.add(count_fact)
        # Note that unless we set the session's autoflush to False, the queries
        # in this loop will flush the previously added count_fact, meaning
        # that the query can find times instruments etc. that have only just
        # been added (in a previous count fact). I wonder how this interacts
        # with the fact that we dropped duplicates earlier? As it stands, I
        # don't think there's any advantage to dropping duplicates before this
        # querying for duplicates, since the query will take place anyway...
        # TODO: Should we commit inside or outside this function? Usually,
        # commits are done outside.


if __name__ == '__main__':
    tree_file = r'../data/test_data/experiments/101/56522_insertion_tree.relabelled.tre'
    biom_file = r'../data/test_data/experiments/101/56522_reference-hit.biom'
    biom_file_with_metadata = r'../data/test_data/experiments/101/44767_otu_table.biom'

    # table = get_table(biom_file)
    # start = time.time()
    # tree = ParsedTree(tree_file, 'newick', taxon_name_re,
    #                   lineage_prefixes=['k__', 'p__', 'c__', 'o__', 'f__',
    #                                     'g__', 's__'])
    # tree.set_index_clades(contains=['A','T','G','C'])
    # lineages = {}
    # for otu_id in table.ids('observation'):
    #     try:
    #         lineage = tree.get_lineage(otu_id, index_only=True)
    #         lineages[otu_id] = lineage
    #     except ValueError:
    #         pass
    #         # print(f'OTU ID: {out_id} has no clade in tree.')
    # end = time.time()
    # print('Took {}s to get all lineages from BIOM.'.format(end-start))

    start = time.time()
    counts = parse_counts(biom_file, tree_file)
    end = time.time()
    print('Took {}s to parse counts from BIOM.'.format(end-start))

    # start = time.time()
    # tree = get_tree(tree_file)
    # end = time.time()
    # print('Took {}s to parse tree.'.format(end-start))

    # start = time.time()
    # asv_clades = {clade.name: clade for clade
    #               in find_clades_by_name(tree, contains=['A','T','G','C'])}
    # end = time.time()
    # print('Took {}s to get all ASV clades.'.format(end-start))

    # LATEST IMPLEMENTATION:
    # lineages = []
    # start = time.time()
    # parents = all_parents(tree)
    # mid = time.time()
    # for clade in asv_clades.values():
    #     path = get_path(clade, parents)
    #     lineages.append(get_lineage_from_path(path, taxon_name_re,
    #                                           prefixes=['k__', 'p__', 'c__',
    #                                                     'o__', 'f__', 'g__',
    #                                                     's__'],
    #                                           all_levels=True))
    # end = time.time()
    # print('LATEST IMPLEMENTATION:')
    # print(f'Took {end-start}s to get all lineages from tree.')
    # print(f'Took {mid-start}s to get all parents dict from tree.')
    # print(f'Took {end-mid}s to get all paths and lineages from paths.')
    # - Best implementation: 5.77s! This used a
    # recommendation in the BioPython cookbook to first
    # get all parents of all nodes in tree and use a
    # dictionary lookup to determine the path.