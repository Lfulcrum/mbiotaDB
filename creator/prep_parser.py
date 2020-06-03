# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 15:23:50 2019

@author: William
"""

# Standard library imports
import re
import json
from dateutil import parser
from collections import defaultdict

# Third-party imports
import networkx as nx

# Local application imports
from model import Preparation, SeqInstrument, Processing, Workflow
from creator.sample_parser import generate_rows, get_string, get_valid_string

# Global regular expressions
re_missing = re.compile(r'^$|(missing:)? *(not provided|not collected|'
                        r'restricted access|na|not applicable|none|'
                        r'unspecified|labcontrol test)',
                        re.I)
re_invalid_numeric_chars = re.compile(r'[^.\d]')


re_forward_primer = re.compile(r'FWD:([ACTG]*)')
re_reverse_primer = re.compile(r'REV:([ACTG]*)')


# Extractor functions (used by wrapper functions)
def extract_forward_primer(col, value):
    fwd_primer_match = re_forward_primer.search(value)
    if fwd_primer_match:
        return fwd_primer_match.group(1)
    else:
        return None


def extract_reverse_primer(col, value):
    rev_primer_match = re_reverse_primer.search(value)
    if rev_primer_match:
        return rev_primer_match.group(1)
    else:
        return None


def extract_region(col, value):
    if value == '0':
        return None
    else:
        return value


# Functions to get values from a row
def get_seq_date(row, dayfirst_dict):
    """Get the date of sample sequencing from a row in the prep/Qiime metadata file.
    
    Parameters
    ----------
    row : dict
        Dictionary whose keys are column names and values are row values. 
        This sort of dictionary is returned by each iteration over a 
        csv.DictReader.
    dayfirst_dict : dict
        Dictionary whose keys are names of columns containing date/time data
        and values are boolean, indicating whether the dates in that column
        should be interpreted as having a day as the first component (True)
        or a month or year as the first component (False).        
    
    Returns
    -------
    seq_date : datetime.datetime or None
        The date of sample sequencing for a particular row in the prep/Qiime
        metadata file.
    """
    re_interval = re.compile(
            r'((?:\d{1,2}/)?(?:\d{1,2}/)?(?:\d{4}|\d{2}))'  # start date
            r'-'                                            # interval sep
            r'((?:\d{1,2}/)?(?:\d{1,2}/)?(?:\d{4}|\d{2}))'  # end date
    )
    seq_date = None
    for col in ['run_date']:
        try:
            timestamp = row[col].strip()
        except KeyError:
            timestamp = None
        else:
            if re_missing.match(timestamp):
                continue
            try:
                seq_date = parser.parse(timestamp, dayfirst=dayfirst_dict[col]).date()
            except ValueError:
                # Assume the timestamp is an interval
                match = re_interval.search(timestamp)
                if match:
                    # Only use the first date (for simplicity)
                    seq_date = parser.parse(match.group(1), dayfirst=dayfirst_dict[col])
                if not seq_date:
                    raise
    return seq_date


# Other functions to get values from a row, defined using wrapper functions
# from sample_parser.py
get_study_id = get_string('qiita_study_id')
get_sample_id = get_string('#SampleID')
get_qiita_prep_id = get_valid_string(['qiita_prep_id'])
get_instrument_model = get_valid_string(['instrument_model'])
get_instrument_name = get_valid_string(['instrument_name'])
get_platform = get_valid_string(['platform'])
get_seq_centre = get_valid_string(['run_center'])
get_seq_run_name = get_valid_string(['center_project_name'])
get_seq_method = get_valid_string(['sequencing_meth'])
get_target_gene = get_valid_string(['target_gene'])
get_region = get_valid_string(['region'], extractor=extract_region)
get_target_subfragment = get_valid_string(['target_subfragment'])
get_forward_primer = get_valid_string(['pcr_primers'],
                                      extractor=extract_forward_primer)
get_reverse_primer = get_valid_string(['pcr_primers'],
                                      extractor=extract_reverse_primer)


# Functions to parse a rows into SQLAlchemy objects
def parse_preparation(row, dayfirst_dict):
    """Parse a row into a Preparation object.
    
    Parameters
    ----------
    row : dict
        Dictionary whose keys are column names and values are row values. 
        This sort of dictionary is returned by each iteration over a 
        csv.DictReader.
    dayfirst_dict : dict
        Dictionary whose keys are names of columns containing date/time data
        and values are boolean, indicating whether the dates in that column
        should be interpreted as having a day as the first component (True)
        or a month or year as the first component (False).        
    
    Returns
    -------
    model.Preparation
        A Preparation object with attribute values, but for which no 
        relationships to other model objects have yet been established.
    """
    preparation = Preparation()
    preparation.study_id = get_study_id(row)
    preparation.prep_id = get_qiita_prep_id(row)
    preparation.sample_id = get_sample_id(row)
    preparation.seq_date = get_seq_date(row, dayfirst_dict)
    preparation.seq_centre = get_seq_centre(row)
    preparation.seq_run_name = get_seq_run_name(row)
    preparation.fwd_pcr_primer = get_forward_primer(row)
    preparation.rev_pcr_primer = get_reverse_primer(row)
    preparation.target_gene = get_target_gene(row)
    preparation.target_subfragment = get_target_subfragment(row)
    preparation.seq_instrument = parse_seq_instrument(row)
    return preparation


# TODO: Should dayfirst_dict be optional? Is study run date always provided?
# TODO: Prep file 10317_prep_1116_20190627-144743.txt has a line completely
# composed of tabs - how will such a line be processed by our parser? Is there
# any way to easily skip the line?
def parse_preparations(metadata_file, dayfirst_dict=None, index_by=['id']):
    """Parse a preparation metadata file into collections of Preparations.

    Parameters
    ----------
    metadata_file : str
        Path to a preparation metadata file
    index_by : str or interable of str, optional
        Acceptable str values are 'id', 'study' and 'sample'. They control the
        dictionaries returned by this function. 'id' is the only value that
        will provide a dictionary whose values are Preparation objects. In this
        case, the prep ID will be provided as a key. 'study' will return a
        dictionary keyed by study ID, with a list of prep IDs as values
        (describing which Preparations are associated with which study).
        'sample' will return a dictionary keyed by prep ID, with lists of
        sample IDs as values (describing which samples are associated with
        which Preparation).
    dayfirst_dict : dict, optional
        Dictionary whose keys are names of columns containing date/time data
        and values are boolean indicating whether the dates in that column
        should be interpreted as having a day as the first component (True)
        or a month or year as the first component (False).

    Returns
    -------
    dict, list of dict
        A dictionary or list of dictionaries keyed (indexed) by index types
        given in index_by.
    """
    rows = generate_rows(metadata_file)
    preparations = {}
    preparation_ids = {}
    study_preparations = defaultdict(list)
    preparation_samples = defaultdict(list)
    for row in rows:
        preparation = parse_preparation(row, dayfirst_dict)
        preparation_ids[preparation.prep_id] = preparation
        study_preparations[preparation.study_id].append(preparation.prep_id)
        preparation_samples[preparation.prep_id].append(preparation.sample_id)
    preparations['id'] = preparation_ids
    preparations['study'] = study_preparations
    preparations['sample'] = preparation_samples
    try:
        # To avoid iterating over str if str provided as indexed_by arg
        # Note: More pythonic than type-checking
        index_by + ''
        return preparations[index_by]
    except TypeError:
        return [preparations[index] for index in index_by]


def parse_seq_instrument(row):
    """Parse a row into a SeqInstrument object.
    
    Parameters
    ----------
    row : dict
        Dictionary whose keys are column names and values are row values. 
        This sort of dictionary is returned by each iteration over a 
        csv.DictReader.     
    
    Returns
    -------
    model.SeqInstrument
        A SeqInstrument object with attribute values, but for which no 
        relationships to other model objects have yet been established.
    """
    seq_instrument = SeqInstrument()
    seq_instrument.study_id = get_study_id(row)
    seq_instrument.sample_id = get_sample_id(row)
    seq_instrument.platform = get_platform(row)
    seq_instrument.model = get_instrument_model(row)
    seq_instrument.name = get_instrument_name(row)
    return seq_instrument


def parse_processing_parents(processings):
    """Return a dictionary relating each processing identifier to its parent.
    
    Parameters
    ----------
    processings : dict
        A dictionary of processing data, whose keys are processing identifiers
        and values are dictionaries containing corresponding processing data.
        This sort of dictionary is generated by reading the JSON file containing
        processing/artifact metadata derived from the processing network/tree on
        Qiita.
    
    Returns
    -------
    dict
        Dictionary whose keys are processing identifiers and values are the 
        identifiers of a parent processing.
    """
    processing_parents = {}
    for proc_id, proc_data in processings.items():
        if 'input_data' in proc_data:
            parent_id = proc_data['input_data']
            processing_parents[proc_id] = parent_id
        elif 'demultiplexed sequences' in proc_data:
            parent_id = proc_data['demultiplexed sequences']
            processing_parents[proc_id] = parent_id
    return processing_parents


def parse_processings(processings, prep_id):
    """Parse processing data into Processing objects.
    
    Parameters
    ----------
    processings : dict
        A dictionary of processing data, whose keys are processing identifiers
        and values are dictionaries containing corresponding processing data.
        This sort of dictionary is generated by reading the JSON file containing
        processing/artifact metadata derived from the processing network/tree on
        Qiita.
    prep_id : str
        The identifier of the preparation to which processings relates.
    
    Returns
    -------
    dict
        Dictionary whose keys are processing identifiers and values are the 
        corresponding Processing objects. Relationships between Processing
        objects and their parents have been established.
    """
    processing_dict = {}
    for proc_id, proc_data in processings.items():
        json_proc_data = json.dumps(proc_data,
                                    separators=[',', ':'],
                                    allow_nan=False)
        processing = Processing(orig_prep_id=prep_id,
                                orig_proc_id=proc_id,
                                parameter_values=json_proc_data)
        processing_dict[proc_id] = processing
    processing_parents = parse_processing_parents(processings)
    for proc_id, parent_id in processing_parents.items():
        processing_dict[proc_id].parent = processing_dict[parent_id]
    return processing_dict


# TODO Remove prep_id argument? Not currently used
def parse_prep_workflows(processing_parents, processings, prep_id):
    tree = nx.DiGraph()
    tree.add_edges_from(processing_parents.items())
    terminals = [node for node, degree in tree.in_degree if degree == 0]
    roots = [node for node, degree in tree.out_degree if degree == 0]
    paths = []
    prep_workflows = {}
    # The following still won't work if we have two disconnected trees
    # and hence more than one root because the terminals will be a list of
    # terminals for both trees and therefore a path won't exist for some
    # terminals to a root.
    for root in roots:
        for terminal in terminals:
            # Assume one path from terminal to root
            path = next(nx.all_simple_paths(tree, terminal, root))
            paths.append(path)
            workflow_processings = []
            for node in path:
                workflow_processings.append(processings[node])
            # Index workflows by the terminal processing id
            workflow = Workflow(processings=workflow_processings)
            prep_workflows[terminal] = workflow
    return prep_workflows


def parse_workflows(processing_file, index_by='terminal_proc'):
    workflow_views = {}
    prep_workflows_view = {}
    proc_workflows_view = {}
    with open(processing_file) as file:
        json_str = file.read()
        processing_data = json.loads(json_str)
    for preps in processing_data:
        for prep_id, processings in preps.items():
            processing_parents = parse_processing_parents(processings)
            processing_dict = parse_processings(processings, prep_id)
            prep_workflows = parse_prep_workflows(processing_parents,
                                                  processing_dict,
                                                  prep_id)
            prep_workflows_view[prep_id] = list(prep_workflows.values())
            for terminal_proc, workflow in prep_workflows.items():
                proc_workflows_view[terminal_proc] = workflow
    workflow_views['terminal_proc'] = proc_workflows_view
    workflow_views['prep'] = prep_workflows_view
    try:
        index_by + ''
        return workflow_views[index_by]
    except TypeError:
        return [workflow_views[index] for index in index_by]


if __name__ == '__main__':
    pass
