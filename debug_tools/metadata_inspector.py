# -*- coding: utf-8 -*-
"""
Module allowing investigation of metadata attributes from (Qiita) studies.

Created on Fri Jan 17 14:54:06 2020

@author: William
"""

# Standard library imports
import os
import csv
import json
import re
import pandas as pd
from collections import defaultdict, Counter


class InspectorError(Exception):
    pass


class NoMatchingFileError(InspectorError):
    pass


def generate_study_paths(path):
    """Generate absolute paths to directories in the given path."""
    base_path = os.path.dirname(path)
    with os.scandir(path) as files:
        for file in files:
            if file.is_dir():
                yield os.path.join(base_path, file.path)


def create_metadata_filename_regex(metadata, study_id):
    """Create regular expressions for study metadata files.

    Parameters
    ----------
    metadata : str, required
        Type of metadata file ('sample' or 'prep'), found in the study_path,
        from which to extract attributes.
    study_id : str, required
        The identifier of the study to which regular expressions should apply.

    Returns
    -------
    re.Pattern
        Compiled regular expression allowing search for a particular study's
        metadata files.
    """
    if metadata == 'sample':
        regex = re.compile(r'^{}_(?!prep).*'.format(study_id))
    elif metadata == 'prep':
        regex = re.compile(r'^{}_prep_.*?_qiime.*'.format(study_id))
    else:
        raise ValueError('Unknown metadata type. Accepted options are "sample" '
                         'or "prep".')
    return regex


# TODO: Should we change to get_attribute_set? Assume there are no duplicate
# attribute columns. Don't think it's necessary, as user can simply call set
# on the returned list for this behaviour if they wish.
# TODO: Reimplement using os.scandir()?
# TODO: By 'break', we assume there is only one prep file, but there may be
# more! ?
def get_attribute_list(study_path, metadata='sample'):
    """Return a list of attributes found in Qiita study metadata files.

    Parameters
    ----------
    study_path : str, required
        Path of a directory in which a study's metadata files are found. The
        name of the directory should be the study identifier.
    metadata : str, required
        Type of metadata file ('sample' or 'prep'), found in the study_path,
        from which to extract attributes.
    """
    # Extract the study id from path
    study_id = os.path.basename(study_path)
    regex = create_metadata_filename_regex(metadata, study_id)
    # Get the a list of headings from metadata files
    for filename in os.listdir(study_path):
        match = regex.search(filename)
        if match:
            filepath = os.path.join(study_path, match.group())
            with open(filepath) as file:
                reader = csv.reader(file, delimiter='\t')
                header = list(next(reader))
            # Break assumes all prep files have the same column headings!
            break
    else:
        raise NoMatchingFileError(f'No metadata file of the type "{metadata}"'
                                  f'was found in "{study_path}"')
    return header


# TODO Lookup how to document returned dictionaries (how to specify key and
# values)!
def get_attributes_by_study(path, metadata='sample'):
    """Get all attributes associated with all studies found in the given path.

    Parameters
    ----------
    path : str, required
        Path to a directory containing all study directories of interest.
    metadata : str, required
        Type of metadata file ('sample' or 'prep'), found in the study_dir,
        from which to extract attributes.

    Returns
    -------
    dict
        Dictionary where keys are study identifiers and values are lists of
        attributes found in the study's metadata (for the given metadata type).
    """
    attributes = {}
    for study_path in generate_study_paths(path):
        study_id = os.path.basename(study_path)
        if metadata == 'sample':
            attributes[study_id] = get_attribute_list(study_path,
                                                      metadata='sample')
        elif metadata == 'prep':
            attributes[study_id] = get_attribute_list(study_path,
                                                      metadata='prep')
        else:
            raise ValueError('Unknown metadata type. Accepted options are "sample" '
                             'or "prep".')
    return attributes


def get_attribute_values(path, metadata='sample'):
    """Get values for each attribute found in metadata files.

    Parameters
    ----------
    path : str
        Path to directory containing all study directories of interest.
    metadata : str
        Type of metadata file ('sample' or 'prep'), found in the study_dir,
        from which to extract attributes.

    Returns
    -------
    dict
        Dictionary whose keys are the metadata attributes and values are sets
        of attribute values.
    """
    attributes = defaultdict(set)
    for study_path in generate_study_paths(path):
        study_id = os.path.basename(study_path)
        regex = create_metadata_filename_regex(metadata, study_id)
        for filename in os.listdir(study_path):
            match = regex.search(filename)
            if match:
                filepath = os.path.join(study_path, match.group())
                df = pd.read_csv(filepath, sep='\t')
                for col in df.columns:
                    values = df[col].unique()
                    attributes[col].update(values)
                break
    return attributes


def get_studies_without_attribute(attribute_name, attribute_dict):
    """Get identifiers of studies whose metadata lacks the given attribute_name.

    Parameters
    ----------
    attribute_name : str
        The name of an attribute to check against attribute_dict in order to
        identify studies missing/without this attribute.
    attribute_dict : dict
        Dictionary whose keys are study identifiers and values are lists
        of attributes found in a particular metadata file. This is the sort of
        dictionary returned by get_attributes_by_study.

    Returns
    -------
    list
        List of all study identifiers of studies that are missing the
        attribute_name in metadata to which attribute_dict relates.
    """
    studies = []
    for study_id, attributes in attribute_dict.items():
        if attribute_name not in attributes:
            studies.append(study_id)
    return studies


def get_studies_with_attribute(attribute_name, attribute_dict):
    """Get identifiers of studies whose metadata contains the given attribute_name.

    Parameters
    ----------
    attribute_name : str
        The name of an attribute to check against attribute_dict in order to
        identify studies containing this attribute.
    attribute_dict : dict
        Dictionary whose keys are study identifiers and values are lists
        of attributes found in a particular metadata file. This is the sort of
        dictionary returned by get_attributes_by_study.

    Returns
    -------
    list
        List of all study identifiers of studies that contain the
        attribute_name in metadata to which attribute_dict relates.
    """
    studies = []
    for study_id, attributes in attribute_dict.items():
        if attribute_name in attributes:
            studies.append(study_id)
    return studies


def count_attributes(attribute_dict):
    """Count the number of times attributes are found in study metadata.

    Parameters
    ----------
    attribute_dict : dict
        Dictionary of study attributes, where keys are study identifiers
        and attributes are values found in the study metadata.

    Returns
    -------
    collections.Counter
        Counter giving the number of times each attribute appears in metadata
        to which the attribute_dict relates.
    """
    counter = Counter()
    for attributes in attribute_dict.values():
        counter += Counter(attributes)
    return counter


# TODO: Decide on the sentence structure of the description of the returned
# dictionary.
def get_studies_by_missing_attributes(attribute_set, attribute_dict):
    """Index studies by attributes that are not found in the study's metadata.

    Parameters
    ----------
    attribute_dict : dict
        Dictionary whose keys are study identifiers and values are lists
        of attributes found in a particular metadata file.
    attribute_set : iterable
        Interable of attributes to check (whether missing in studies).

    Returns
    -------
    dict
        Dictionary mapping attributes to lists of study identifiers for studies
        whose metadata do not contain the given attribute.
    """
    missing_attributes = defaultdict(list)
    for attribute in attribute_set:
        for study_id, attributes in attribute_dict.items():
            if attribute not in attributes:
                missing_attributes[attribute].append(study_id)
    return missing_attributes


def get_studies_by_present_attributes(attribute_set, attribute_dict):
    """Index studies by attributes that are found in the study's metadata.

    Parameters
    ----------
    attribute_dict : dict
        Dictionary whose keys are study identifiers and values are lists
        of attributes found in a particular metadata file.
    attribute_set : iterable
        Interable of attributes to check (whether present in studies).

    Returns
    -------
    dict
        Dictionary mapping attributes to lists of study identifiers for studies
        whose metadata contains the given attribute.
    """
    present_attributes = defaultdict(list)
    for attribute in attribute_set:
        for study_id, attributes in attribute_dict.items():
            if attribute in attributes:
                present_attributes[attribute].append(study_id)
    return present_attributes


def get_similar_attributes(search_str, present_dict):
    """Search study metadata for attributes matching a given pattern.

    Parameters
    ----------
    search_str : str
        (Raw) string that is a valid regular expression to search for matching
        attribute names.
    present_dict : dict
        Dictionary returned by get_studies_by_present_attributes.

    Returns
    -------
    list
        List of metadata attributes matching the given pattern.
    """
    attr_re = re.compile(search_str, re.I)
    attrs = []
    for attr in present_dict:
        if attr_re.search(attr):
            attrs.append(attr)
    return attrs


def get_study_set_from_attrs(attribute_set, present_dict):
    """Find studies whose metadata contains any of the given attributes.

    Parameters
    ----------
    attribute_set : iterable
        Interable of attributes to check (whether present in studies).
    present_dict : dict
        Dictionary returned by get_studies_by_present_attributes.

    Returns
    -------
    list
        List of metadata attributes matching the given pattern.
    """
    studies = set()
    for attr in attribute_set:
        if attr in present_dict:
            studies.update(present_dict[attr])
    return studies


def write_attribute_map_template_to_json(attribute_set, output):
    """Create an attribute map template file.

    Write a JSON file in the given file_path, storing an object mapping
    attributes given in the attribute_set to empty strings. This file was meant
    to serve as a template for matching attributes in the sample metadata and
    prep files to attributes of a Sample, Subject or Preparation object.

    Parameters
    ----------
    attribute_set : Iterable
        Iterable of attributes to include in the attribute map template.
    output : str
        Path to the output (attribute map template) file.
    """
    attribute_map_template = dict.fromkeys(attribute_set, '')
    json_str = json.dumps(attribute_map_template, indent=4)
    with open(output, 'w') as file:
        file.write(json_str)


def load_attribute_map_from_json(file_path):
    """Load an attribute dictionary stored in a JSON file.

    Parameters
    ----------
    file_path : str
        Path to the attribute map file.

    Returns
    -------
    dict
        Dictionary whose keys and values are provided by the object stored in
        the given JSON file. Keys are attributes found in the sample or prep
        metadata files; values are the names of attributes in a Sample, Subject
        or Preparation object.
    """
    with open(file_path) as file:
        json_str = file.read()
    attribute_map = json.loads(json_str)
    return attribute_map


if __name__ == '__main__':
    # Investigation into Qiita metadata headings
	# This is useful if there are many experiments in path below.
    path = r'../data/test_data/experiments'

    sample_dict = get_attributes_by_study(path, metadata='sample')
    prep_dict = get_attributes_by_study(path, metadata='prep')
    count_samp = count_attributes(sample_dict)
    count_prep = count_attributes(prep_dict)
    missing_samp = get_studies_by_missing_attributes(count_samp.keys(), sample_dict)
    missing_prep = get_studies_by_missing_attributes(count_prep.keys(), prep_dict)
    present_samp = get_studies_by_present_attributes(count_samp.keys(), sample_dict)
    present_prep = get_studies_by_present_attributes(count_prep.keys(), prep_dict)

    # Try to find attributes based on regex pattern
    attrs = get_similar_attributes(r'age_', present_samp)
    attr_study = get_studies_by_present_attributes(attrs, sample_dict)
    attr_studies_set = get_study_set_from_attrs(attrs, present_samp)
