# -*- coding: utf-8 -*-
"""
Created on Sat May 23 22:52:23 2020

@author: William
"""

# Local application imports
from model import Sample, Subject, SamplingSite, Time

def parse_sample(row):
    sample = Sample()
    return sample

def parse_subject(row):
    subject = Subject()
    return subject

# NOTE: Because each of the following functions depends on df.iterrows(), the
# parsing of the sample and subject will depend on the column order in the
# dataframe. This is clearly undesirable.
def parse(df):
    samples = []
    subjects = []
    for row in df.iterrows():
        sample = parse_sample(row)
        subject = parse_subject(row)
        sample.subject = subject
        samples.append(sample)
        subjects.append(subject)
    return samples, subjects

def parse_samples(df):
    samples = {}
    for row in df.iterrows():
        sample = parse_samples(row)
        samples[sample.orig_id] = sample
    return samples

def parse_subjects(df):
    subjects = {}
    for row in df.iterrows():
        subject = parse_subjects(row)
        subjects[subject.orig_id] = subject
    return subjects

# An alternative approach:
def parse_samples(df):
    return df.apply(parse_sample, axis=1)

def parse_subjects(df):
    return df.apply(parse_subject, axis=1)

# Note: for both implementations of form_relationships below, we assume a one
# to one correspondence between sample and subject objects.
# This is fine, as long as we are able to check for duplicate subjects before
# database insertion. If this is not the case, see third implementation.
# The advantage of this first implementation is that it is easier to
# understand and makes no assumptions about the type of collections of samples
# and subjects.
def form_relationships(samples, subjects):
    for sample, subject in zip(samples, subjects):
        sample.subject = subject

# This is possibly less understandable than the above form_relationships, but
# perhaps it is faster/optimized?
# This implementation assumes samples and subjects are pandas Series objects.
def form_relationships(samples, subjects):
    samples.combine(subjects, add_subject)

def add_subject(sample, subject):
    sample.subject = subject



# Most flexible approach

# TODO: Lookup what you should properly call an "indexable" collection.
# Maybe you can just say dict-like?
def parse_sample(row, attr_map, site_attrs_map=None, time_attr=None):
    """Parse a row of an indexable collection into a Sample.

    Parameters
    ----------
    row : indexable collection
        A row to be parsed into a Sample.
    attr_map : dict
        A dictionary mapping indexes for the indexable collection `row` to
        attribute names of the Sample object.
    """
    sample = Sample()
    # print(row)
    for index, attr in attr_map.items():
        setattr(sample, attr, row[index])
    if site_attrs_map:
        sample.sampling_site = parse_sample_site(row, site_attrs_map)
    if time_attr:
        sample.sampling_time = parse_sample_time(row, time_attr)
    return sample

def parse_subject(row, attr_map):
    subject = Subject()
    for index, attr in attr_map.items():
        setattr(subject, attr, row[index])
    return subject

def parse_samples(df, id_col, attr_map):
    samples = df.apply(parse_sample, axis=1, args=(attr_map,))
    return dict(zip(df[id_col], samples))

# TODO: This function relies on drop_duplicates. It assumes that __eq__ is
# overridden for a subject object.
def parse_subjects(df, id_col, attr_map):
    subjects = df.apply(parse_subject, axis=1, args=(attr_map,))
    subjects.drop_duplicates(keep='first', inplace=True)
    new_ids = df[id_col].drop_duplicates(keep='first') # TODO: There will be a better way of doing this!
    return dict(zip(new_ids, subjects))

# TODO: The sample and subject id column info is used in several places.
# Consider including these functions as methods in some object that has the
# id column names as an attribute. Could be a general DataParser object, that
# stores parameters for parsing all different kinds of objects?
def form_relationship(df, sample_id_col, subject_id_col, samples, subjects):
    sample_subjects = zip(df[sample_id_col], df[subject_id_col])
    for sample_id, subject_id in sample_subjects:
        sample = samples[sample_id]
        subject = subjects[subject_id]
        sample.subject = subject


def parse_sample_time(row, datetime_col):
    datetime = row[datetime_col]
    return Time.from_datetime(datetime)

# TODO: Actually, is there any point in having sample_site attributes in a
# separate table?
def parse_sample_site(row, attr_map):
    sample_site = SamplingSite()
    for index, attr in attr_map.items():
        setattr(sample_site, attr, row[index])
    return sample_site



if __name__ == '__main__':
    pass