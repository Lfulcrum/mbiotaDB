# -*- coding: utf-8 -*-
"""
Created on Tue May 26 19:46:05 2020

@author: William
"""

# Local application imports
from model import Preparation, SeqInstrument

# TODO: Lookup what you should properly call an "indexable" collection.
# Maybe you can just say dict-like?
def parse_prep(row, attr_map):
    """Parse a row of an indexable collection into a Preparation.

    Parameters
    ----------
    row : indexable collection
        A row to be parsed into a Sample.

    attr_map : dict
        A dictionary mapping indexes for the indexable collection `row` to
        attribute names of the Sample object.
    """
    sample = Preparation()
    # print(row)
    for index, attr in attr_map.items():
        setattr(sample, attr, row[index])
    return sample

# TODO: Note that the sample, subject, prep, seq_instrument all use the
# following more general code (place it somewhere sensible):
def parse_from_row(row, cls, attr_map):
    obj = cls()
    for index, attr in attr_map.items():
        setattr(obj, attr, row[index])
    return obj

def parse_seq_instrument(row, attr_map):
    return parse_from_row(row, SeqInstrument, attr_map)

# TODO: Similarly, this funcationality is repeated in several places.
def parse_preps(df, id_col, attr_map):
    preps = df.apply(parse_prep, axis=1, args=(attr_map,))
    return dict(zip(df[id_col], preps))

# TODO: Similarly, this functionality is repeated in several places.
# TODO: This function relies on drop_duplicates. It assumes that __eq__ is
# overridden for a SeqInstrument object.
def parse_seq_instruments(df, id_col, attr_map):
    seq_instruments = df.apply(parse_seq_instrument, axis=1, args=(attr_map,))
    seq_instruments.drop_duplicates(keep='first', inplace=True)
    new_ids = df[id_col].drop_duplicates(keep='first') # TODO: There will be a better way of doing this!
    return dict(zip(new_ids, seq_instruments))

# TODO: Similarly, this functionality is repeated in several places.
def form_relationship(df, prep_id_col, seq_instrument_id_col,
                      preps, seq_instruments):
    prep_seq_instruments = zip(df[prep_id_col], df[seq_instrument_id_col])
    for prep_id, seq_instrument_id in prep_seq_instruments:
        prep = preps[prep_id]
        seq_instrument = seq_instruments[seq_instrument_id]
        prep.seq_instrument = seq_instrument
