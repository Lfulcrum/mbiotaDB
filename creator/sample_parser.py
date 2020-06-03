# -*- coding: utf-8 -*-
"""
Parse Experiments, Subjects and Samples from metadata file

Created on Sat Oct  5 14:47:45 2019
@author: William
"""

# Standard library imports
import re
import csv
import datetime
import logging
import os.path
from os.path import dirname
from dateutil import parser
from contextlib import contextmanager
from collections import defaultdict

# Third-party imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.engine.url import URL
from pint import UndefinedUnitError
from pandas import read_csv, isna
from numpy import vectorize, nan

# Local application imports
import model
from model import Source, Provenance, Experiment, Sample, Subject
from model import SamplingSite, Time
from . import ureg


# Constants
ROOT_DIR = dirname(dirname(__file__))

# Setup logging
logging.basicConfig(filename=os.path.join(ROOT_DIR, 'log', 'sample_parser.log'),
                    filemode='w',
                    format='%(asctime)s    %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG)

# Global regular expressions
re_missing = re.compile(r'^$|(missing:)? *(not provided|not collected|'
                        r'restricted access|na|not applicable|none|'
                        r'unspecified|labcontrol test|no data|blank)',
                        re.I)
re_invalid_numeric_chars = re.compile(r'[^.\d]')
male_re = re.compile(r'male|m', re.I)
female_re = re.compile(r'female|f', re.I)

# The units for variables that we want our database to assume.
database_units = {'age': ureg.years,
                  'height': ureg.metres,
                  'weight': ureg.kilograms}
# Default units for variables of interest in metadata file
# i.e. if metadata file leaves units ambiguous, what units should we assume.
file_units = {'age': ureg.years,
              'height': ureg.metres,
              'weight': ureg.kilograms}


def generate_rows(metadata_file):
    with open(metadata_file) as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            yield row


# Define wrapper functions
# Will return their nested functions given appropriate arguments for
# extracting particular data from the rows of a sample metadata file.
def get_string(column, required=True):
    if required:
        def string_getter(row):
            try:
                variable = row[column].strip()
            except KeyError:
                raise Exception(f'Sample metadata file does not have a '
                                f'"{column}" column.')
            if re_missing.match(variable):
                raise Exception(f'Missing value detected in "{column}" column.')
            return variable
    else:
        def string_getter(row):
            try:
                variable = row[column].strip()
            except KeyError:
                raise Exception(f'Sample metadata file does not have a '
                                f'"{column}" column.')
            if re_missing.match(variable):
                return None
            return variable
    return string_getter


def get_valid_string(columns, extractor=None):
    def string_getter(row):
        for col in columns:
            try:
                variable = row[col].strip()
            except KeyError:
                continue
            if re_missing.match(variable):
                continue
            if extractor:
                variable = extractor(col, variable)
            return variable
    return string_getter


def get_numeric(columns):
    def numeric(row):
        for col in columns:
            try:
                variable = row[col].strip()
                if re_missing.match(variable):
                    variable = None
                else:
                    variable = float(variable)
                    break
                # TODO Should we remove all non-numeric characters from col value
            # TODO Should we move this up to just below 'variable' assignment
            # and use continue statement?
            except KeyError:
                variable = None
        return variable
    return numeric


# Define wrapper functions to get units
def get_units(variable, columns):
    def units_function(row, default=None):
        study_id = get_study_id(row)
        subject_id = get_subject_id(row)
        sample_id = get_sample_id(row)
        for col in columns:
            try:
                units = row[col].strip().lower()
            except KeyError:
                units = None
                continue
            else:
                # Search other unit columns if missing value detected
                if re_missing.match(units):
                    continue
                # Search for recognized units (also excludes missing values)
                try:
                    units = ureg.parse_expression(units).units
                    break
                except UndefinedUnitError:
                    logging.info(
                        f'Ambiguous {variable} units "{units}" in '
                        f'column: {col} for '
                        f'study: {study_id}, '
                        f'subject: {subject_id}, '
                        f'sample: {sample_id}. \n'
                    )
                    units = None
                    continue
        if units:
            return units
        if default:
            logging.info(
                f'No {variable} units found for '
                f'study: {study_id}, '
                f'subject: {subject_id}, '
                f'sample: {sample_id}. '
                f'Using default {variable} unit: "{default}".'
            )
            return default
        else:
            raise Exception(f'No {variable} units were found for '
                            f'study: {study_id}, '
                            f'subject: {subject_id}, '
                            f'sample: {sample_id}. '
                            f'and no default was given.'
                            )
    return units_function


def get_numeric_with_units(variable, columns, unit_regex, units_function):
    default_to_units = database_units[variable]
    default_from_units = file_units[variable]

    def numeric_with_units(row,
                           to_units=default_to_units,
                           from_units=default_from_units):
        re_valid_units = re.compile(unit_regex)
        for col in columns:
            try:
                variable = row[col].strip().lower()
            except KeyError:
                variable = None
                continue
            # Find variable units
            units_in_value = re_valid_units.search(variable)
            units_in_col_name = re_valid_units.search(col)
            if units_in_value:
                units = units_in_value.group()
                units = ureg.parse_expression(units).units
            elif units_in_col_name:
                units = units_in_col_name.group()
                units = ureg.parse_expression(units).units
            else:
                units = units_function(row, default=default_from_units)
            # Check if variable is missing, else remove any invalid chars
            if re_missing.match(variable):
                variable = None
                # Search other variable columns (if there are any remaining)
                continue
            else:
                variable = float(re_invalid_numeric_chars.sub('', variable))
            # Apply necessary conversions
            try:
                variable = (variable*units).to(to_units)
            except KeyError:
                raise Exception(f'Conversion of {variable} '
                                f'from {units} to {to_units} not supported.')
            # If successful conversion
            break
        if variable:
            return variable.magnitude
        return variable
    return numeric_with_units


# Extractor functions (used by wrapper functions)
# Valid string value extractors
def extract_gender(col, value):
    if male_re.match(value):
        return 'male'
    elif female_re.match(value):
        return 'female'
    else:
        return 'not known'


def extract_country(col, value):
    if col == 'country':
        try:
            country = value.split(':')[1]
        except IndexError:
            # TODO could produce a invalid value?
            country = value
    elif col == 'geo_loc_name':
        country = value.split(':')[0]
    return country


def extract_race(col, value):
    if value in ('White', 'Caucasian', 'Black', 'African American',
                 'Black/African American', 'Asian', 'Other',
                 'Asian Caucasian hybrid', 'Asian or Pacific Islander',
                 'Hispanic'):
        return value
    else:
        return None


def extract_csection(col, value):
    if col in ('delivery_mode', 'birth_mode'):
        if value in ('c-section', 'cesarean delivery', 'Cesarea'):
            csection = True
        elif value in ('vaginal', 'vaginal delivery', 'Vaginal'):
            csection = False
        else:
            csection = None
    elif col in ('csection'):
        if value in ('True', 'true', 'EC', 'PC', 'Yes', 'yes', 'CS'):
            # Note: EC/PC might be Emergency/Planned Caesarean
            csection = True
        elif value in ('False', 'false', 'V', 'Vaginal', 'No', 'no'):
            csection = False
        else:
            csection = None
    return csection


def extract_disease(col, value):
    re_healthy = re.compile(r'healthy|no|none', re.I)
    if value in ('cystic fibrosis'):
        return value
    elif re_healthy.search(value):
        return 'healthy'
    else:
        return None


# Subclass parserinfo class in order to correctly parse likely birthdates
# for ambiguous years given as just 2 digits.
# Yields incorrect dob when people pass age of 100 at the time the study
# is parsed. Unfortunately, this means if a person was 89 back in 2008, their
# date of birth would be incorrectly inferred as today in 2019!
# TODO: Improve function by taking into account the year the study was
# performed and asserting that a birthdate with ambiguous year should be
# interpreted as a year 100 years prior to the year of the study.
class dob_parserinfo(parser.parserinfo):

    def __init__(self):
        super(dob_parserinfo, self).__init__()

    def convertyear(self, year, century_specified=False):
        """Converts two-digit years to year within [-99, 0]
        range of self._year (current local time)
        """
        # Function contract is that the year is always positive
        assert year >= 0
        if year < 100 and not century_specified:
            # assume current century to start
            year += self._century
            if year >= self._year + 1:  # if too far in future
                year -= 100
            elif year < self._year - 100:  # if too far in past
                year += 100
        return year


def extract_dob(col, value):
    try:
        date = parser.parse(value, parserinfo=dob_parserinfo()).date()
    except ValueError:
        return None
    return date


# Functions to parse sample collection date and time
# Note: Doesn't use a function wrapper (with extractor)
def has_day_first(timestamp, default=False):
    # Assumes month first by default!
    if isna(timestamp):
        return default
    re_day_month_year = re.compile(r'(?:^|[^-/\d])'             # border char
                                   r'(0?[1-9]|[1-2][0-9]|3[0-1])[/-]'  # day
                                   r'(0?[1-9]|1[0-2])[/-]'              # month
                                   r'(\d{2}|\d{4})'                    # year
                                   r'(?:$|[^-/\d])'             # border char
                                   )
    re_month_day_year = re.compile(r'(?:^|[^-/\d])'             # border char
                                   r'(0?[1-9]|1[0-2])[/-]'             # month
                                   r'(0?[1-9]|[1-2][0-9]|3[0-1])[/-]'  # day
                                   r'(\d{2}|\d{4})'                    # year
                                   r'(?:$|[^-/\d])'             # border char
                                   )
    ddmm = re_day_month_year.search(timestamp)
    mmdd = re_month_day_year.search(timestamp)
    if ddmm and not mmdd:
        return True
    elif mmdd and not ddmm:
        return False
    else:
        return default


def infer_date_formats(metadata_file):
    cols = {'collection_timestamp', 'collection_date',
            'collection_time', 'collectiontime', 'sample_date', 'run_date'}
    with open(metadata_file) as file:
        header = next(file).rstrip().split('\t')
        usecols = cols.intersection(header)
        dtypes = dict.fromkeys(usecols, str)
        df = read_csv(file, names=header, sep='\t', usecols=usecols,
                      dtype=dtypes)
    # Replace all strings that look like missing values
    df.replace(re_missing, nan, regex=True, inplace=True)
    dayfirst_dict = {}
    for col in df.columns:
        new_col = has_day_first(df[col])
        if True in new_col:
            dayfirst_dict[col] = True
        else:
            dayfirst_dict[col] = False
    return dayfirst_dict


def get_collection_datetime(row, dayfirst_dict):
    re_interval = re.compile(r'((?:\d{1,2}/)?(?:\d{1,2}/)?(?:\d{4}|\d{2}))'  # start date
                             r'-'                                            # interval sep
                             r'((?:\d{1,2}/)?(?:\d{1,2}/)?(?:\d{4}|\d{2}))') # end date
    re_time = re.compile(r'(\d{1,2})(.)(\d{1,2})\s*(?:am|pm)')
    sample_date = None
    sample_time = None
    for col in ['collection_timestamp', 'collection_date',
                'collection_time', 'sample_date']:
        try:
            timestamp = row[col].strip()
        except KeyError:
            timestamp = None
        else:
            if re_missing.match(timestamp):
                continue
            try:
                dt = parser.parse(timestamp, dayfirst=dayfirst_dict[col])
            except ValueError:
                # Assume the timestamp is an interval
                match = re_interval.search(timestamp)
                if match:
                    # Only use the first date (for simplicity)
                    dt = parser.parse(match.group(1), dayfirst=dayfirst_dict[col])
                # If a strange time format like 11_30am is encountered:
                # Only search times, NOT dates i.e. matching beginning of string
                match = re_time.match(timestamp)
                if match:
                    sep = match.group(2)
                    timestamp = timestamp.replace(sep, ':')
                    dt = parser.parse(timestamp)
                if not dt:
                    raise
            (date, time) = extract_date_time_from(dt)
            if not sample_date:
                sample_date = date
            if not sample_time:
                sample_time = time
    return (sample_date, sample_time)


def extract_date_time_from(dt):
    """Extract the date and time components from a datetime object.

    If the date is earlier than 1980 or at/later than the current date, 
    assume no date.
    If the time is midnight, assume no time.

    Returns
    -------
    Tuple with two elements, the date and time objects or None if no date/time
    found or date/time was invalid.
    """
    date = dt.date()
    time = dt.time()
    if (date < datetime.date(1980, 1, 1) or date >= datetime.date.today()):
        date = None
    if time == datetime.time(0, 0):
        time = None
    return (date, time)


# Functions to get values from a row:
get_study_id = get_string('qiita_study_id')
get_sample_id = get_string('sample_name')
get_subject_id = get_string('host_subject_id', required=False)
get_body_habitat = get_valid_string(['body_habitat'])
get_body_product = get_valid_string(['body_product'])
get_body_site = get_valid_string(['body_site'])
get_env_biom = get_valid_string(['env_biome'])
get_env_feature = get_valid_string(['env_feature'])
get_sex = get_valid_string(['sex', 'gender'], extractor=extract_gender)
get_country = get_valid_string(['geo_loc_name', 'country'],
                               extractor=extract_country)
get_race = get_valid_string(['race_code', 'race', 'racescrq_self_rpt',
                             'raceethnicity'], extractor=extract_race)
get_csection = get_valid_string(['delivery_mode', 'birth_mode'],
                                extractor=extract_csection)
get_disease = get_valid_string(['disease'], extractor=extract_disease)
get_dob = get_valid_string(['dob', 'birth_date', 'date_of_birth', 'birth_year'],
                           extractor=extract_dob)
get_height_units = get_units('height',
                             ['height_unit', 'height_units', 'host_height_units'])
get_age_units = get_units('age',
                          ['age_unit', 'age_units'])
get_weight_units = get_units('weight',
                             ['weight_units', 'host_weight_units'])
get_age = get_numeric_with_units('age',
                                 ['age_years', 'age_months', 'age_weeks',
                                  'age_days', 'age', 'host_age'],
                                 r'years|months|weeks|days',
                                 get_age_units)
get_height = get_numeric_with_units('height',
                                    ['height_m', 'height_cm', 'height_mm',
                                     'height_ft', 'height_in', 'height',
                                     'host_height', 'height_or_length'],
                                    r'mm|cm|m|in',
                                    get_height_units)
get_weight = get_numeric_with_units('weight',
                                    ['weight', 'host_weight', 'weight_kg'],
                                    r'kg|lbs',
                                    get_weight_units)
get_latitude = get_numeric(['latitude'])
get_longitude = get_numeric(['longitude'])
get_elevation = get_numeric(['elevation'])
get_bmi = get_numeric(['bmi', 'body_mass_index', 'host_body_mass_index'])
has_day_first = vectorize(has_day_first)


# Functions to parse a rows into SQLAlchemy objects

# TODO Could make this function more modular - write separate functions to
# parse source name.
# TODO Differentiate between raised exceptions!
def parse_source(row, name=None, type_=None, url=None, provenance=None):
    """Parse a row into a Source object.
    
    Parameters
    ----------
    row : dict-like 
        Keys are column headings and values are the row values of a metadata 
        file.
    name : str, optional
        Name of data source.
    type_ : str, optional
        Type of data source.
    url : str, optional
        URL of data source.
    provenance : Provenance, optional
        Provenance object to be linked to the data source.
    
    Raises
    ------
    Exception : if there are no columns ending in 'study_id'.
    Exception : if no source name could be found for the given row and no name 
    argument was given.

    Returns
    -------
    Source
    """
    source = Source()
    # Initialize default values for attributes
    source_name = str(name) if name else None
    source_type = str(type_) if type_ else None
    source_url = str(url) if url else None
    
    # Find source_name in 'study_id' column
    for key in row.keys():
        match = re.match(r'(.*)study_id$', key)
        if match:
            if match.group(1):
                source_name = match.group(1).rstrip('_')
            break
    else:
        raise Exception('The metadata file must contain exactly one column '
                        'ending in "study_id".')
    # Replace source_name with 'source' column value if it exists
    new_source_name = row.get('source', '')
    if not re_missing.match(new_source_name):
        source_name = new_source_name
    # If source_name could not be initialized
    if not source_name:
        raise Exception('No source name could be found in the "study_id" '
                        'column heading, or as a valid value in the "source" '
                        'column of the metadata_file, and no source name '
                        'argument was provided.')
    
    # Replace source_type with 'source_type' column value if it exists
    new_source_type = row.get('source_type', '')
    if not re_missing.match(new_source_type):
        source_type = new_source_type
    if not source_type:
        raise Exception('No source type could be found in the "source_type" '
                        'column of the metadata_file, and no source type_ '
                        'argument was provided.')
    
    # Replace source_url with 'source_url' column value if it exists
    new_source_url = row.get('source_url', '')
    if not re_missing.match(new_source_url):
        source_url = new_source_url
    if not source_url:
        raise Exception('No source url could be found in the "source_url" '
                        'column of the metadata_file, and no source url'
                        'argument was provided.')
    
    # Assign to source attributes
    source.name = source_name
    source.type_ = source_type
    source.url = source_url
    if provenance:
        source.provenances.append(provenance)
    return source


# TODO: Distinguish raised exceptions!
def parse_provenance(object_, source=None, insert_timestamp=None,
                     orig_timestamp=None, orig_id=None):
    """Create a Provenance object for the given object_.
    
    Parameters
    ----------
    object_ : sqlalchemy.ext.declarative.api.Base, required
        The object for which we want to create a Provenance.
    source : model.Source, optional
        Source of the object_ for which we want to create a Provenance.
    insert_timestamp : str or datetime.datetime, optional
        Timestamp denoting the time that this object is inserted into the
        database.
    orig_timestamp : str or datetime.datetime, optional
        Timestamp derived from the source of the object.
    orig_id : str, optional
        Original identifier derived from the source of the object.
    
    Raises
    ------
    AttributeError
        If the given object_ does not have all attributes required to create a
        Provenance.
    TypeError
        If the given insert_timestamp or orig_timestamp could not be converted
        to a string.
    ValueError
        If the string representation of the given insert_timestamp or
        orig_timestamp could not be parsed into a datetime.datetime object.
    ValueError
        If object_.id is None.

    Returns
    -------
    Provenance
        Provenance for the given object_.
    """
    provenance = Provenance()
    # Difficult to check valid datetime formats, so if missing date/time
    # elements, default_datetime will be used by dateutil.parser.parse to fill
    # in missing data e.g. a missing year will be filled by current_year.
    # It is either this, or we explicitly specify valid input datetime formats
    # through regex or using datetime.strptime
    current_year = datetime.datetime.now().year
    default_datetime = datetime.datetime(current_year,1,1,0,0,0)
    try:
        provenance.object_id = object_.id
        provenance.object_type = object_.__tablename__
        if not orig_timestamp:
            # Assumes object.orig_timestamp is a valid timestamp
            provenance.orig_timestamp = object_.orig_timestamp
        if not orig_id:
            # Assumes object.orig_id is a valid orig_id
            provenance.orig_id = object_.orig_id
    except AttributeError:
        raise AttributeError('The given object_ must have at least an id and '
                             '__tablename__ attribute. It must also have an '
                             'orig_timestamp and orig_id attribute if these are '
                             'not given as arguments.')
    if provenance.object_id is None:
        raise ValueError('No value for the id attribute of the given object_.')
    # Note: update_timestamp and delete_timestamp attributes are already
    # initialized to None. If no insert_timestamp is given, this attribute
    # will also be initialized to None.
    if insert_timestamp:
        try:
            insert_timestamp = parser.parse(str(insert_timestamp),
                                            default=default_datetime)
            provenance.insert_timestamp = insert_timestamp
        except TypeError as type_err:
            raise type_err('Could not convert the given insert_timestamp '
                           f'{insert_timestamp!r} to a string.')
        except ValueError as timestamp_exp:
            raise timestamp_exp('Could not parse the insert_timestamp '
                               f'string "{insert_timestamp!s}" into a '
                               'datetime.datetime object.')
    # If supplied, orig_timestamp and orig_id will overwrite values supplied
    # as object_ attributes
    if orig_timestamp:
        try:
            provenance.orig_timestamp = parser.parse(str(orig_timestamp),
                                                     default=default_datetime)
        except TypeError as type_err:
            raise type_err('Could not convert the given orig_timestamp '
                           f'{orig_timestamp!r} to a string.')
        except ValueError as timestamp_exp:
            raise timestamp_exp('Could not parse the orig_timestamp '
                               f'string "{orig_timestamp!s}" into a '
                               'datetime.datetime object.')
    if orig_id:
        try:
            provenance.orig_id = str(orig_id)
        except TypeError as type_err:
            raise type_err('Could not convert the given orig_id '
                           f'{orig_id!r} to a string.')
    # Upon insert of a provenance, 'current' should always be True
    provenance.current = True
    # Establish relationship with source, if given
    if source:
        provenance.source = source
    return provenance


def parse_subject(row, source=None):
    """Parse a row into a Subject.

    Parameters
    ----------
    row : dict-like
        Object whose keys are column headings and values are the row values.
    source : model.Source
        Source for the returned subject.

    Returns
    -------
    Subject
    """
    subject = Subject()
    subject.sex = get_sex(row)
    subject.country = get_country(row)
    subject.race = get_race(row)
    subject.csection = get_csection(row)
    subject.disease = get_disease(row)
    subject.dob = get_dob(row)
    
    # Initialize equality attrs
    if not source:
        subject.source = parse_source(row)
    elif isinstance(source, Source):
        subject.source = source
    else:
        raise TypeError(f'Given source was not of type {type(Source())!r}.')
    subject.orig_study_id = get_study_id(row)
    subject.orig_subject_id = get_subject_id(row)
    return subject


def parse_sample(row, dayfirst_dict, source=None):
    """Parse a row into a Sample.

    Parameters
    ----------
    row : dict-like 
        Object whose keys are column headings and values are the row values.
    dayfirst_dict : dict
        Dictionary whose keys are names of columns containing date/time data 
        and values are boolean indicating whether the dates in that column 
        should be interpreted as having a day as the first component (True)
        or a month or year as the first component (False).
    source : model.Source
        Source for the returned sample.

    Returns
    -------
    Sample
    """
    sample = Sample()
    sample.age_units = get_age_units(row, ureg.years)
    sample.age = get_age(row)
    sample.latitude = get_latitude(row)
    sample.longitude = get_longitude(row)
    sample.elevation = get_elevation(row)
    sample.height_units = get_height_units(row, ureg.metres)
    sample.height = get_height(row)
    sample.weight_units = get_weight_units(row, ureg.kilograms)
    sample.weight = get_weight(row)
    sample.bmi = get_bmi(row)
    (d, t) = get_collection_datetime(row, dayfirst_dict)
    sample.sample_date = d
    sample.sample_time = t
    sample.sampling_time = parse_sampling_time(sample)
    sample.sampling_site = parse_sampling_site(row)
    
    # Initialize equality attrs
    if not source:
        sample.source = parse_source(row)
    elif isinstance(source, Source):
        sample.source = source
    else:
        raise TypeError(f'Given source was not of type {type(Source())!r}.')
    sample.orig_study_id = get_study_id(row)
    sample.orig_subject_id = get_subject_id(row)
    sample.orig_sample_id = get_sample_id(row)
    return sample


def parse_sampling_site(row):
    """Parse a row into a sampling site."""
    sampling_site = SamplingSite()
    sampling_site.uberon_habitat_term = get_body_habitat(row)
    sampling_site.uberon_product_term = get_body_product(row)
    sampling_site.uberon_site_term = get_body_site(row)
    sampling_site.env_biom_term = get_env_biom(row)
    sampling_site.env_feature_term = get_env_feature(row)
    return sampling_site


def parse_sampling_time(object_):
    """Parse an object into a sampling time.
    
    Parameters
    ----------
    object_ : datetime.datetime, model.Sample
        The object to be parsed into a Time object, representing the sampling
        time.
    """
    # Parse from datetime-like object
    try:
        time = Time.from_datetime(object_)
    except AttributeError:
        time = None
    else:
        return time
    # Parse from model.Sample
    try:
        sample_date = object_.sample_date
        sample_time = object_.sample_time
        default_date = datetime.date(1,1,1)
        default_time = datetime.time(0,0,0)
        if sample_date and sample_time:
            timestamp = datetime.datetime.combine(sample_date, sample_time)
        elif sample_date:
            timestamp = datetime.datetime.combine(sample_date, default_time)
        elif sample_time:
            timestamp = datetime.datetime.combine(default_date, sample_time)
        else:
            # Sample object has no date or time
            return None
        time = Time.from_datetime(timestamp)
    except AttributeError:
        time = None
    else:
        return time
    return time


# TODO Implement Exceptions specific to each parsed object (Source, Experiment,
# sample, subject etc.), specify under Raises in docstring here!
def parse_objects(metadata_file, returning='experiments'):
    """Parse the given metadata_file into a collection of Experiments.
    
    Parameters
    ----------
    metadata_file : str
        Path to metadata file to be parsed.
    
    Returns
    -------
    dict
        Dictionary of Experiments with relationships established to Subjects 
        and Samples. The keys of the dictionary are unique identifiers, while
        values are Experiments.
    """
    # Collections of SQLAlchemy objects
    sources = {}
    experiments = {}
    subjects = {}
    samples = {}
    sampling_sites = {}
    
    # Reference dictionaries:
    # These provide the ability to lookup objects via their identifiers
    experiment_ids = {}
    subject_ids = {}
    sample_ids = {}
    
    # BEGIN PARSING
    # Infer date format
    dayfirst_dict = infer_date_formats(metadata_file)
    row_generator = generate_rows(metadata_file)
    for row in row_generator:
        # Parse row into SQLAlchemy objects
        # TODO Implement properly - without need for specifying these keyword
        # arguments
        source = parse_source(row, name='Qiita', 
                              type_='Database (Public)', 
                              url=f'https://qiita.ucsd.edu/study/description/0')
        try:
            source = sources[source.equality_attrs]
        except KeyError:
            sources[source.equality_attrs] = source
        subject = parse_subject(row, source=source)
        sample = parse_sample(row, dayfirst_dict, source=source)
        sampling_site = parse_sampling_site(row)
        # TODO Write parser for Experiment alone?
        experiment = Experiment()
        experiment.source = source
        experiment.orig_study_id = get_study_id(row)
        # Search collections for equivalent objects and replace if found
        try:
            experiment = experiments[experiment.equality_attrs]
        except KeyError:
            experiments[experiment.equality_attrs] = experiment
        try:
            subject = subjects[subject.equality_attrs]
        except KeyError:
            subjects[subject.equality_attrs] = subject
        # Although each row has a unique sample_id for files metadata files 
        # derived from Qiita, each file also corresponds to one experiment in 
        # Qiita. If we want our parser to cope with the possibility of more
        # than one experiment per file, we cannot guarantee sample_id 
        # uniqueness, so we use several attributes in the sample.equality_attrs
        # (not just orig_sample_id).
        try:
            sample = samples[sample.equality_attrs]
        except KeyError:
            samples[sample.equality_attrs] = sample
        try:
            sampling_site = sampling_sites[sampling_site.equality_attrs]
        except KeyError:
            sampling_sites[sampling_site.equality_attrs] = sampling_site
        # Create provenance objects
        # TODO We could do this at a later stage, but perhaps it is convenient
        # to do it here? If done here, we would need to relax the restriction
        # about the given object having a non-None id attribute in 
        # parse_provenance.
        # TODO Write function to parse orig_timestamp for each object and
        # default to the timestamp in the filename!
#        experiment_provenance = parse_provenance(experiment, source,
#                                                 orig_timestamp=ts,
#                                                 orig_id = study_id)
        # Don't check yet whether any object (incl. sampling site) is already
        # in the database, we will do that later (in another function).
        # Note: Collecting the objects together is necessary because they are
        # not all linked in the database design. They could be, but not 
        # necessarily!
        
        # TODO Note that the following assignments render the prior equality 
        # checking redundant! I think that we should handle the object 
        # relationships (equality checking) during preprocessing (which would
        # add a uniq_*_id column for each type of object). Then we should use
        # these uniq_*_ids instead of orig_*_ids to establish relationships here.
        experiment_ids[experiment.orig_study_id] = experiment
        subject_ids[subject.orig_subject_id] = subject
        sample_ids[sample.orig_sample_id] = sample
        
        # Establish relationships
        sample.sampling_site = sampling_site
        subject.add_sample(sample)
        experiment.add_sample(sample)
    object_dicts = {'experiments': experiment_ids,
                    'subjects': subject_ids,
                    'samples': sample_ids}
    try:
        # To avoid iterating over str if str provided as indexed_by arg
        # Note: More pythonic than type-checking
        returning + ''
        return object_dicts[returning]
    except TypeError:
        return [object_dicts[value] for value in returning]


if __name__ == '__main__':
    pass
