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
import dateutil
import logging
import sys
import os.path
from os.path import dirname

# Third-party imports
import numpy as np
import pandas as pd
from pandas import read_csv, isna, Series, DataFrame
from pint import UndefinedUnitError, DimensionalityError

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
                        r'unspecified|labcontrol test|no data)',
                        re.I)
re_invalid_numeric_chars = re.compile(r'[^.\d]')
re_male = re.compile(r'male|^m$', re.I)
re_female = re.compile(r'female|^f$', re.I)
re_vaginal = re.compile(r'^(V)$|vag', re.I)
re_csection = re.compile(r'^(C|CS|EC|PC)$|c[-\s]*sec|ca?esar', re.I)


# Global regular expression templates
# Note: These are format strings! Regex qualifiers {n,m} etc. must use double
# braces {{n,m}}
re_template_date = '((?:\d{{1,2}}{0}|\d{{4}}{0})?(?:\d{{1,2}}{0})?(?:\d{{4}}|\d{{1,2}}))'
re_template_time = '(\d{{1,2}}{0}\d{{1,2}}(?:{0}\d{{1,2}})?\s*(?:am|pm)?)'

# The units for variables that we want our database to assume.
database_units = {'age': ureg.years,
                  'height': ureg.metres,
                  'weight': ureg.kilograms}
# Default units for variables of interest in metadata file
# i.e. if metadata file leaves units ambiguous, what units should we assume.
file_units = {'age': ureg.years,
              'height': ureg.metres,
              'weight': ureg.kilograms}

# Log message templates
std_missing_column = ('WARNING: `{}` column is not used in this DataFrame. '
                      'Processing for this column was skipped.')
invalid_type_column = ('WARNING: `{}` column is not a valid column type. '
                       'Column type is `{}`, but expected type `{}`.'
                       'Processing ({}) for this column was skipped.')


# MAIN FUNCTIONS (MANIPULATING DATA)

# TODO: converters and use_columns parameters exactly duplicate parameters of
# pandas.read_csv() [although use_columns is called usecols], should we get
# rid of them (they could still be supplied as keyword arguments to read_csv via
# **kwargs). Will keeping them cause any problems? If a user supplies both
# usecols and use_columns, we will have a repeated kwarg (usecols) in read_csv.
# One simple solution would be to rename use_columns to usecols.
# TODO: Supply a default_dayfirst parameter that will be used when dayfirst
# inference fails.
def parse_file(file, sep='\t', na_regex=None, strip=False,
               column_types={}, invalid_dates={}, invalid_times={}, **kwargs):
    """Parse a delimited file into a cleaner pandas DataFrame.

    This function convert a "dirty" delimited file (e.g. csv, tsv) into
    a "cleaner" pandas DataFrame, whose column values have been converted into
    values and types that are useful for further processing. Relevant columns
    (of interest) can be selected. Arbitrary converter functions can be applied
    using the `converters` parameter, then specific conversions based on column
    type information (in the `column_type` parameter) are performed.

    Parameters
    ----------
    file : str, path object or file-like object
        Any valid string path is acceptable, as is any ``os.PathLike`` object,
        or file-like objects (those with a ``read()`` method, including a file
        handler (e.g. returned by the builtin ``open()`` function), or
        ``StringIO``.
        Same as `filepath_or_buffer` parameter of ``pandas.read_csv()``.
    sep : str, default '\t'
        Delimiter to use.
        Same as the `sep` (or `delimiter`) parameter of ``pandas.read_csv()``,
        but with different default value ('\t' instead of ',').
    na_regex : str or re.Pattern
        A regular expression that will be used to replace all values that
        represent missing values with numpy.nan.
    strip : bool
        If true, whitespace is stripped from the left and right of every string
        value in the file.
    column_types : dict-like
        Maps columns onto their particular types. Recognized values for column
        types are: 'unit', 'date', 'time', 'timestamp', 'interval', 'numeric'.
    invalid_dates : dict of dicts
        Maps columns onto dictionaries, providing parameter values of function
        ``is_invalid_date()`` that will be used to check whether the column
        has invalid dates and to replace any such dates with None. Similar to
        `invalid_times` parameter.
    invalid_times : dict of dicts
        Maps columns onto dictionaries, providing parameter values of function
        ``is_invalid_time()`` that will be used to check whether the column
        has invalid times and to replace any such times with None. Similar to
        `invalid_dates` parameter.
    **kwargs
        Any other keyword parameters accepted by ``pandas.read_csv()``. No
        guarantee is made that all such parameters will be compatible with
        other processing parameters provided with this function.
    """
    df = read_csv(file, sep=sep, skip_blank_lines=True, skipinitialspace=True,
                  **kwargs)
    if na_regex:
        df.replace(to_replace=na_regex, value=np.nan, inplace=True)
    if strip:
        # Get all string-like columns
        str_cols = df.select_dtypes(['object'])
        df[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())
    recognized_column_types = {'unit', 'date', 'time', 'timestamp', 'interval',
                               'numeric'}
    invalid_column_types = set(column_types).difference(recognized_column_types)
    if invalid_column_types:
        raise ValueError()
    all_column_types = dict.fromkeys(recognized_column_types, [])
    all_column_types.update(column_types)
    # Start processing columns by type
    for unit_col in all_column_types['unit']:
        if not column_exists(df, unit_col): continue
        df[unit_col] = df[unit_col].apply(unit_converter, args=('replace',))
    for ts_col in all_column_types['timestamp']:
        if not column_exists(df, ts_col): continue
        # TODO: If the following pd str method fails, the column doesn't contain str
        # What should we do in this case, leave it to raise an error? Raise our
        # own error? Log the error and skip processing of that column?
        dates = df[ts_col].str.split('\s+')[0]  # Assume whitespace cannot be date separator
        try:
            dayfirst = infer_dayfirst(dates)
            df[ts_col] = pd.to_datetime(df[ts_col], dayfirst=dayfirst)
        except ValueError:
            # Search column entries for str that look like dates and times
            dates = extract_from_timestamps(df[ts_col], extract='date')
            times = extract_from_timestamps(df[ts_col], extract='time')
            # Overwrite original timestamp column
            df[ts_col] = dates + ' ' + times
            dayfirst = infer_dayfirst(dates)
            df[ts_col] = pd.to_datetime(df[ts_col], dayfirst=dayfirst)
        # Check for valid dates and times
        ts_col_invalid_dates = invalid_dates.get(ts_col, {})
        ts_col_invalid_times = invalid_times.get(ts_col, {})
        dates, times = validate_datetimes(df[ts_col],
                                          invalid_dates=ts_col_invalid_dates,
                                          invalid_times=ts_col_invalid_times)
        dates.name = ts_col + '_date'
        times.name = ts_col + '_time'
        df = pd.concat([df, dates, times], axis=1)
#            # Search column entries for str that look like dates and times
#            dates = extract_from_timestamps(df[ts_col], extract='date')
#            times = extract_from_timestamps(df[ts_col], extract='time')
##            dates.name = ts_col + '_date'
##            times.name = ts_col + '_time'
##            df = pd.concat([df, dates, times], axis=1)
#            # TODO: Should we overwrite the original column, or add additional
#            # column(s) similar to commented code above?
#            dayfirst = infer_dayfirst(dates)
#            df[ts_col] = dates + ' ' + times
#            df[ts_col] = pd.to_datetime(df[ts_col], dayfirst=dayfirst)
#        # Check if dates are sensible
#        dates = df[ts_col].apply(extract_from_datetime, args=('date',))
#        invalid_dates = ((dates < datetime.date(1980, 1, 1)) |
#                        (dates >= datetime.date.today()))
#        dates[invalid_dates] = None
#        times = df[ts_col].apply(extract_from_datetime, args=('time',))
#        invalid_times = (times == datetime.time(0, 0))
#        times[invalid_times] = None
#        dates.name = ts_col + '_date'
#        times.name = ts_col + '_time'
#        df = pd.concat([df, dates, times], axis=1)
    for date_col in all_column_types['date']:
        if not column_exists(df, date_col): continue
        dayfirst = infer_dayfirst(df[date_col])
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=dayfirst)
        # Check for valid dates
        date_col_invalid_dates = invalid_dates.get(date_col, {})
        dates, times = validate_datetimes(df[date_col],
                                          invalid_dates=date_col_invalid_dates)
        df[date_col] = dates
    for time_col in all_column_types['time']:
        if not column_exists(df, time_col): continue
        df[time_col] = pd.to_datetime(df[time_col])
#        # Necessary to work with 24hr times (am/pm)
#        df[time_col] = df[time_col].apply(convert_time)
        # TODO: I considered storing as a timedelta type (native to pandas),
        # but I think ultimately it will be easier to store as datetime.time
#        df[time_col] = pd.to_timedelta(df[time_col])
        # Check for valid times
        time_col_invalid_times = invalid_times.get(time_col, {})
        dates, times = validate_datetimes(df[time_col],
                                          invalid_times=time_col_invalid_times)
        df[time_col] = times
        # TODO: If time is explicitly given as midnight in a time column, assume
        # it is a true time? Unlike a midnight in a timestamp. Note: If all
        # time entries are midnight, we should probably just avoid using that
        # column. Could rely on user to check, or remove column if all values
        # are midnight.
        all_midnight = np.all(df[time_col] == datetime.time(0, 0, 0))
        if all_midnight:
            df.drop(time_col, inplace=True)
    for interval_col in all_column_types['interval']:
        if not column_exists(df, interval_col): continue
        re_date = re_template_date.format('[/-]')
        # Feels a bit messy to do it this way
        date_matches = df[interval_col].str.extractall(re_date).unstack()[0]
        if len(date_matches.columns) == 1:
            message = (f'WARNING: `{interval_col}` column is an interval column '
           'containing only one date. Treating column as date column.')
            logging.warning(message)
            print(message)
            date_matches = date_matches.squeeze()
            dayfirst = infer_dayfirst(date_matches)
            date_matches = pd.to_datetime(date_matches, infer_datetime_format=True,
                                          dayfirst=dayfirst)
            df[interval_col] = date_matches.apply(extract_from_datetime, args=('date',))
        elif len(date_matches.columns) == 2:
            start = date_matches.iloc[:, 0]
            end = date_matches.iloc[:, 1]
            dayfirst_start = infer_dayfirst(start)
            dayfirst_end = infer_dayfirst(end)
            start = pd.to_datetime(start, infer_datetime_format=True,
                                   dayfirst=dayfirst_start)
            end = pd.to_datetime(end, infer_datetime_format=True,
                                   dayfirst=dayfirst_end)
            start = start.apply(extract_from_datetime, args=('date',))
            end = end.apply(extract_from_datetime, args=('date',))
            start.name = interval_col + '_start'
            end.name = interval_col + '_end'
            pd.concat([df, start, end], axis=1, inplace=True)
            # TODO While the following would be simpler, it cannot perform
            # independent dayfirst inference for start and end. Is that really
            # necessary, when we could infer dayfirst for one or both columns
            # and assume that both columns are formatted in the same way.
#            date_matches.columns = [interval_col + '_start',
#                                    interval_col + '_end']
#            dayfirst = infer_dayfirst(date_matches.iloc[:, 0])
#            date_matches = date_matches.apply(pd.to_datetime,
#                                              kwargs={'infer_datetime_format': True,
#                                                      'dayfirst': dayfirst})
#            date_matches = date_matches.apply(extract_from_datetime, args=('date',))
#            pd.concat([df, date_matches], axis=1, inplace=True)  # TODO: Check if this behaves correctly!
        else:
            message = (f'WARNING: `{interval_col}` column is an interval column '
                       'containing more than two dates.')
            logging.warning(message)
            print(message)
    for num_col in all_column_types['numeric']:
        if not column_exists(df, num_col): continue
        # Check type of column
        if pd.api.types.is_object_dtype(df[num_col]):  # pandas failed to convert str
            num_strings = df[num_col].str.extract(r'([+-]?(?:\d*\.)?\d+(?:[eE][+-]?\d+)?)', expand=False)
            df[num_col] = pd.to_numeric(num_strings, downcast='integer')
        elif pd.api.types.is_numeric_dtype(df[num_col]):
            continue
        else:
            message = (f'WARNING: `{num_col}` column is not numeric and '
                       'numeric data cannot be extracted from it.')
            logging.warning(message)
            print(message)
    return df


def convert_units(df, values_to_units={}, to_units={}, decimal_places={},
                  add_unit_columns=False, remove_unit_columns=True,
                  recognized_units={}):
    for value_col, unit in values_to_units.items():
        if not column_exists(df, value_col): continue
        if column_exists(df, unit, log=False, stdout=False):
            # Convert the units
            df[unit] = df[unit].apply(unit_converter, args=('replace',),
                                      kwds=recognized_units)
            quantities = df[value_col] * df[unit]
        else:
            try:
                unit = ureg.parse_expression(str(unit)).units
            except UndefinedUnitError:
                raise ValueError(f'Invalid unit {unit!r} provided as a value in '
                                 '`value_to_unit` argument.')
            else:
                quantities = df[value_col].apply(lambda x: x * unit)
        # Perform conversion
        try:
            conversion_unit = to_units[value_col]
        except KeyError:
            raise ValueError('Argument for parameter `to_units` must contain '
                             'the same keys as `value_to_unit`. '
                             f'Missing key: "{value_col}".')
        try:
            conversion_unit = ureg.parse_expression(str(conversion_unit)).units
        except UndefinedUnitError:
            raise ValueError(f'Invalid unit `{conversion_unit}` provided as '
                             'a value in `to_units` argument.')
        try:
            df[value_col] = quantities.apply(convert_quantity, args=(conversion_unit,))
        except DimensionalityError:
            raise ValueError(f'Units for values in column "{value_col}" are incompatible '
                             f'for conversion to unit `{unit}` provided for this '
                             'column in the `to_units` argument.')
        if add_unit_columns:
            unit_col_name = value_col + '_unit'
            df[unit_col_name] = conversion_unit
        if column_exists(df, unit, log=False, stdout=False):
            if remove_unit_columns:
                df.drop(unit, axis=1, inplace=True)
            else:
                # Provide the correct units in an existing column
                df[unit] = conversion_unit
    # Round values in columns
    for value_col, num_dp in decimal_places.items():
        message = (f'WARNING: "{value_col}" column was not found in this DataFrame. '
                   'This column was given as a key in `decimal_places`. '
                   'Rounding for this column was skipped.')
        if not column_exists(df, value_col, log_msg=message):
            continue
        df[value_col] = df[value_col].round(num_dp)
    # TODO: Alternatively (instead of loop), we round all the columns specified
    # in decimal places using pandas.DataFrame.round(). This will silently
    # ignore columns in df that are not in decimal_places and vice versa.
#    df = df.round(decimal_places)
    return df

# Note: After parsing and converting the data, we will want to select columns,
# select rows of interest. And also renaming columns to something more sensible.
# We can use standard pandas functions to achieve this (no need to write
# separate functions, I think).
# Our goal is to unify the representation of the data. We should really
# adopt and adhere to standards, otherwise we will just contribute to the mess
# of heterogenity that hinders progress.


# HELPER CONVERTER FUNCTIONS

## TODO: Maybe move these to a separate file so they don't clutter things up.
## They will probably be source-specific e.g. different converters for Qiita
## and other (future) sources.
#
# These should return values that are as close as possible (ideally identical)
# to the values expected by the database model so that we don't have to do much
# more conversion. In fact, this should be the purpose of invoking parse_file
# convert_units in general.
#
# Take convert_sex as one example:
# In the database model, we model this as an enumeration with 4 possible values:
# not known, male, female, and not applicable. Not applicable is interpreted as
# "not relevant for a particular object/purpose".
# Note: Difference between "biological sex" and "gender identity".
# I tried to follow the concept of ISO/IEC 5218:2004, but avoid
# numerical encoding because it is less intuitive - you would need to know
# that the database is following this ISO exactly to interpret the integer
# values.
# Note: Other encodings exist. For more info see:
# https://epub.uni-regensburg.de/35886/1/SHTI228-0344.pdf
# As I am uncertain of the regex to construct for 'not applicable' I have left
# it out. The database will likely only store human or mouse info (but if it
# ever stored info on plants, 'not applicable' might be useful).
def convert_sex(value):
    """Convert an object/str to a sex."""
    value = str(value)
    if re_male.match(value):
        return 'male'
    elif re_female.match(value):
        return 'female'
    else:
        return 'not known'


def convert_time(value):
    """Convert an object/str to a datetime.time object."""
    value = str(value)
    try:
        time = dateutil.parser.parse(value).time()
        return time
    except TypeError:
        return None


def convert_csection(value):
    """Convert an object/str to a delivery mode (csection)."""
    value = str(value)
    if re_csection.match(value):
        return True
    elif re_vaginal.match(value):
        return False
    else:
        return None


def convert_timestamps(timestamps, date_sep='/-'):
    date_sep_pattern = '[' + ''.join(list(date_sep)) + ']'
    dates = timestamps.str.split('s+')[0]  # Assume whitespace cannot be date separator
    try:
        dayfirst = infer_dayfirst(dates, date_sep=date_sep)
        timestamps.replace(date_sep_pattern)
        timestamps = pd.to_datetime(timestamps, dayfirst=dayfirst)
    except ValueError:
        # Search column entries for str that look like dates and times
        dates = extract_from_timestamps(timestamps, extract='date')
        times = extract_from_timestamps(timestamps, extract='time')
#            dates.name = ts_col + '_date'
#            times.name = ts_col + '_time'
#            df = pd.concat([df, dates, times], axis=1)
        # TODO: Should we overwrite the original column, or add additional
        # column(s) similar to commented code above?
        dayfirst = infer_dayfirst(dates, date_sep=date_sep)
        timestamps = dates + ' ' + times
        timestamps = pd.to_datetime(timestamps, dayfirst=dayfirst)
    return timestamps


def validate_datetimes(datetimes, invalid_dates={}, invalid_times={},
                        raise_or_replace='replace'):
    # Check if dates are sensible
    # TODO: Move invalid dates and times into parameters invalid_dates and
    # invalid_times. These could take list_likes of strings (that can be
    # parsed into datetime objects) and include date ranges, as well as
    # individual dates. You could add support for an open ended date/time
    # interval, as we do below (e.g. '-1980' or 'today-'). And, even better,
    # get parameters to take a dictionary, so that you can supply invalid
    # dates and times per column.
    # Provide some sensible defaults if no values specified in dictionary
    # Note: These defaults should probably be specified by the user in
    # some configuration file, rather than hard-coded.
    # First create shallow copies of dicts (avoid modifying passed-in dicts)
    invalid_dates = invalid_dates.copy()
    invalid_times = invalid_times.copy()
    # Earlier than 1980 or later than and including today => invalid date
    invalid_dates.setdefault('before', datetime.date(1980, 1, 1))
    invalid_dates.setdefault('after', datetime.date.today())
    invalid_dates.setdefault('strict_before', True)
    invalid_dates.setdefault('strict_after', False)
    # Midnight => invalid time
    invalid_times.setdefault('invalid_times', [datetime.time(0, 0)])
    dates = datetimes.apply(extract_from_datetime, args=('date',))
    times = datetimes.apply(extract_from_datetime, args=('time',))
    invalid_dates = dates.apply(is_invalid_date, **invalid_dates)
    invalid_times = times.apply(is_invalid_time, **invalid_times)
    if raise_or_replace == 'replace':
        dates[invalid_dates] = None
        times[invalid_times] = None
        return dates, times
    elif raise_or_replace == 'raise':
        if np.any(invalid_dates | invalid_times):
            raise ValueError('Invalid datetime detected.')
    else:
        raise ValueError('Invalid argument for `raise_or_replace`.')



# TODO: Should we model latitude and logitude as a Point type in PostgreSQL?


def extract_numeric(numeric_str):
    pass



# VALIDATION FUNCTIONS

# TODO: Should we go for the more general is_invalid_ran
def is_in_invalid_interval(element, before=None, after=None, strict_before=True,
                    strict_after=True, invalid_elements=[], invalid_ranges=[]):
    invalid_elements = set(invalid_elements)
    if element in invalid_elements:
        return True
    if before:
        if strict_before:
            is_before = element < before
        else:
            is_before = element <= before
        if is_before:
            return True
    if after:
        if strict_after:
            is_after = element > after
        else:
            is_after = element >= after
        if is_after:
            return True
    if invalid_ranges:
        for start, end in invalid_ranges:
            if start > end:
                raise ValueError(f'Invalid {type(element)} range ({start}, {end}). '
                                 f'Start {start} is after {end}.')
            if (element >= start) and (element <= end):
                return True
    return False


def is_invalid_date(date, before=None, after=None, strict_before=True,
                    strict_after=True, invalid_dates=[], invalid_ranges=[]):
    return is_in_invalid_interval(date, before=before, after=after,
                               strict_before=strict_before, strict_after=strict_after,
                               invalid_elements=invalid_dates,
                               invalid_ranges=invalid_ranges)


def is_invalid_time(time, before=None, after=None, strict_before=True,
                    strict_after=True, invalid_times=[], invalid_ranges=[]):
    return is_in_invalid_interval(time, before=before, after=after,
                               strict_before=strict_before, strict_after=strict_after,
                               invalid_elements=invalid_times,
                               invalid_ranges=invalid_ranges)


def column_exists(df, column, log=True, stdout=True, log_msg=None):
    """Check whether a column exists in a given pandas DataFrame."""
    if column in df.columns:
        return True
    if log:
        if log_msg:
            log_msg = str(log_msg)
        else:
            log_msg = (f'WARNING: `{column}` column is not used in this DataFrame. '
                       'Processing for this column was skipped.')
        logging.warning(log_msg)
    if stdout:
        print(log_msg)
    return False


def is_missing(value):
    if re_missing.match(value):
        return True
    return False


def replace_missing(df, inplace=False):
    new_df = df.replace(to_replace=re_missing, value=np.nan, inplace=inplace)
    if not inplace:
        return new_df


# DATE AND TIME UTILITY FUNCTIONS

# TODO: Might need to make a better date and time regex. We currently don't
# validate the digits used in the date or time. So e.g. '3000-32-32 25:61:61 pm'
# would still provide a date and time to extract! I think the regex matching is
# currently faster than it would be if we fixed it to also validate.
# We can always leave the validation to another tool and simply state that this
# function does not validate.
def extract_from_timestamp(timestamp, extract='both', date_sep='/-', time_sep=':'):
    """Extract date or time from a string that contains a date and time.

    The given string `timestamp` might look like a timestamp, but this function
    will extract the leftmost date and time that can be found in the string
    (regardless of whether it looks like a timestamp).
    The function assumes a date is formatted like nn?.nn?.nnnn, nnnn.nn?.nn?,
    nn?.nn?.nn?, nn?.nn?, nnnn, nn?, where 'n' is a digit and '.' is a separator
    specified by the `date_sep` parameter, and '?' indicates that the preceding
    digit may or may not be present.
    The function assumes a time is formatted like nn?.nn?.nn?, nn?.nn? where 'n'
    and '?' are interpreted as above, and '.' is a separator specified by the
    `time_sep` parameter. Times can be followed by a case-insensitive 'AM' or
    'PM' (possibly preceded by whitespace). These are also included in the
    extracted time, but intervening whitespace is reduced to a single space.

    Parameters
    ----------
    timestamp : str
        String containing a date or time (to extract).
    extract : str {'both', 'date', 'time'}
        The substring to extract. If 'both', then both date and time will be
        extracted.
    date_sep : str or iterable of str
        Separators that may be present in the date substring of`timestamp`
        (the default is '/-').
    time_sep : str or iterable of str
        Separators that may be present in the time substring of `timestamp`
        (the default is ':').

    Returns
    -------
    str or tuple
        If `extract` == 'date' or `extract` == 'time', then the extracted date
        substring or time substring is returned, respectively.
        If `extract` == 'both', then a tuple of strings (date, time) is
        returned.
    """
    date_sep = '[' + ''.join(list(date_sep)) + ']'
    time_sep = '[' + ''.join(list(time_sep)) + ']'
    re_date = re_template_date.format(date_sep)
    re_time = re_template_time.format(time_sep)
    if extract == 'date':
        date_match = re.search(re_date, str(timestamp))
        if date_match:
            return date_match.group()
    elif extract == 'time':
        time_match = re.search(re_time, str(timestamp), flags=re.I)
        if time_match:
            return time_match.group()
    elif extract == 'both':
        date_match = re.search(re_date, str(timestamp))
        time_match = re.search(re_time, str(timestamp))
        date = date_match.group() if date_match else None
        time = time_match.group() if time_match else None
        # To squeeze all whitespace to a single space
        time = ' '.join(time.split())
        return date, time
    else:
        raise ValueError('Invalid value for `extract`.')


def extract_from_datetime(datetime, extract='both'):
    """Extract dates or times from a datetime object.

    Parameters
    ----------
    datetime : datetime_like
        Datetime object from which to extract a `datetime.date` or `datetime.time`.
    extract : str {'both', 'date', 'time'}
        The part of the datetime to extract. If 'both', then both date and
        time will be extracted.
    """
    try:
        date = datetime.date()
        time = datetime.time()
    except (AttributeError, ValueError):
        # Note: Apparently NaT.time() will raise a ValueError rather than
        # the expected AttributeError, as raised by np.nan.time().
        if extract == 'both':
            return (None, None)
        else:
            return None
    else:
        if extract == 'both':
            return (date, time)
        elif extract == 'date':
            return date
        elif extract == 'time':
            return time
        else:
            raise ValueError('Invalid value for `extract`.')


# My initial idea here was to be very flexible (to accomodate arbitrary strings
# which might contain text as well as timestamps). Ultimately, I think it will
# be easier to assume that a column contains well formed timestamps and
# simply convert to datetime.
# TODO: Should we accept time intervals in the timestamp, or should we have
# another function to extract start and end datetimes from time interval
# strings? I think we should include it in this function...
def extract_from_timestamps(timestamps, extract='both', date_sep='/-', time_sep=':'):
    """Extract dates or times from timestamps.

    Parameters
    ----------
    timestamps : str or iterable of str
        Timestamp string(s) from which to extract a date or time substring.
    extract : str {'both', 'date', 'time'}
        The part of the timestamp to extract. If 'both', then both date and
        time will be extracted.
    date_sep : str or iterable of str
        Separators that may be present in the date part of given `timestamps`
        (the default is '/-').
    time_sep : str or iterable of str
        Separators that may be present in the time part of given `timestamps`
        (the default is ':').

    Returns
    -------
    pandas Series or pandas DataFrame
        If `extract` == 'date' or `extract` == 'time', then a pandas Series
        containing either date strings or time strings is returned.
        If `extract` == 'both', then a pandas DataFrame with two columns
        (containing date and time strings) is returned.
    """
    date_sep = '[' + ''.join(list(date_sep)) + ']'
    time_sep = '[' + ''.join(list(time_sep)) + ']'
    re_date = f'((?:\d{{1,2}}{date_sep}|\d{{4}}{date_sep})?(?:\d{{1,2}}{date_sep})?(?:\d{{4}}|\d{{1,2}}))'
    re_time = f'(\d{{1,2}}{time_sep}\d{{1,2}}(?:{time_sep}\d{{1,2}})?\s*(?:am|pm)?)'
    timestamps = pd.Series(timestamps)
    dates = timestamps.str.extract(re_date, expand=False)
    times = timestamps.str.extract(re_time, flags=re.I, expand=False)
    dates.name = 'date'
    times.name = 'time'
    if extract == 'date':
        return dates
    elif extract == 'time':
        return times
    elif extract == 'both':
        return pd.concat([dates, times], axis=1)
    else:
        raise ValueError('Invalid value for `extract`.')


def infer_dayfirst(dates, date_sep='/-', default=False):
    """Check whether dates have a format with day first.

    Parameters
    ----------
    dates : str or iterable of str
        Date string(s) to check.
    date_sep : str or iterable of str
        Separators that may be present in the given `dates` (the default is '/-').
    default : bool
        Indicating the default value to return if the date cannot be inferred
        (the default is False, meaning the day is not first).

    Returns
    -------
    bool
        True if day is inferred to come first in the date format, False otherwise.
    """
    dates = pd.Series(dates)
    dates.dropna(inplace=True)
    date_sep_pattern = '[' + ''.join(list(date_sep)) + ']'
    date_df = dates.str.split(date_sep_pattern, expand=True)
    date_df = date_df.astype('int')
    if len(date_df.columns) < 3:
        # Assume that mm/yyyy, mm/yy or yyyy are given.
        return False
    # Check if year first
    if np.any(date_df[0].apply(lambda x: len(str(x)) == 4)) or np.any(date_df[0] > 31):
        return False
    # Check day position
    day_first = np.all(date_df[0] <= 31) and np.any(date_df[0] > 12)
    day_second = np.all(date_df[1] <= 31) and np.any(date_df[1] > 12)
    # Note: If year is specified as 2 digits, the format could still be yy/mm/dd
    # I think we can assume that if a year is specified first it will be
    # given as 4 digits i.e. yyyy/mm/dd!
    if day_first and day_second:
        print('WARNING: Malformed dates or mixtures of date formats detected. '
              f'Returned default: {default}')
        return default
    elif day_first:
        return True
    elif day_second:
        return False
    else:
        # Date format could not be determined unambiguously
        return default


# UNIT CONVERSION (UTILITIES)

# Dict linking lowercase strings found in dataframe to valid ureg units
# TODO Could use values as units themselves e.g. ureg.years to save a call to
# ureg.parse_expression()
recognized_units = {'y': 'years'}

class UnrecognizedUnitError(ValueError):
    pass

def convert_quantity(quantity, conversion_unit, value_only=True):
    try:
        converted_quantity = quantity.to(conversion_unit)
        if value_only:
            return converted_quantity.magnitude
        else:
            return converted_quantity
    except AttributeError:
        return np.nan

# TODO: Might be a pointless function
def create_quantity(value, unit):
    try:
        return value * unit
    except (AttributeError, TypeError):
        return np.nan

def unit_converter(string, raise_or_replace='raise', recognized_units={},
                   *args, **kwds):
    """Convert a string to a pint Unit object.

    Parameters
    ----------
    string : str
        A string (representing a unit of measurement) to convert to a pint
        Unit object.
    raise_or_replace : {'raise', 'replace'}
        If `raise_or_replace` == 'raise', an exception will be raised if the
        string cannot be converted to a pint Unit.
        If `raise_or_replace` == 'replace', then np.nan will be returned.

    Returns
    -------
    unit : pint Unit or np.nan
        Returns a pint Unit if `string` can be converted to a pint Unit.
        Returns np.nan if the `string` cannot be converted to a pint Unit and
        `raise_or_replace` == 'replace'.

    Raises
    ------
    UnrecognizedUnitError
        If the given `string` is not a recognized unit and cannot be converted
        to a pint Unit, and `raise_or_replace` == 'raise'.
    """
    string = str(string).lower()
    try:
        unit = ureg.parse_expression(string).units
    except UndefinedUnitError:
        try:
            new_string = recognized_units[string]
            # TODO: Should recognized units depend on column i.e. should we
            # have recognized_units[column][string] and pass the column as arg
            # to this function?
        except KeyError:
            if raise_or_replace == 'raise':
                raise UnrecognizedUnitError('String was not a recognized unit '
                                            'and could not be parsed into '
                                            'a pint Unit.')
            elif raise_or_replace == 'replace':
                return np.nan
            else:
                raise ValueError("Invalid value for `raise_or_replace`. "
                                 "Valid values are 'raise' or 'replace'.")
        try:
            unit = ureg.parse_expression(new_string).units
        except UndefinedUnitError:
            if raise_or_replace == 'raise':
                raise UnrecognizedUnitError('Invalid unit provided as value in '
                                            '`recognized_units` dictionary.')
            elif raise_or_replace == 'replace':
                return np.nan
            else:
                raise ValueError("Invalid value for `raise_or_replace`. "
                                 "Valid values are 'raise' or 'replace'.")

    return unit


if __name__ == '__main__':
    pass
