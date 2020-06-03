# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 08:09:55 2020

@author: William
"""

import os
import pandas as pd
from typing import Union

class NoTaxonLevelPresent(ValueError):
    pass


# TODO: Update this function to allow merging on sequence variants too?
def aggregate_at_taxon_level(df, taxon_level='genus', 
                             grouping_cols=[],
                             dummy_values={},
                             missing_method: Union['remove', 'zero_count', 'sum'] = 'sum',
                             check_brackets=True,
                             simple_aggregation=False,
                             keep_all_cols=False):
    """Aggregates counts for the a dataframe containing taxonomic count data.
    
    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        A long format count table, where each row corresponds to a count for
        a unit of interest (uniquely identified by other columns). In 
        particular, it should have a count column, columns of taxonomic
        information (kingdom, phylum, class, ...) and other columns relating
        to the unit of interest (usually a sample). All taxonomic column names
        should be lowercase.
    taxon_level : str
        The taxonomic level at which the counts should be aggregated. This can
        be any of the available columns of taxonomic information in df.
    dummy_values : dict
        A dictionary mapping taxonomic levels to dummy values for that level.
        Not all taxonomic levels need be provided. If some levels are omitted,
        their dummy values will default to the dummy value '<level>__', where 
        <level> is replaced by a taxonomic level e.g. kingdom, phylum, class etc.
    missing_method : str
        The way to handle the units of interest that are missing taxonomic data
        at the given taxon_level. 
        
        'remove' will remove all rows where the given taxonomic level is either 
        missing (recognized as NA by pandas) or a dummy value (default 
        dummy values are 'k__' for kingdom, 'p__' for phylum etc.). You can 
        override these default dummy values by supplying a dictionary mapping 
        taxonomic levels to taxonomic level-specific dummy values, using the 
        dummy_values parameter.
        
        'zero count' will set the count for each row where the given taxonomic 
        level is either missing (recognized as NA by pandas) or a dummy value.
        
        'sum' will sum all the counts (after grouping by the grouping_cols)
        for all rows where the given taxonomic level is either 
        missing (recognized as NA by pandas) or a dummy value. It will then
        append these rows with summed counts (using NA for all lineage 
        information) to the bottom of the returned DataFrame.
    check_backets : bool
        If True, then any values in the dataframe containing left and right 
        brackets, '[' and ']', will have these brackets removed. These brackets
        may result from proposed taxonomy in reference databases such as 
        Greengenes and could potentially interfere with aggregation over 
        taxonomic levels.
    grouping_cols : iterable
        The columns in the given df that you want to group by (excluding any 
        taxonomic columns). The chosen columns depend on the exact df. However,
        Qiita provide data on a per workflow basis. Thus, it may be necessary 
        group by most columns that are not the taxonomic or count columns. E.g.
        experiment_id, subject_id, sample_id, preparation_id, workflow_id.
    simple_aggregation : bool
        If True, then grouping of taxonomic columns is performed for only
        one specified taxon_level e.g. 'genus', assuming all genera names are
        unique (even across lineages). This is then the only available 
        taxonomic column in the returned DataFrame.
        If False, then grouping is performed for all taxonomic columns that
        correspond to taxonomic levels higher than the specified taxon level.
        E.g. if the taxon level is 'class', then grouping will occur on the
        columns 'class', 'phylum' and 'kingdom'.
    keep_all_cols : bool
        If True, all columns of the given df are also present in the returned 
        DataFrame (with the exception of irrelevant taxonomic cols (See 
        simple_aggregation parameter). Note, however, that any columns that 
        were not specified as grouping_cols and are not relevant taxonomic 
        columns may have been aggregated (together with count) so the values in
        these columns may not make any sense!
        If False, then only columns specified in grouping_cols and relevant
        taxonomic levels (See simple_aggregation parameter), together with an
        aggregated 'count' column are retained.
    
    Returns
    -------
    pandas.core.frame.DataFrame
        The same count table, as provided in input, but with counts aggregated
        to the given taxon_level and missing taxonomic data handled using the
        given missing_method.
    """
    # Don't mess around with original DataFrame
    df = df.copy()
    taxon_level = taxon_level.lower()
    recognized_taxon_levels = ['kingdom', 'phylum', 'class', 'order', 
                               'family','genus', 'species']
    dummy_levels = ['k__', 'p__', 'c__', 'o__', 'f__', 'g__', 's__']
    dummy_dict = dict(zip(recognized_taxon_levels, dummy_levels))
    # Process user-provided dummy values
    for key, value in dummy_values.items():
        key = key.lower()
        if key not in dummy_dict:
            raise ValueError('The key {key!r} in the given dummy_values is not '
                             'a recognized taxon level.')
        else:
            dummy_dict[key] = str(value)
    # Check that taxon_level is a valid argument
    if taxon_level not in recognized_taxon_levels:
        raise ValueError(f'The given taxon_level {taxon_level!r} is not a recognized taxon level. '
                         f'Please choose from: {recognized_taxon_levels}.')
    # Get taxonomic levels expected for proper merging
    taxon_level_index = recognized_taxon_levels.index(taxon_level)
    required_taxon_levels = recognized_taxon_levels[:taxon_level_index+1]
    required_taxon_levels = [level for index, level in enumerate(recognized_taxon_levels)
                             if index <= taxon_level_index]
    # Check which taxonomic levels are present as columns in df
    taxon_cols_present = df.columns.intersection(recognized_taxon_levels)
    # Note: columns_not_found is a list to preserve order of taxonomic levels
    columns_not_found = [level for level in required_taxon_levels 
                         if level not in taxon_cols_present]
    
    # Create custom repr string for pandas DataFrame for pretty error reporting
    df_class = repr(df.__class__).split('\'')[1]
    # TODO Might a problem formatting memory address like this. I chose this
    # format because it seemed to be similar to the format chosen by pandas
    # for some of their objects e.g. a DataFrameGroupBy object
    repr_df = '<{} object at 0x{:016X}>'.format(df_class, id(df))
    # Check that taxon_level is present in the pandas DataFrame columns
    if taxon_level not in taxon_cols_present:
        raise NoTaxonLevelPresent(
                f'The given df {repr_df} does not contain a column with the '
                f'same name as the given taxon_level {taxon_level!r}.')
    # Check that other required taxonomic columns are present in the pandas DataFrame
    if columns_not_found:
        raise NoTaxonLevelPresent(
                f'The given taxon_level {taxon_level!r} requires that higher '
                f'taxonomic levels {required_taxon_levels} are present in the '
                f'given df {repr_df}. Didn\'t find taxonomic columns: '
                f'{columns_not_found}.')
    
    # TODO: Implement checks to make sure grouping_cols are appropriate/present in dataframe?
    if grouping_cols:
        grouping_cols = list(grouping_cols)
        columns_not_found = [col for col in grouping_cols if col not in df.columns]
        if columns_not_found:
            raise ValueError('At least one of the supplied column names in '
                             f'grouping_cols is not present in given df {repr_df}. '
                             f'Didn\'t find columns: {columns_not_found}.')
    else:
        # Assumes all columns left of 'kingdom' (and other taxonomic columns) are grouping columns
        # TODO Is there a better assumption to make here? 
        upper_index = df.columns.get_loc('kingdom')
        grouping_cols = df.columns.names[:upper_index]
    if simple_aggregation:
        columns = grouping_cols + [taxon_level]
    else:
        columns = grouping_cols + required_taxon_levels
    # Check taxonomic columns for bracketed taxa names
    if check_brackets:
        df[taxon_cols_present] = df.loc[:, taxon_cols_present].replace(r'[\[\]]', '', regex=True)
        # Could also perform replacement like this, but possibly less efficient?
#        for col in taxon_cols_present:
#            df[col] = df[col].str.replace(r'[\[\]]', '', regex=True)
    # Perform aggregation
    grouped_df = df.groupby(columns)
    summed_df = grouped_df.sum()
    new_df = summed_df.reset_index()
    # At this stage, new_df is of the form we want if using simple_aggregation
    # and missing_method == 'sum'.
#    print(summed_df.head())
#    print(summed_df.columns)
    print(new_df.head())
    if not simple_aggregation:
        try:
            count_unknown_genera = summed_df.xs(dummy_dict[taxon_level], level=taxon_level, axis=0)
        except KeyError:
            # If the taxon level has no missing/dummy values
            new_rows = None
        else:
            new_rows = count_unknown_genera.groupby(grouping_cols).sum().reset_index()
#            print(new_rows.head())
#            print(new_df.shape)
            new_df = new_df[new_df[taxon_level] != dummy_dict[taxon_level]]
#            print(new_df.shape)
            new_df = new_df.append(new_rows, ignore_index=True, sort=False)
        
#        print(new_df.shape)
    if missing_method == 'sum':
        pass
    elif missing_method == 'remove':
        remove_condition = (new_df[taxon_level] == dummy_dict[taxon_level]) | (new_df[taxon_level].isna()) 
        new_df = new_df.loc[~remove_condition]
    elif missing_method == 'zero_counts':
        unique_lineages = new_df[required_taxon_levels].drop_duplicates()
        print(unique_lineages.shape)
        for name, group in new_df.groupby(grouping_cols):
            new_group = group.merge(unique_lineages, how='right', on=required_taxon_levels)
            print(new_group)
            print(new_group.shape)
            break
        # To set all missing/dummy counts equal to 0 [NOT DESIRED FUNCTIONALITY]
        new_df = summed_df.reset_index()
        new_df.loc[new_df[taxon_level] == dummy_dict[taxon_level], 'count'] = 0
    else:
        raise ValueError(f'The given {missing_method} is not valid. Please choose '
                         'from `sum`, `remove` or `zero_counts`.')
    # Filter for columns of interest
    if keep_all_cols:
        return new_df
    else:
        return new_df[columns + ['count']]
    
    
if __name__ == '__main__':
    os.chdir(r'C:\Users\William\Documents\OneDrive backup\Bioinformatics\Thesis\Data')
    ## TEST TO RAISE EXCEPTIONS
#    df = pd.read_csv('output3.csv')
#    aggregate_at_taxon_level(df, taxon_level='blah')
    
#    df = pd.read_csv('output3.csv')[[col for col in df.columns if col != 'kingdom']]
#    aggregate_at_taxon_level(df, taxon_level='kingdom')
#    aggregate_at_taxon_level(df, taxon_level='genus')
    
    ## TEST TO SUM COUNTS
#    df = pd.read_csv('output3.csv')[[col for col in df.columns if col not in ['seq_var_id', 'id', 'lineage_id']]]
    df = pd.read_csv('output3.csv')
#    new_df = aggregate_at_taxon_level(df, taxon_level='genus', grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'])
    # Check default grouping_cols functionality
#    new_df = aggregate_at_taxon_level(df, taxon_level='genus')
    # Check simple aggregation functionality
#    new_df = aggregate_at_taxon_level(
#            df, taxon_level='genus', 
#            grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'],
#            simple_aggregation=True)
    
#    new_df = aggregate_at_taxon_level(
#        df, taxon_level='genus', 
#        grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'])
    
#    new_df = aggregate_at_taxon_level(
#    df, taxon_level='genus', missing_method='remove', 
#    grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'])
    
#    new_df = aggregate_at_taxon_level(
#    df, taxon_level='genus', missing_method='remove', simple_aggregation=True,
#    grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'])
#
#    another_df = aggregate_at_taxon_level(
#    df, taxon_level='genus', missing_method='remove', simple_aggregation=True, check_brackets=False,
#    grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'])        
    new_df = aggregate_at_taxon_level(
        df, taxon_level='genus', 
        grouping_cols=['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id'],
        simple_aggregation=False,
        missing_method='zero_counts')