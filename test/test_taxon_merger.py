# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 08:51:35 2020

@author: William
"""

import unittest
import os
import io
import pandas as pd

from creator.taxon_merger import aggregate_at_taxon_level

# Change this variable to True if you want to generate new output files to
# generate comparison text for use in tests.
generate_output_files = False
# Note: to generate strings for StringIO below, we simply parse each line in
# the output files using a find and replace regex. But this little helper
# function works too!
def convert_to_stringio(filename):
    """Convert a file to a valid stringIO representation."""
    with open(filename) as f:
        for line in f:
            print(repr(line))

def get_all_stringio(directory):
    """Convert all csv files in the given directory to a stringIO equivalent."""
    for dir_entry in os.scandir(directory):
        if dir_entry.is_file() and dir_entry.name.lower().endswith('csv'):
            print(dir_entry.name + ':')
            convert_to_stringio(dir_entry)
            print()

class TaxonMergerTest(unittest.TestCase):
    def setUp(self):
        self.columns = ['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id', 'lineage_id', 'seq_var_id', 'count', 'id', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
        self.grouping_cols = ['experiment_id', 'subject_id', 'sample_id', 'sample_time_id', 'sample_site_id', 'preperation_id', 'workflow_id']
        self.taxon_cols = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
        self.count_col = 'count'
        self.header = ','.join(self.columns)
        self.data = io.StringIO(
            '11,607,3001,2125,33,10,32,41767,,1146,41767,k__Bacteria,p__Verrucomicrobia,c__Verrucomicrobiae,o__Verrucomicrobiales,f__Verrucomicrobiaceae,g__Akkermansia,s__muciniphila\n'
            '11,607,3001,2125,33,10,32,41802,,2,41802,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Ruminococcaceae,g__,s__\n'
            '11,607,3001,2125,33,10,32,42664,,1,42664,k__Bacteria,p__Bacteroidetes,c__Bacteroidia,o__Bacteroidales,f__Bacteroidaceae,g__Bacteroides,s__\n'
            '11,607,3001,2125,33,10,32,41803,,1,41803,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Ruminococcaceae,g__,s__\n'
            '12,608,3002,2126,33,11,33,41805,,42,41805,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Lachnospiraceae,g__,s__\n'
            '12,608,3002,2126,33,11,33,41971,,1,41971,k__Bacteria,p__Bacteroidetes,c__Bacteroidia,o__Bacteroidales,f__Bacteroidaceae,g__Bacteroides,s__\n'
            '12,608,3002,2126,33,11,33,42138,,1,42138,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Lachnospiraceae,g__,s__\n'
            '12,608,3002,2126,33,11,33,41811,,5,41811,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Lachnospiraceae,g__,s__\n'
            '13,609,3003,2127,33,12,34,41976,,2,41976,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Lachnospiraceae,g__,s__\n'
            '13,609,3003,2127,33,12,34,42223,,1,42223,k__Bacteria,p__Bacteroidetes,c__Bacteroidia,o__Bacteroidales,f__Bacteroidaceae,g__Bacteroides,s__\n'
            '13,609,3003,2127,33,12,34,41815,,1,41815,k__Bacteria,p__Firmicutes,c__Clostridia,o__Clostridiales,f__Ruminococcaceae,g__,s__\n'
            '13,609,3003,2127,33,12,34,41817,,1,41817,k__Bacteria,p__Bacteroidetes,c__Bacteroidia,o__Bacteroidales,f__Bacteroidaceae,g__Bacteroides,s__uniformis\n'
        )        
        self.data2 = io.StringIO(
            '1,1,1,1,1,1,1,1,,1,1,k__A,p__B,c__E,o__H,f__K,g__O,s__q\n'
            '1,1,1,1,1,1,1,2,,2,2,k__A,p__C,c__F,o__I,f__L,g__,s__\n'
            '1,1,1,1,1,1,1,3,,1,3,k__A,p__D,c__G,o__J,f__M,g__P,s__\n'
            '1,1,1,1,1,1,1,4,,2,4,k__A,p__C,c__F,o__I,f__L,g__,s__\n'
            '2,2,2,2,2,2,2,5,,1,5,k__A,p__C,c__F,o__I,f__N,g__,s__\n'
            '2,2,2,2,2,2,2,6,,2,6,k__A,p__D,c__G,o__J,f__M,g__P,s__\n'
            '2,2,2,2,2,2,2,7,,1,7,k__A,p__C,c__F,o__I,f__N,g__,s__\n'
            '2,2,2,2,2,2,2,8,,2,8,k__A,p__C,c__F,o__I,f__N,g__,s__\n'
            '3,3,3,3,3,3,2,9,,1,9,k__A,p__C,c__F,o__I,f__N,g__,s__\n'
            '3,3,3,3,3,3,2,10,,2,10,k__A,p__D,c__G,o__J,f__M,g__P,s__\n'
            '3,3,3,3,3,3,2,11,,1,11,k__A,p__C,c__F,o__I,f__L,g__,s__\n'
            '3,3,3,3,3,3,2,12,,2,12,k__A,p__D,c__G,o__J,f__M,g__P,s__r\n'
        )
        self.simple_sum = io.StringIO(
            'experiment_id,subject_id,sample_id,sample_time_id,sample_site_id,preperation_id,workflow_id,genus,count\n'
            '1,1,1,1,1,1,1,g__,4\n'
            '1,1,1,1,1,1,1,g__O,1\n'
            '1,1,1,1,1,1,1,g__P,1\n'
            '2,2,2,2,2,2,2,g__,4\n'
            '2,2,2,2,2,2,2,g__P,2\n'
            '3,3,3,3,3,3,2,g__,2\n'
            '3,3,3,3,3,3,2,g__P,4\n'
        )
        self.complex_sum_genus = io.StringIO(
            'experiment_id,subject_id,sample_id,sample_time_id,sample_site_id,preperation_id,workflow_id,kingdom,phylum,class,order,family,genus,count\n'
            '1,1,1,1,1,1,1,k__A,p__B,c__E,o__H,f__K,g__O,1\n'
            '1,1,1,1,1,1,1,k__A,p__D,c__G,o__J,f__M,g__P,1\n'
            '2,2,2,2,2,2,2,k__A,p__D,c__G,o__J,f__M,g__P,2\n'
            '3,3,3,3,3,3,2,k__A,p__D,c__G,o__J,f__M,g__P,4\n'
            '1,1,1,1,1,1,1,,,,,,,4\n'
            '2,2,2,2,2,2,2,,,,,,,4\n'
            '3,3,3,3,3,3,2,,,,,,,2\n'        
        )
        self.complex_sum_class = io.StringIO(
            'experiment_id,subject_id,sample_id,sample_time_id,sample_site_id,preperation_id,workflow_id,kingdom,phylum,class,count\n'
            '1,1,1,1,1,1,1,k__A,p__B,c__E,1\n'
            '1,1,1,1,1,1,1,k__A,p__C,c__F,4\n'
            '1,1,1,1,1,1,1,k__A,p__D,c__G,1\n'
            '2,2,2,2,2,2,2,k__A,p__C,c__F,4\n'
            '2,2,2,2,2,2,2,k__A,p__D,c__G,2\n'
            '3,3,3,3,3,3,2,k__A,p__C,c__F,2\n'
            '3,3,3,3,3,3,2,k__A,p__D,c__G,4\n'
        )
        self.simple_sum_custom_dummy = io.StringIO(
            'experiment_id,subject_id,sample_id,sample_time_id,sample_site_id,preperation_id,workflow_id,genus,count\n'
            '1,1,1,1,1,1,1,CUSTOM,4\n'
            '1,1,1,1,1,1,1,g__O,1\n'
            '1,1,1,1,1,1,1,g__P,1\n'
            '2,2,2,2,2,2,2,CUSTOM,4\n'
            '2,2,2,2,2,2,2,g__P,2\n'
            '3,3,3,3,3,3,2,CUSTOM,2\n'
            '3,3,3,3,3,3,2,g__P,4\n'
        )
        self.df = pd.read_csv(self.data2, header=None, names=self.columns)
    
    def test_simple_sum(self):
        out_df = aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                          simple_aggregation=True, 
                                          grouping_cols=self.grouping_cols)
        if generate_output_files:
            out_df.to_csv('simple_sum.csv', index=False)
        exp_df = pd.read_csv(self.simple_sum)
        self.assertTrue(out_df.equals(exp_df))
        
    def test_complex_sum_genus(self):
        out_df = aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                          simple_aggregation=False, 
                                          grouping_cols=self.grouping_cols)
        if generate_output_files:
            out_df.to_csv('complex_sum_genus.csv', index=False)
        exp_df = pd.read_csv(self.complex_sum_genus)
        self.assertTrue(out_df.equals(exp_df))        
    
    def test_complex_sum_class(self):
        out_df = aggregate_at_taxon_level(self.df, taxon_level='class', 
                                          simple_aggregation=False, 
                                          grouping_cols=self.grouping_cols)
        if generate_output_files:
            out_df.to_csv('complex_sum_class.csv', index=False)
        exp_df = pd.read_csv(self.complex_sum_class)
        self.assertTrue(out_df.equals(exp_df))  
    
    def test_simple_keep_all_cols(self):
        out_df = aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                          simple_aggregation=True, 
                                          grouping_cols=self.grouping_cols,
                                          keep_all_cols=True)
        out_columns = set(out_df.columns)
        exp_columns = set(self.df.columns).difference(self.taxon_cols)
        exp_columns.add('genus')
        self.assertEqual(out_columns, exp_columns) 
    
    def test_complex_keep_all_cols(self):
        out_df = aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                          simple_aggregation=False, 
                                          grouping_cols=self.grouping_cols,
                                          keep_all_cols=True)
        out_columns = set(out_df.columns)
        exp_columns = set(self.df.columns)
        exp_columns.remove('species')
        self.assertEqual(out_columns, exp_columns) 
    
    def test_custom_dummy_values(self):
        temp_df = self.df.copy()
        temp_df.loc[temp_df['genus'] == 'g__', 'genus'] = 'CUSTOM'
        print(temp_df)
        out_df = aggregate_at_taxon_level(temp_df, taxon_level='genus',
                                          dummy_values={'genus': 'CUSTOM'},
                                          simple_aggregation=True, 
                                          grouping_cols=self.grouping_cols)
        if generate_output_files:
            out_df.to_csv('simple_sum_custom_dummy.csv', index=False)
        exp_df = pd.read_csv(self.simple_sum_custom_dummy)
        self.assertTrue(out_df.equals(exp_df))
    
    # TEST EXCEPTION RAISING
    def test_unrecognized_dummy_value_key(self):
        with self.assertRaises(ValueError):
            aggregate_at_taxon_level(self.df, taxon_level='genus',
                                     dummy_values={'gibberish': 'g__'},
                                     simple_aggregation=True, 
                                     grouping_cols=self.grouping_cols)
    
    def test_unrecognized_taxon_level(self):
        with self.assertRaises(ValueError):
            aggregate_at_taxon_level(self.df, taxon_level='gibberish', 
                                     simple_aggregation=True, 
                                     grouping_cols=self.grouping_cols)
    
    def test_no_taxon_level_in_df(self):
        self.df.rename(columns={'genus': 'no_genus'}, inplace=True)
        with self.assertRaises(ValueError):
            aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                     simple_aggregation=True, 
                                     grouping_cols=self.grouping_cols)

    def test_not_all_required_taxon_cols_in_df(self):
        self.df.rename(columns={'kingdom': 'no_kingdom'}, inplace=True)
        with self.assertRaises(ValueError):
            aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                     simple_aggregation=True, 
                                     grouping_cols=self.grouping_cols)
    
    def test_not_all_required_grouping_cols_in_df(self):
        self.df.rename(columns={'experiment_id': 'NONE'}, inplace=True)
        with self.assertRaises(ValueError):
            aggregate_at_taxon_level(self.df, taxon_level='genus', 
                                     simple_aggregation=True, 
                                     grouping_cols=self.grouping_cols)


if __name__ == '__main__':
    # Run tests
    unittest.main()
    # Generate new stringio strings
    print()
    if generate_output_files:
        get_all_stringio('.')
