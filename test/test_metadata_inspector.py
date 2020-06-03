# -*- coding: utf-8 -*-
"""
Metadata inspector tests

Created on Mon Jan 20 11:29:05 2020

@author: William
"""

import unittest
import tempfile
import shutil
import csv
import json
import os
import re
from collections import defaultdict, Counter
from debug_tools import metadata_inspector as mi


class TestMetadata(unittest.TestCase):
    header = ['attr1', 'attr2', 'attr3']
    data = [[11, 12, 13], [21, 22, 23], [31, 32, 33]]

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.sid = os.path.basename(self.test_dir)
        self.sample_filename = f'{self.sid}_(random_text_here).txt'
        self.prep_filename = f'{self.sid}_prep_(random)_qiime_(random_text_here).txt'
        self.sample_file_path = os.path.join(self.test_dir, self.sample_filename)
        self.prep_file_path = os.path.join(self.test_dir, self.prep_filename)
        self.non_matching_filename = 'some_random_file.txt'
        # Note: This filename would match if the test_dir name happens to be 'some'
        self.non_matching_filepath = os.path.join(self.test_dir,
                                                  self.non_matching_filename)
        self.attribute_set = ['attr1', 'attr2']

    def tearDown(self):
        # Remove directory after test
        shutil.rmtree(self.test_dir)

    def test_create_sample_regex(self):
        sample_regex = mi.create_metadata_filename_regex(metadata='sample',
                                                         study_id=self.sid)
        expected_sample_regex = re.compile(r'^{}_(?!prep).*'.format(self.sid))
        self.assertEqual(sample_regex, expected_sample_regex)

    def test_create_prep_regex(self):
        prep_regex = mi.create_metadata_filename_regex(metadata='prep',
                                                       study_id=self.sid)
        expected_prep_regex = re.compile(r'^{}_prep_.*?_qiime.*'.format(self.sid))
        self.assertEqual(prep_regex, expected_prep_regex)

    def test_create_regex_unknown_metadata_type(self):
        with self.assertRaises(ValueError):
            mi.create_metadata_filename_regex(metadata='unknown',
                                              study_id=self.sid)

    def test_get_attribute_list_sample(self):
        with open(self.sample_file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(self.header)
            writer.writerows(self.data)
        header = mi.get_attribute_list(self.test_dir, metadata='sample')
        self.assertEqual(header, self.header)

    # TODO: I don't think there's much point in this test. It tests whether the
    # self.prep_filename is matched by the regex created by
    # create_metadata_filename_regex.
    def test_get_attribute_list_prep(self):
        with open(self.prep_file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(self.header)
            writer.writerows(self.data)
        header = mi.get_attribute_list(self.test_dir, metadata='prep')
        self.assertEqual(header, self.header)

    # TODO: We don't handle an unknown dir in any way, so the default Python
    # exception is raised. I don't think there's any point in testing this.
    def test_get_attribute_list_unknown_dir(self):
        with self.assertRaises(FileNotFoundError):
            mi.get_attribute_list('./some/non-existent/dir', metadata='prep')

    def test_get_attribute_list_no_matching_files(self):
        with open(self.non_matching_filepath, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(self.header)
            writer.writerows(self.data)
        with self.assertRaises(mi.NoMatchingFileError):
            mi.get_attribute_list(self.test_dir, metadata='sample')

    def test_get_attribute_list_unknown_metadata_type(self):
        with open(self.sample_file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(self.header)
            writer.writerows(self.data)
        with self.assertRaises(ValueError):
            mi.get_attribute_list(self.test_dir, metadata='unknown')

    def test_write_attribute_map_template_to_json(self):
        output_file = os.path.join(self.test_dir, 'test.json')
        mi.write_attribute_map_template_to_json(self.attribute_set, output_file)
        file_exists = os.path.isfile(output_file)
        self.assertTrue(file_exists)
        with open(output_file, 'r', newline='') as f:
            content = json.loads(f.read())
        expected_content = {'attr1': '', 'attr2': ''}
        self.assertEqual(content, expected_content)


class TestMetadataGeneral(unittest.TestCase):
    header = ['attr1', 'attr2', 'attr3']
    data = [[11, 12, 13], [21, 22, 23], [31, 32, 33]]

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.dirs = {}
        for i in range(2):
            sid = str(i)
            self.dirs[sid] = os.path.join(self.test_dir, sid)
            os.mkdir(self.dirs[sid])
            sample_filename = f'{sid}_(random_text_here).txt'
            prep_filename = f'{sid}_prep_(random)_qiime_(random_text_here).txt'
            sample_file_path = os.path.join(self.dirs[sid], sample_filename)
            prep_file_path = os.path.join(self.dirs[sid], prep_filename)
            with open(sample_file_path, 'w', newline='') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(self.header)
                writer.writerows(self.data)
            with open(prep_file_path, 'w', newline='') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(self.header)
                writer.writerows(self.data)
#        self.non_matching_filename = 'some_random_file.txt'
#        # Note: This filename would match if the test_dir name happens to be 'some'
#        self.non_matching_filepath = os.path.join(self.test_dir,
#                                                  self.non_matching_filename)

    def tearDown(self):
        # Remove directory after test
        shutil.rmtree(self.test_dir)

    def test_get_attributes_by_study(self):
        attributes = mi.get_attributes_by_study(self.test_dir, metadata='sample')
        expected_attributes = dict.fromkeys(self.dirs.keys(), self.header)
        self.assertEqual(attributes, expected_attributes)

    def test_get_attributes_by_study_unknown_metadata_type(self):
        with self.assertRaises(ValueError):
            mi.get_attributes_by_study(self.test_dir, metadata='unknown')

    # TODO Is there a better way to construct the expected attributes dict?
    def test_get_attribute_values(self):
        expected_attributes = defaultdict(set)
        for i, attr in enumerate(self.header):
            for row in self.data:
                expected_attributes[attr].add(row[i])
        attributes = mi.get_attribute_values(self.test_dir, metadata='sample')
        self.assertEqual(attributes, expected_attributes)

    def test_get_attribute_values_unknown_metadata_type(self):
        with self.assertRaises(ValueError):
            mi.get_attribute_values(self.test_dir, metadata='unknown')


class TestMetadataDict(unittest.TestCase):
    header = ['attr1', 'attr2', 'attr3']
    data = [[11, 12, 13], [21, 22, 23], [31, 32, 33]]

    def setUp(self):
        self.attribute_name = 'attr1'
        self.non_attribute = 'nonexistent attribute'
        self.attribute_set_1 = ['attr1', 'attr2']
        self.attribute_set_2 = []
        self.attribute_set_3 = ['attr1', 'attr1']
        self.attribute_dict = {'1': ['attr1', 'attr2'],
                               '2': ['attr1', 'attr3'],
                               '3': ['attr3']}
        self.present_dict = {'attr1': ['1', '2'],
                             'attr2': ['1'],
                             'no_match_attr': ['3']}

    def tearDown(self):
        pass

    def test_get_studies_without_attribute(self):
        studies = mi.get_studies_without_attribute(self.attribute_name,
                                                   self.attribute_dict)
        expected_studies = ['3']
        self.assertEqual(studies, expected_studies)

    def test_get_studies_without_non_existent_attribute(self):
        studies = mi.get_studies_without_attribute(self.non_attribute,
                                                   self.attribute_dict)
        expected_studies = ['1', '2', '3']
        self.assertEqual(studies, expected_studies)

    def test_get_studies_without_attribute_empty_dict(self):
        studies = mi.get_studies_without_attribute(self.non_attribute, {})
        expected_studies = []
        self.assertEqual(studies, expected_studies)

    def test_get_studies_with_attribute(self):
        studies = mi.get_studies_with_attribute(self.attribute_name,
                                                self.attribute_dict)
        expected_studies = ['1', '2']
        self.assertEqual(studies, expected_studies)

    def test_get_studies_with_non_existent_attribute(self):
        studies = mi.get_studies_with_attribute(self.non_attribute,
                                                self.attribute_dict)
        expected_studies = []
        self.assertEqual(studies, expected_studies)

    def test_get_studies_with_attribute_empty_dict(self):
        studies = mi.get_studies_with_attribute(self.non_attribute, {})
        expected_studies = []
        self.assertEqual(studies, expected_studies)

    def test_count_attributes(self):
        counter = mi.count_attributes(self.attribute_dict)
        expected_counter = Counter({'attr1': 2, 'attr2': 1, 'attr3': 2})
        self.assertEqual(counter, expected_counter)

    def test_get_studies_by_missing_attributes(self):
        missing_attrs = mi.get_studies_by_missing_attributes(self.attribute_set_1,
                                                             self.attribute_dict)
        expected_missing_attrs = defaultdict(list)
        expected_missing_attrs.update({'attr1': ['3'],
                                       'attr2': ['2', '3']})
        self.assertEqual(missing_attrs, expected_missing_attrs)

    def test_get_studies_by_missing_attributes_empty_attr_set(self):
        missing_attrs = mi.get_studies_by_missing_attributes(self.attribute_set_2,
                                                             self.attribute_dict)
        expected_missing_attrs = defaultdict(list)
        self.assertEqual(missing_attrs, expected_missing_attrs)

    def test_get_studies_by_missing_attributes_duplicates_in_attr_set(self):
        missing_attrs = mi.get_studies_by_missing_attributes(self.attribute_set_3,
                                                             self.attribute_dict)
        expected_missing_attrs = defaultdict(list)
        expected_missing_attrs.update({'attr1': ['3']})
        self.assertEqual(missing_attrs, expected_missing_attrs)

    def test_get_studies_by_present_attributes(self):
        present_attrs = mi.get_studies_by_present_attributes(self.attribute_set_1,
                                                             self.attribute_dict)
        expected_present_attrs = defaultdict(list)
        expected_present_attrs.update({'attr1': ['1', '2'],
                                       'attr2': ['1']})
        self.assertEqual(present_attrs, expected_present_attrs)

    def test_get_studies_by_present_attributes_empty_attr_set(self):
        present_attrs = mi.get_studies_by_present_attributes(self.attribute_set_2,
                                                             self.attribute_dict)
        expected_present_attrs = defaultdict(list)
        self.assertEqual(present_attrs, expected_present_attrs)

    def test_get_studies_by_present_attributes_duplicates_in_attr_set(self):
        present_attrs = mi.get_studies_by_present_attributes(self.attribute_set_3,
                                                             self.attribute_dict)
        expected_present_attrs = defaultdict(list)
        expected_present_attrs.update({'attr1': ['1', '2']})
        self.assertEqual(present_attrs, expected_present_attrs)

    def test_get_similar_attributes(self):
        attrs = mi.get_similar_attributes(r'attr\d', self.present_dict)
        expected_attrs = ['attr1', 'attr2']
        self.assertEqual(attrs, expected_attrs)

    def test_get_study_set_from_attrs(self):
        studies = mi.get_study_set_from_attrs(self.attribute_set_1,
                                              self.present_dict)
        expected_studies = {'1', '2'}
        self.assertEqual(studies, expected_studies)


if __name__ == '__main__':
    unittest.main()
