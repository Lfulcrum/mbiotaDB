# -*- coding: utf-8 -*-
"""
Created on Sat Aug 24 16:47:31 2019

@author: William
"""

# Standard library imports
import unittest
import os
import datetime
from collections import OrderedDict

# Local application imports
from creator.sample_parser import infer_date_formats
from creator.sample_parser import parse_sample, parse_subject, parse_objects
#from creator.sample_parser import ParsedObjects
from creator.prep_parser import parse_processings
from creator import ureg
from model import (Source, Experiment, Subject, Sample, SamplingSite, Time,
                   Preparation, SeqInstrument, Processing)

				   
class ModelAddTest(unittest.TestCase):

    def setUp(self):
        self.experiment1 = Experiment()
        self.subject1 = Subject(orig_subject_id=1)
        self.subject2 = Subject(orig_subject_id=2)
        self.sample1 = Sample(orig_sample_id=1)
        self.sample2 = Sample(orig_sample_id=2)
        self.sample3 = Sample(orig_sample_id=3)
        self.sample4 = Sample(orig_sample_id=4)
        # Set up relationships
        self.subject1._samples = {self.sample1, self.sample2}
        self.subject2._samples = {self.sample3}
        self.sample1._subject = self.subject1
        self.sample2._subject = self.subject1
        self.sample3._subject = self.subject2

    def tearDown(self):
        del self.experiment1
        del self.subject1
        del self.subject2
        del self.sample1
        del self.sample2
        del self.sample3
        del self.sample4

    def test_add_subject_to_experiment(self):
        # Attributes that should change
        expected_subject_experiments = {self.experiment1}
        expected_experiment_subjects = {self.subject1}
        expected_experiment_samples = {self.sample1, self.sample2}
        expected_sample_experiments = {self.experiment1}
        # Call the function
        self.experiment1.add_subject(self.subject1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject_experiments)
        self.assertEqual(self.subject2.experiments, set())
        self.assertEqual(self.experiment1.subjects, expected_experiment_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment_samples)
        self.assertEqual(self.sample1.experiments, expected_sample_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample_experiments)

    def test_add_experiment_to_subject(self):
        # Attributes that should change
        expected_subject_experiments = {self.experiment1}
        expected_experiment_subjects = {self.subject1}
        expected_experiment_samples = {self.sample1, self.sample2}
        expected_sample_experiments = {self.experiment1}
        # Call the function
        self.subject1.add_experiment(self.experiment1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject_experiments)
        self.assertEqual(self.subject2.experiments, set())
        self.assertEqual(self.experiment1.subjects, expected_experiment_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment_samples)
        self.assertEqual(self.sample1.experiments, expected_sample_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample_experiments)

    def test_add_sample_to_experiment(self):
        # Attributes that should change
        expected_subject_experiments = {self.experiment1}
        expected_experiment_subjects = {self.subject1}
        expected_experiment_samples = {self.sample1}
        expected_sample_experiments = {self.experiment1}
        # Call the function
        self.experiment1.add_sample(self.sample1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject_experiments)
        self.assertEqual(self.subject2.experiments, set())
        self.assertEqual(self.experiment1.subjects, expected_experiment_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment_samples)
        self.assertEqual(self.sample1.experiments, expected_sample_experiments)
        self.assertEqual(self.sample2.experiments, set())

    def test_add_experiment_to_sample(self):
        # Attributes that should change
        expected_subject_experiments = {self.experiment1}
        expected_experiment_subjects = {self.subject1}
        expected_experiment_samples = {self.sample1}
        expected_sample_experiments = {self.experiment1}
        # Call the function
        self.sample1.add_experiment(self.experiment1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject_experiments)
        self.assertEqual(self.subject2.experiments, set())
        self.assertEqual(self.experiment1.subjects, expected_experiment_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment_samples)
        self.assertEqual(self.sample1.experiments, expected_sample_experiments)
        self.assertEqual(self.sample2.experiments, set())

    # Where the subject is not linked to any experiment
    def test_add_sample_to_subject(self):
        # Attributes that should change
        expected_sample_subject = self.subject1
        expected_subject_samples = {self.sample1, self.sample2, self.sample4}
        # Call the function
        self.subject1.add_sample(self.sample4)
        self.assertEqual(self.subject1.samples, expected_subject_samples)
        self.assertEqual(self.sample4.subject, expected_sample_subject)
        # Attributes that should not be affected
        self.assertEqual(self.subject2.samples, {self.sample3})
        self.assertEqual(self.subject1.experiments, set())
        self.assertEqual(self.subject2.experiments, set())
        self.assertEqual(self.experiment1.subjects, set())
        self.assertEqual(self.experiment1.samples, set())
        self.assertEqual(self.sample1.experiments, set())
        self.assertEqual(self.sample2.experiments, set())
        self.assertEqual(self.sample1.subject, self.subject1)
        self.assertEqual(self.sample3.subject, self.subject2)

    # Where the subject is not linked to any experiment
    def test_add_subject_to_sample(self):
        # Attributes that should change
        expected_sample_subject = self.subject1
        expected_subject_samples = {self.sample1, self.sample2, self.sample4}
        # Call the function (perform assignment)
        self.sample4.subject = self.subject1
        self.assertEqual(self.subject1.samples, expected_subject_samples)
        self.assertEqual(self.sample4.subject, expected_sample_subject)
        # Attributes that should not be affected
        self.assertEqual(self.subject2.samples, {self.sample3})
        self.assertEqual(self.subject1.experiments, set())
        self.assertEqual(self.subject2.experiments, set())
        self.assertEqual(self.experiment1.subjects, set())
        self.assertEqual(self.experiment1.samples, set())
        self.assertEqual(self.sample1.experiments, set())
        self.assertEqual(self.sample2.experiments, set())
        self.assertEqual(self.sample1.subject, self.subject1)
        self.assertEqual(self.sample3.subject, self.subject2)


# TODO: Implement tests to check that errors are raised if you try to add a
# sample to an experiment that is not yet associated with a subject, OR if you
# try to add a subject to an experiment that is not yet associated with any
# samples. This does partly depend on what we want our implementation to do!
# E.g. Should we allow a subject without samples to be associated with an
# experiment? What about orphaned subjects (when we have removed all samples
# of a particular subject from an experiment)?
class ModelRemoveTest(unittest.TestCase):

    def setUp(self):
        self.experiment1 = Experiment(orig_study_id=1)
        self.experiment2 = Experiment(orig_study_id=2)
        self.experiment3 = Experiment(orig_study_id=3)
        self.subject1 = Subject(orig_subject_id=1)
        self.subject2 = Subject(orig_subject_id=2)
        self.sample1 = Sample(orig_sample_id=1)
        self.sample2 = Sample(orig_sample_id=2)
        self.sample3 = Sample(orig_sample_id=3)
        self.sample4 = Sample(orig_sample_id=4)  # TODO: Delete?
        # Set up relationships
        self.subject1._samples = {self.sample1, self.sample2}
        self.subject2._samples = {self.sample3}
        self.sample1._subject = self.subject1
        self.sample2._subject = self.subject1
        self.sample3._subject = self.subject2
        self.experiment1._samples = {self.sample1, self.sample2}
        self.experiment1._subjects = {self.subject1}
        self.experiment2._samples = {self.sample1}
        self.experiment2._subjects = {self.subject1}
        self.experiment3._samples = {self.sample1, self.sample3}
        self.experiment3._subjects = {self.subject1, self.subject2}
        self.subject1._experiments = {self.experiment1, self.experiment2,
                                      self.experiment3}
        self.subject2._experiments = {self.experiment3}
        self.sample1._experiments = {self.experiment1, self.experiment2,
                                     self.experiment3}
        self.sample2._experiments = {self.experiment1}
        self.sample3._experiments = {self.experiment3}

    def tearDown(self):
        del self.experiment1
        del self.experiment2
        del self.experiment3
        del self.subject1
        del self.subject2
        del self.sample1
        del self.sample2
        del self.sample3
        del self.sample4

    def test_remove_subject_from_experiment(self):
        # Attributes that should change
        expected_subject1_experiments = {self.experiment2, self.experiment3}
        expected_experiment1_subjects = set()
        expected_experiment1_samples = set()
        expected_sample1_experiments = {self.experiment2, self.experiment3}
        expected_sample2_experiments = set()
        # Attributes that should not be affected
        expected_subject2_experiments = self.subject2.experiments
        expected_experiment2_subjects = self.experiment2.subjects
        expected_experiment3_subjects = self.experiment3.subjects
        expected_experiment2_samples = self.experiment2.samples
        expected_experiment3_samples = self.experiment3.samples
        expected_sample3_experiments = self.sample3.experiments
        # Call the function
        self.experiment1.remove_subject(self.subject1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject1_experiments)
        self.assertEqual(self.experiment1.subjects, expected_experiment1_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment1_samples)
        self.assertEqual(self.sample1.experiments, expected_sample1_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample2_experiments)
        self.assertEqual(self.subject2.experiments, expected_subject2_experiments)
        self.assertEqual(self.experiment2.subjects, expected_experiment2_subjects)
        self.assertEqual(self.experiment3.subjects, expected_experiment3_subjects)
        self.assertEqual(self.experiment2.samples, expected_experiment2_samples)
        self.assertEqual(self.experiment3.samples, expected_experiment3_samples)
        self.assertEqual(self.sample3.experiments, expected_sample3_experiments)

    def test_remove_experiment_from_subject(self):
        # Attributes that should change
        expected_subject1_experiments = {self.experiment2, self.experiment3}
        expected_experiment1_subjects = set()
        expected_experiment1_samples = set()
        expected_sample1_experiments = {self.experiment2, self.experiment3}
        expected_sample2_experiments = set()
        # Attributes that should not be affected
        expected_subject2_experiments = self.subject2.experiments
        expected_experiment2_subjects = self.experiment2.subjects
        expected_experiment3_subjects = self.experiment3.subjects
        expected_experiment2_samples = self.experiment2.samples
        expected_experiment3_samples = self.experiment3.samples
        expected_sample3_experiments = self.sample3.experiments
        # Call the function
        self.subject1.remove_experiment(self.experiment1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject1_experiments)
        self.assertEqual(self.experiment1.subjects, expected_experiment1_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment1_samples)
        self.assertEqual(self.sample1.experiments, expected_sample1_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample2_experiments)
        self.assertEqual(self.subject2.experiments, expected_subject2_experiments)
        self.assertEqual(self.experiment2.subjects, expected_experiment2_subjects)
        self.assertEqual(self.experiment3.subjects, expected_experiment3_subjects)
        self.assertEqual(self.experiment2.samples, expected_experiment2_samples)
        self.assertEqual(self.experiment3.samples, expected_experiment3_samples)
        self.assertEqual(self.sample3.experiments, expected_sample3_experiments)

    def test_remove_sample_from_experiment(self):
        # Attributes that should change
        expected_experiment1_samples = {self.sample2}
        expected_sample1_experiments = {self.experiment2, self.experiment3}
        # Attributes that should not be affected
        expected_subject1_experiments = {self.experiment1, self.experiment2,
                                         self.experiment3}
        expected_subject2_experiments = self.subject2.experiments
        expected_experiment1_subjects = {self.subject1}
        expected_experiment2_subjects = self.experiment2.subjects
        expected_experiment3_subjects = self.experiment3.subjects
        expected_experiment2_samples = self.experiment2.samples
        expected_experiment3_samples = self.experiment3.samples
        expected_sample2_experiments = {self.experiment1}
        expected_sample3_experiments = self.sample3.experiments
        # Call the function
        self.experiment1.remove_sample(self.sample1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject1_experiments)
        self.assertEqual(self.experiment1.subjects, expected_experiment1_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment1_samples)
        self.assertEqual(self.sample1.experiments, expected_sample1_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample2_experiments)
        self.assertEqual(self.subject2.experiments, expected_subject2_experiments)
        self.assertEqual(self.experiment2.subjects, expected_experiment2_subjects)
        self.assertEqual(self.experiment3.subjects, expected_experiment3_subjects)
        self.assertEqual(self.experiment2.samples, expected_experiment2_samples)
        self.assertEqual(self.experiment3.samples, expected_experiment3_samples)
        self.assertEqual(self.sample3.experiments, expected_sample3_experiments)

    def test_remove_experiment_from_sample(self):
        # Attributes that should change
        expected_experiment1_samples = {self.sample2}
        expected_sample1_experiments = {self.experiment2, self.experiment3}
        # Attributes that should not be affected
        expected_subject1_experiments = {self.experiment1, self.experiment2,
                                         self.experiment3}
        expected_subject2_experiments = self.subject2.experiments
        expected_experiment1_subjects = {self.subject1}
        expected_experiment2_subjects = self.experiment2.subjects
        expected_experiment3_subjects = self.experiment3.subjects
        expected_experiment2_samples = self.experiment2.samples
        expected_experiment3_samples = self.experiment3.samples
        expected_sample2_experiments = {self.experiment1}
        expected_sample3_experiments = self.sample3.experiments
        # Call the function
        self.sample1.remove_experiment(self.experiment1)
        # Check
        self.assertEqual(self.subject1.experiments, expected_subject1_experiments)
        self.assertEqual(self.experiment1.subjects, expected_experiment1_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment1_samples)
        self.assertEqual(self.sample1.experiments, expected_sample1_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample2_experiments)
        self.assertEqual(self.subject2.experiments, expected_subject2_experiments)
        self.assertEqual(self.experiment2.subjects, expected_experiment2_subjects)
        self.assertEqual(self.experiment3.subjects, expected_experiment3_subjects)
        self.assertEqual(self.experiment2.samples, expected_experiment2_samples)
        self.assertEqual(self.experiment3.samples, expected_experiment3_samples)
        self.assertEqual(self.sample3.experiments, expected_sample3_experiments)

    def test_remove_sample_from_subject(self):
        # Attributes that should change
        expected_subject1_samples = {self.sample2}
        expected_experiment1_samples = {self.sample2}
        expected_sample1_subject = None
        expected_sample1_experiments = set()
        expected_experiment3_samples = {self.sample3}
        expected_experiment3_subjects = {self.subject2}
        expected_experiment2_samples = set()
        expected_experiment2_subjects = set()
        # Attributes that should not be affected
        expected_subject1_experiments = self.subject1.experiments
        expected_subject2_experiments = self.subject2.experiments
        expected_experiment1_subjects = self.experiment1.subjects
        expected_sample2_experiments = self.sample2.experiments
        expected_sample3_experiments = self.sample3.experiments
        # Call the function
        self.subject1.remove_sample(self.sample1)
        # Check
        self.assertEqual(self.subject1.samples, expected_subject1_samples)
        self.assertEqual(self.sample1.subject, expected_sample1_subject)
        self.assertEqual(self.subject1.experiments, expected_subject1_experiments)
        self.assertEqual(self.experiment1.subjects, expected_experiment1_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment1_samples)
        self.assertEqual(self.sample1.experiments, expected_sample1_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample2_experiments)
        self.assertEqual(self.subject2.experiments, expected_subject2_experiments)
        self.assertEqual(self.experiment2.subjects, expected_experiment2_subjects)
        self.assertEqual(self.experiment3.subjects, expected_experiment3_subjects)
        self.assertEqual(self.experiment2.samples, expected_experiment2_samples)
        self.assertEqual(self.experiment3.samples, expected_experiment3_samples)
        self.assertEqual(self.sample3.experiments, expected_sample3_experiments)

    def test_remove_subject_from_sample(self):
        # Attributes that should change
        expected_subject1_samples = {self.sample2}
        expected_experiment1_samples = {self.sample2}
        expected_sample1_subject = None
        expected_sample1_experiments = set()
        expected_experiment3_samples = {self.sample3}
        expected_experiment3_subjects = {self.subject2}
        expected_experiment2_samples = set()
        expected_experiment2_subjects = set()
        # Attributes that should not be affected
        expected_subject1_experiments = self.subject1.experiments
        expected_subject2_experiments = self.subject2.experiments
        expected_experiment1_subjects = self.experiment1.subjects
        expected_sample2_experiments = self.sample2.experiments
        expected_sample3_experiments = self.sample3.experiments
        # Call the function
        del self.sample1.subject  # TODO What about self.sample1.subject = None?
        # Check
        self.assertEqual(self.subject1.samples, expected_subject1_samples)
        self.assertEqual(self.sample1.subject, expected_sample1_subject)
        self.assertEqual(self.subject1.experiments, expected_subject1_experiments)
        self.assertEqual(self.experiment1.subjects, expected_experiment1_subjects)
        self.assertEqual(self.experiment1.samples, expected_experiment1_samples)
        self.assertEqual(self.sample1.experiments, expected_sample1_experiments)
        self.assertEqual(self.sample2.experiments, expected_sample2_experiments)
        self.assertEqual(self.subject2.experiments, expected_subject2_experiments)
        self.assertEqual(self.experiment2.subjects, expected_experiment2_subjects)
        self.assertEqual(self.experiment3.subjects, expected_experiment3_subjects)
        self.assertEqual(self.experiment2.samples, expected_experiment2_samples)
        self.assertEqual(self.experiment3.samples, expected_experiment3_samples)
        self.assertEqual(self.sample3.experiments, expected_sample3_experiments)


class SampleParserTest(unittest.TestCase):
    sample_test_file = './data/test_data/samp_metadata/sample1.txt'

    row = OrderedDict([
            ('sample_name', '317.F10'),
            ('age', '22'),
            ('age_unit', 'years'),
            ('altitude', '0'),
            ('anatomical_body_site', 'FMA:Palm'),
            ('anonymized_name', 'F10'),
            ('body_habitat', 'UBERON:skin'),
            ('body_product', 'UBERON:sebum'),
            ('body_site', 'UBERON:zone of skin of hand'),
            ('collection_date', '11/12/2006'),
            ('country', 'GAZ:United States of America'),
            ('depth', '0'),
            ('description', 'human skin metagenome'),
            ('dna_extracted', 'true'),
            ('dominant_hand', ''),
            ('elevation', '1591.99'),
            ('env_biome', 'ENVO:human-associated habitat'),
            ('env_feature', 'ENVO:human-associated habitat'),
            ('host_common_name', 'human'),
            ('host_subject_id', 'F1'),
            ('host_taxid', '9606'),
            ('latitude', '40'),
            ('longitude', '-105'),
            ('palm_size', ''),
            ('physical_specimen_remaining', 'false'),
            ('public', 'true'),
            ('qiita_study_id', '317'),
            ('sample_type', 'XXQIITAXX'),
            ('sex', 'female'),
            ('time_since_last_wash', '0'),
            ('title', 'The influence of sex handedness and washing on the diversity of hand surface bacteriaS1_V160')
    ])
    dayfirst_dict = {'collection_date': False}

    # TODO Update details of source (when necessary)
    source1 = Source(name='qiita', type_='Database (Public)',
                     url='https://qiita.ucsd.edu/study/description/0')
    experiment1 = Experiment(source=source1, orig_study_id='317')
    subject1 = Subject(source=source1, orig_study_id='317', orig_subject_id='F1',
                       sex='female', country='United States of America',
                       race=None, csection=None, disease=None, dob=None,)
    subject2 = Subject(source=source1, orig_study_id='317', orig_subject_id='F2',
                       sex='female', country='United States of America',
                       race=None, csection=None, disease=None, dob=None,)
    sampling_site = SamplingSite(uberon_habitat_term='UBERON:skin',
                         uberon_product_term='UBERON:sebum',
                         uberon_site_term='UBERON:zone of skin of hand',
                         env_biom_term='ENVO:human-associated habitat',
                         env_feature_term='ENVO:human-associated habitat')
    sampling_time = Time(timestamp=datetime.datetime(2006, 11, 12),
                         uncertainty=None, date=datetime.date(2006, 11, 12),
                         time=None, year=2006, month=11, day=12, hour=None,
                         minute=None, second=None, season='autumn')
    sample1 = Sample(source=source1, orig_study_id='317', orig_subject_id='F1',
                     orig_sample_id='317.F10', age_units=ureg.years,
                     age=22.0, latitude=40.0, longitude=-105.0, elevation=1591.99,
                     height_units=ureg.metres, height=None,
                     weight_units=ureg.kilograms, weight=None, bmi=None,
                     sample_date=datetime.date(2006, 11, 12), sample_time=None,
                     sampling_site=sampling_site, sampling_time=sampling_time)
    sample2 = Sample(source=source1, orig_study_id='317', orig_subject_id='F1',
                     orig_sample_id='317.F12', age_units=ureg.years, age=22.0,
                     latitude=40.0, longitude=-105.0, elevation=1591.99,
                     height_units=ureg.metres, height=None,
                     weight_units=ureg.kilograms, weight=None, bmi=None,
                     sample_date=datetime.date(2006, 11, 12), sample_time=None,
                     sampling_site=sampling_site, sampling_time=sampling_time)
    sample3 = Sample(source=source1, orig_study_id='317', orig_subject_id='F2',
                     orig_sample_id='317.F20', age_units=ureg.years,
                     age=None, latitude=40.0, longitude=-105.0, elevation=1591.99,
                     height_units=ureg.metres, height=None,
                     weight_units=ureg.kilograms, weight=None, bmi=None,
                     sample_date=datetime.date(2006, 11, 12), sample_time=None,
                     sampling_site=sampling_site, sampling_time=sampling_time)
    # Not necessary to establish these relationships for purpose of
    # test_parse_objects:
    sample1._subject = subject1
    sample2._subject = subject1
    sample3._subject = subject2
    subject1._samples = {sample1, sample2}
    subject2._samples = {sample3}
    experiment1._subjects = {subject1, subject2}
    experiment1._samples = {sample1, sample2, sample3}

    def test_parse_objects(self):
        experiment_ids = parse_objects(self.sample_test_file)
        self.assertIn('317', experiment_ids)
        experiment = experiment_ids['317']
        self.assertEqual(self.experiment1, experiment)
        self.assertIn(self.subject1, experiment.subjects)
        self.assertIn(self.subject2, experiment.subjects)
        self.assertIn(self.sample1, experiment.samples)
        self.assertIn(self.sample2, experiment.samples)
        self.assertIn(self.sample3, experiment.samples)

    # TODO: We will have to test without the source keyword at some point.
    def test_parse_sample(self):
        self.maxDiff=None
        blacklist_attrs = ['_sa_instance_state', 'source', 'counts',
                           '_experiments', '_subject', '_preparations']
        sample = parse_sample(self.row, self.dayfirst_dict, source=self.source1)
        sample_attrs = set((key, value) for key, value
                           in sample.__dict__.items()
                           if key not in blacklist_attrs)
        expected_attrs = set((key, value) for key, value
                             in self.sample1.__dict__.items()
                             if key not in blacklist_attrs)
        self.assertEqual(sample_attrs, expected_attrs)
        self.assertEqual(sample.source, self.source1)
        self.assertEqual(sample.counts, self.sample1.counts)
        # When sample is parsed, it is not yet associated with subject/experiments
        self.assertEqual(sample._subject, None)
        self.assertEqual(sample._experiments, set())
        self.assertEqual(sample._preparations, set())

    def test_parse_subject(self):
        self.maxDiff=None
        blacklist_attrs = ['_sa_instance_state', 'source', 'counts',
                           'perturbation_facts', '_experiments', '_samples',
                           '_perturbations']
        subject = parse_subject(self.row, source=self.source1)
        subject_attrs = set((key, value) for key, value
                            in subject.__dict__.items()
                            if key not in blacklist_attrs)
        expected_attrs = set((key, value) for key, value
                             in self.subject1.__dict__.items()
                             if key not in blacklist_attrs)
        self.assertEqual(subject_attrs, expected_attrs)
        self.assertEqual(subject.source, self.source1)
        self.assertEqual(subject.counts, self.subject1.counts)
        self.assertEqual(subject.perturbation_facts, self.subject1.perturbation_facts)
        # When subject is parsed, it is not yet associated with samples/experiments
        self.assertEqual(subject._experiments, set())
        self.assertEqual(subject._samples, set())
        self.assertEqual(subject._perturbations, set())

    def test_parse_processing(self):
        self.maxDiff=None
        processing1 = Processing(
            parent=None,
            parameter_values='{}',
            orig_prep_id='577',
            orig_proc_id='2593'
        )
        processing2 = Processing(
            parent=processing1,
            parameter_values='{'
                '"barcode_type":"golay_12",'
                '"command":"Split libraries (QIIMEq2 1.9.1)",'
                '"disable_bc_correction":"False",'
                '"disable_primers":"False",'
                '"generated on":"2016-01-14 17:01",'
                '"input_data":"2593",'
                '"max_ambig":"6",'
                '"max_barcode_errors":"1.5",'
                '"max_homopolymer":"6",'
                '"max_primer_mismatch":"0",'
                '"max_seq_len":"1000",'
                '"min_qual_score":"25",'
                '"min_seq_len":"200",'
                '"qual_score_window":"0",'
                '"reverse_primer_mismatches":"0",'
                '"reverse_primers":"disable",'
                '"trim_seq_length":"False",'
                '"truncate_ambi_bases":"False"'
            '}',
            orig_prep_id='577',
            orig_proc_id='310'
        )
        processing3 = Processing(
            parent=processing2,
            parameter_values='{'
                '"command":"Pick closed-reference OTUs (QIIMEq2 1.9.1)",'
                '"generated on":"2015-06-30 14:06",'
                '"input_data":"310",'
                '"reference-seq":"/databases/gg/13_8/rep_set/97_otus.fasta",'
                '"reference-tax":"/databases/gg/13_8/taxonomy/97_otu_taxonomy.txt",'
                '"similarity":"0.97",'
                '"sortmerna_coverage":"0.97",'
                '"sortmerna_e_value":"1",'
                '"sortmerna_max_pos":"10000",'
                '"threads":"1"'
            '}',
            orig_prep_id='577',
            orig_proc_id='2594'
        )
        expected_processings = {'2593': processing1,
                                '310': processing2,
                                '2594': processing3}
        processings = parse_processings('./data/test_data/proc1.json')
        # TODO: Implement workflows and parents as mocks?
        blacklist_attrs = ['_sa_instance_state', 'workflows', 'parent']
        for proc_id, processing in processings.items():
            self.assertIn(proc_id, expected_processings)
            processing_attrs = set((key, value) for key, value
                                   in processing.__dict__.items()
                                   if key not in blacklist_attrs)
            expected_attrs = set((key, value) for key, value
                                 in expected_processings[proc_id].__dict__.items()
                                 if key not in blacklist_attrs)
            self.assertEqual(processing_attrs, expected_attrs)



if __name__ == '__main__':
    unittest.main(verbosity=2)
