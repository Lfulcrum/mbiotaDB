# -*- coding: utf-8 -*-
"""
Database model (using SQLAlchemy)

Created on Mon Mar 18 16:28:49 2019
@author: William
"""

# Standard library imports
from datetime import datetime, date
import enum

# Third-party imports
from dateutil import parser
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Table, Column, ForeignKey,
						UniqueConstraint, CheckConstraint,
						Integer, SmallInteger, Text, Boolean, 
						Numeric, Enum, DateTime, Date, Time,
						Interval)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


# Custom functions
# TODO: Is it better to put setter functionality into a separate
# function like this? I was starting to see a lot of repetition in
# setter code, so it is probably better to keep things DRY. E.g. we could
# define the preparations setter for an Experiment as:
# @preparations.setter
# def preparations(self, preparations):
#     set_setter(self.perturbations, perturbations,
#               self.add_perturbation, self.remove_perturbation)
# TODO: A more elegant solution may be to use a mixin.
def set_setter(old_values, new_values, add_func, remove_func):
    if not new_values:
        del old_values
        return
    try:
        values_to_add = new_values.difference(old_values)
        values_to_remove = old_values.difference(new_values)
    except AttributeError:
        try:
            values_to_add = set(new_values).difference(old_values)
            values_to_remove = old_values.difference(set(new_values))
        except TypeError:
            values_to_add = {new_values}.difference(old_values)
            values_to_remove = old_values.difference({new_values})
    for value in values_to_remove:
        remove_func(value)
    for value in values_to_add:
        add_func(value)


def get_repr(class_name, attr_dict, sep='='):
    internal_strs = [f'{{{i}[0]}}{sep}{{{i}[1]}}' for i
                     in range(len(attr_dict))]
    frmt_str = class_name + '(' + ', '.join(internal_strs) + ')'
    repr_str = frmt_str.format(*attr_dict.items())
    return repr_str


# Create a Base class to use Declarative system
Base = declarative_base()

# Tables to handle article relationships
article_authors = \
    Table('article_authors',
          Base.metadata,
          Column('article_id',
                 ForeignKey('articles.id'),
                 primary_key=True),
          Column('author_id',
                 ForeignKey('authors.id'),
                 primary_key=True)
          )

article_experiments = \
    Table('article_experiments',
          Base.metadata,
          Column('article_id',
                 ForeignKey('articles.id'),
                 primary_key=True),
          Column('experiment_id',
                 ForeignKey('experiments.id'),
                 primary_key=True)
          )

article_collective_authors = \
    Table('article_collective_authors',
          Base.metadata,
          Column('article_id',
                 ForeignKey('articles.id'),
                 primary_key=True),
          Column('collective_author_id',
                 ForeignKey('collective_authors.id'),
                 primary_key=True)
          )


class Article(Base):
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    pub_year = Column(Integer)
    journal = Column(Text)
    journal_iso = Column(Text)
    vol = Column(Text)
    issue = Column(Text)
    pages = Column(Text)
    doi = Column(Text)
    pmid = Column(Text)

    authors = relationship('Author',
                           secondary=article_authors,
                           back_populates='articles')
    experiments = relationship('Experiment',
                               secondary=article_experiments,
                               back_populates='articles')
    collective_authors = relationship('CollectiveAuthor',
                                      secondary=article_collective_authors,
                                      back_populates='articles')

    def __repr__(self):
        return f"""<Article(pmid='{self.pmid}')>"""


class Author(Base):
    __tablename__ = 'authors'
    __tableargs__ = (UniqueConstraint('first_initial', 'first_name',
                                      'middle_initials', 'last_name'),)

    id = Column(Integer, primary_key=True)
    first_initial = Column(Text)
    first_name = Column(Text)
    middle_initials = Column(ARRAY(Text))
    last_name = Column(Text)

    articles = relationship('Article',
                            secondary=article_authors,
                            back_populates='authors')

    def __repr__(self):
        return f"""<Author(last_name='{self.last_name}',
                           first_initial={self.first_initial})>"""


class CollectiveAuthor(Base):
    __tablename__ = 'collective_authors'

    id = Column(Integer, primary_key=True)
    name = Column(Text)

    articles = relationship('Article',
                            secondary=article_collective_authors,
                            back_populates='collective_authors')

    def __repr__(self):
        return f"""<CollectiveAuthor(name='{self.name}')>"""

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.name == other.name)

    # Note: this implies that once created, the CollectiveAuthor self.name
    # should never change! Python docs and other sources describe why it is
    # bad practice to make mutable objects hashable!
    def __hash__(self):
        return hash(self.name)


# Database Part 1 - Data Provenance
# Separate the database into two distinct parts:
# 1) Data Provenance - Keeps track of objects made in Part 2
# 2) Counts and Experimental Context Data
#

class Source(Base):
    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    type_ = Column('type', Text)
    url = Column(Text)

    provenances = relationship('Provenance',
                               back_populates='source')

    @property
    def equality_attrs(self):
        return self.name, self.type_, self.url

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)


class Provenance(Base):
    __tablename__ = 'provenances'

    # TODO: How to ensure object_id is not automatically populated
    object_id = Column(Integer, primary_key=True)
    object_type = Column(Text, primary_key=True)
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)
    insert_timestamp = Column(DateTime, nullable=False)
    update_timestamp = Column(DateTime)  # Last time object was modified
    delete_timestamp = Column(DateTime)
    orig_id = Column(Text)
    orig_timestamp = Column(DateTime)
    # current gives the truth of tuple for current database state
    current = Column(Boolean, nullable=False)

    source = relationship('Source',
                          back_populates='provenances')


# Database Part 2 - Counts and Experimental Context Data

class Experiment(Base):
    __tablename__ = 'experiments'

    id = Column(Integer, primary_key=True)
    duration = Column(Interval)
    count_subjects = Column(Integer)
    count_samples = Column(Integer)

    articles = relationship('Article',
                            secondary=article_experiments,
                            back_populates='experiments')
    counts = relationship('Count',
                          back_populates='experiment')

    @property
    def equality_attrs(self):
        return (self.source, self.orig_study_id)

    def __init__(self, id=None, duration=None, count_subjects=None,
                 count_samples=None, articles=[], counts=[],
                 source=None, orig_study_id=None,
                 subjects=set(), samples=set(), preparations=set(),
                 workflows=set()):
        self.id = id
        self.duration = duration
        self.count_subjects = count_subjects
        self.count_samples = count_samples
        self.articles = articles
        self.counts = counts
        # Non-Column attributes
        self.source = source
        self.orig_study_id = orig_study_id
        self._subjects = set()
        self._samples = set()
        self._preparations = set()
        self._workflows = set()
        self.subjects = subjects
        self.samples = samples
        self.preparations = preparations
        self.workflows = workflows

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)

    @property
    def samples(self):
        return self._samples

    @samples.setter
    def samples(self, samples):
        if not samples:
            del self.subjects
            return
        try:
            new_samples = samples.difference(self.samples)
            old_samples = self.samples.difference(samples)
        except AttributeError:
            try:
                new_samples = set(samples).difference(self.samples)
                old_samples = self.samples.difference(set(samples))
            except TypeError:
                new_samples = {samples}.difference(self.samples)
                old_samples = self.samples.difference({samples})
        for sample in old_samples:
            self.remove_sample(sample)
        for sample in new_samples:
            self.add_sample(sample)

    @samples.deleter
    def samples(self):
        for sample in self.samples.copy():
            self.remove_sample(sample)

    @property
    def subjects(self):
        return self._subjects

    @subjects.setter
    def subjects(self, subjects):
        if not subjects:
            del self.subjects
            return
        try:
            new_subjects = subjects.difference(self.subjects)
            old_subjects = self.subjects.difference(subjects)
        except AttributeError:
            try:
                new_subjects = set(subjects).difference(self.subjects)
                old_subjects = self.subjects.difference(set(subjects))
            except TypeError:
                new_subjects = {subjects}.difference(self.subjects)
                old_subjects = self.subjects.difference({subjects})
        for subject in old_subjects:
            self.remove_subject(subject)
        for subject in new_subjects:
            self.add_subject(subject)

    @subjects.deleter
    def subjects(self):
        for subject in self.subjects.copy():
            self.remove_subject(subject)

    @property
    def preparations(self):
        return self._preparations

    @preparations.setter
    def preparations(self, preparations):
        if not preparations:
            del self.preparations
            return
        try:
            new_preparations = preparations.difference(self.preparations)
            old_preparations = self.preparations.difference(preparations)
        except AttributeError:
            try:
                new_preparations = set(preparations).difference(self.preparations)
                old_preparations = self.preparations.difference(set(preparations))
            except TypeError:
                new_preparations = {preparations}.difference(self.preparations)
                old_preparations = self.preparations.difference({preparations})
        for preparation in old_preparations:
            self.remove_preparation(preparation)
        for preparation in new_preparations:
            self.add_preparation(preparation)

    @preparations.deleter
    def preparations(self):
        for preparation in self.preparations.copy():
            self.remove_preparation(preparation)

    @property
    def workflows(self):
        return self._workflows

    @workflows.setter
    def workflows(self, workflows):
        if not workflows:
            del self.workflows
            return
        try:
            new_workflows = workflows.difference(self.workflows)
            old_workflows = self.workflows.difference(workflows)
        except AttributeError:
            try:
                new_workflows = set(workflows).difference(self.workflows)
                old_workflows = self.workflows.difference(set(workflows))
            except TypeError:
                new_workflows = {workflows}.difference(self.workflows)
                old_workflows = self.workflows.difference({workflows})
        for workflow in old_workflows:
            self.remove_workflow(workflow)
        for workflow in new_workflows:
            self.add_workflow(workflow)

    @workflows.deleter
    def workflows(self):
        for workflow in self.workflows.copy():
            self.remove_workflow(workflow)

    def add_subject(self, subject):
        if isinstance(subject, Subject):
            self.subjects.add(subject)
            subject.experiments.add(self)
            self.samples.update(subject.samples)
            for sample in subject.samples:
                sample.experiments.add(self)
        else:
            raise TypeError(f'Given subject must be of type '
                            '{type(Subject())!r}.')

    def remove_subject(self, subject):
        if isinstance(subject, Subject):
            if subject not in self.subjects:
                raise KeyError(f'Given subject {subject!r} is not '
                               f'associated with this experiment {self!r}.')
            self.subjects.remove(subject)
            subject.experiments.remove(self)
            samples_to_remove = self.samples.intersection(subject.samples)
            for sample in samples_to_remove:
                self.samples.remove(sample)
                sample.experiments.remove(self)
            # Alternative:
#            self.samples.difference_update(subject.samples)
#            for sample in subject.samples:
#                try:
#                    sample.experiments.remove(self)
#                except KeyError:
#                    continue
        else:
            raise TypeError(f'Given subject must be of type '
                            '{type(Subject())!r}.')

    def add_sample(self, sample):
        if isinstance(sample, Sample):
            if not sample.subject:
                raise AttributeError('Cannot add a sample to an experiment if the '
                                     'sample is not yet associated with a subject.')
            self.samples.add(sample)
            sample.experiments.add(self)
            self.subjects.add(sample.subject)
            sample.subject.experiments.add(self)
        else:
            raise TypeError(f'Given sample must be of type '
                            '{type(Sample())!r}.')

    def remove_sample(self, sample):
        if isinstance(sample, Sample):
            if sample not in self.samples:
                raise KeyError(f'Given sample {sample!r} is not '
                               f'associated with this experiment {self!r}.')
            self.samples.remove(sample)
            sample.experiments.remove(self)
            subject = sample.subject
            if not (subject.samples).intersection(self.samples):
                self.subjects.remove(subject)
                subject.experiments.remove(self)
        else:
            raise TypeError(f'Given sample must be of type '
                            '{type(Sample())!r}.')

    def add_preparation(self, preparation):
        if isinstance(preparation, Preparation):
            self.preparations.add(preparation)
            preparation.experiments.add(self)
        else:
            raise TypeError(f'Given preparation must be of type '
                            f'{type(preparation())!r}.')

    def remove_preparation(self, preparation):
        if isinstance(preparation, Preparation):
            if preparation not in self.preparations:
                raise KeyError(f'Given preparation {preparation!r} is not '
                               f'associated with this experiment {self!r}.')
            self.preparations.remove(preparation)
            preparation.experiments.remove(self)
        else:
            raise TypeError(f'Given preparation must be of type '
                            f'{type(preparation())!r}.')

    def add_workflow(self, workflow):
        if isinstance(workflow, Workflow):
            self.workflows.add(workflow)
            workflow.experiments.add(self)
        else:
            raise TypeError(f'Given workflow must be of type '
                            f'{type(Workflow())!r}.')

    def remove_workflow(self, workflow):
        if isinstance(workflow, Workflow):
            if workflow not in self.workflows:
                raise KeyError(f'Given workflow {workflow!r} is not '
                               f'associated with this experiment {self!r}.')
            self.workflows.remove(workflow)
            workflow.experiments.remove(self)
        else:
            raise TypeError(f'Given workflow must be of type '
                            f'{type(Workflow())!r}.')


# Subject and sample relations

class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True)
    sex = Column(Enum('not known', 'male', 'female', 'not applicable',
                      name='sex'))
    country = Column(Text)
    nationality = Column(Text)
    race = Column(Text)
    csection = Column(Boolean)
    # For chronic diseases or to explicitly describe healthy condition
    disease = Column(Text)
    dob = Column(Date)

    counts = relationship('Count',
                          back_populates='subject')
    perturbation_facts = relationship('PerturbationFact',
                                      back_populates='subject')

    @property
    def equality_attrs(self):
        return (self.source, self.orig_study_id,
                self.orig_subject_id)

    def __init__(self, id=None, sex=None, country=None, nationality=None,
                 race=None, csection=None, disease=None, dob=None,
                 counts=[], perturbation_facts=[],
                 source=None, orig_study_id=None, orig_subject_id=None,
                 experiments=set(), samples=set(), perturbations=set()):
        self.id = id
        self.sex = sex
        self.country = country
        self.nationality = nationality
        self.race = race
        self.csection = csection
        self.disease = disease
        self.dob = dob
        self.counts = counts
        self.perturbation_facts = perturbation_facts
        # Non-Column attributes
        self.source = source
        self.orig_study_id = orig_study_id
        self.orig_subject_id = orig_subject_id
        self._experiments = set()
        self._samples = set()
        self._perturbations = set()
        self.experiments = experiments
        self.samples = samples
        self.perturbations = perturbations

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)

    @property
    def experiments(self):
        return self._experiments

    @experiments.setter
    def experiments(self, experiments):
        if not experiments:
            del self.experiments
            return
        try:
            new_experiments = experiments.difference(self.experiments)
            old_experiments = self.experiments.difference(experiments)
        except AttributeError:
            try:
                new_experiments = set(experiments).difference(self.experiments)
                old_experiments = self.experiments.difference(set(experiments))
            except TypeError:
                new_experiments = {experiments}.difference(self.experiments)
                old_experiments = self.experiments.difference({experiments})
        for experiment in old_experiments:
            self.remove_experiment(experiment)
        for experiment in new_experiments:
            self.add_experiment(experiment)

    @experiments.deleter
    def experiments(self):
        for experiment in self.experiments.copy():
            self.remove_experiment(experiment)

    @property
    def samples(self):
        return self._samples

    @samples.setter
    def samples(self, samples):
        if not samples:
            del self.samples
            return
        try:
            new_samples = samples.difference(self.samples)
            old_samples = self.samples.difference(samples)
        except AttributeError:
            try:
                new_samples = set(samples).difference(self.samples)
                old_samples = self.samples.difference(set(samples))
            except TypeError:
                new_samples = {samples}.difference(self.samples)
                old_samples = self.samples.difference({samples})
        for sample in old_samples:
            self.remove_sample(sample)
        for sample in new_samples:
            self.add_sample(sample)

    @samples.deleter
    def samples(self):
        for sample in self.samples.copy():
            self.remove_sample(sample)

    @property
    def perturbations(self):
        return self._perturbations

    @perturbations.setter
    def perturbations(self, perturbations):
        if not perturbations:
            del self.perturbations
            return
        try:
            new_perturbations = perturbations.difference(self.perturbations)
            old_perturbations = self.perturbations.difference(perturbations)
        except AttributeError:
            try:
                new_perturbations = set(perturbations).difference(self.perturbations)
                old_perturbations = self.perturbations.difference(set(perturbations))
            except TypeError:
                new_perturbations = {perturbations}.difference(self.perturbations)
                old_perturbations = self.perturbations.difference({perturbations})
        for perturbation in old_perturbations:
            self.remove_perturbation(perturbation)
        for perturbation in new_perturbations:
            self.add_perturbation(perturbation)

    @perturbations.deleter
    def perturbations(self):
        for perturbation in self.perturbations.copy():
            self.remove_perturbation(perturbation)

    def add_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            self.experiments.add(experiment)
            experiment.subjects.add(self)
            experiment.samples.update(self.samples)
            for sample in self.samples:
                sample.experiments.add(experiment)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')

    def remove_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            if experiment not in self.experiments:
                raise KeyError(f'Given experiment {experiment!r} is not '
                               f'associated with this subject {self!r}.')
            self.experiments.remove(experiment)
            experiment.subjects.remove(self)
            for sample in self.samples:
                experiment.samples.remove(sample)
                sample.experiments.remove(experiment)
            # Alternative:
#            experiment.samples.difference_update(self.samples)
#            for sample in self.samples:
#                sample.experiments.remove(experiment)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(experiment())!r}.')

    def add_sample(self, sample):
        if isinstance(sample, Sample):
            self._samples.add(sample)
            sample._subject = self
            # To update a subject's experiments with new samples:
            # for experiment in self.experiments:
            #     experiment.add_sample(sample)
        else:
            raise TypeError(f'Given sample must be of type '
                            '{type(Sample())!r}.')

    def remove_sample(self, sample):
        if isinstance(sample, Sample):
            if sample not in self.samples:
                raise KeyError(f'Given sample {sample!r} is not '
                               f'associated with this subject {self!r}.')
            for experiment in sample.experiments.copy():
                experiment.samples.remove(sample)
                sample.experiments.remove(experiment)
                subject = sample.subject
                if not (subject.samples).intersection(experiment.samples):
                    experiment.subjects.remove(subject)
                    subject.experiments.remove(experiment)
            self.samples.remove(sample)
            sample._subject = None
        else:
            raise TypeError(f'Given sample must be of type '
                            '{type(Sample())!r}.')

    def add_perturbation(self, perturbation):
        if isinstance(perturbation, Perturbation):
            self.perturbations.add(perturbation)
        else:
            raise TypeError(f'Given perturbation must be of type '
                            f'{type(Perturbation())!r}.')

    def remove_perturbation(self, perturbation):
        if isinstance(perturbation, Perturbation):
            if perturbation not in self.perturbations:
                raise KeyError(f'Given perturbation {perturbation!r} is not '
                               f'associated with this subject {self!r}.')
            self.perturbations.remove(perturbation)
        else:
            raise TypeError(f'Given perturbation must be of type '
                            f'{type(Perturbation())!r}.')


# Encoding ISO/IEC 5218:2004
# Since Enum types always store strings (when using SQLAlchemy at least),
# it may be better to use a TINYINT should you want to encode the sex using
# this ISO. Either that or we ignore the ISO and just store 4 strings in
# English: 'not known', 'male', 'female' and 'not applicable'. I think I
# will do this for the time being.
# Note: using the values_callable parameter of SQLAlchemy's Enum will
# only provide possibility to return string values (NOT int) from Python's
# Enum class below.
class Sex(enum.Enum):
    not_known = 0
    male = 1
    female = 2
    not_applicable = 9


class Sample(Base):
    __tablename__ = 'samples'

    id = Column(Integer, primary_key=True)
    timepoint = Column(Integer)
    age = Column(Numeric)
	# TODO: Should we model latitude and longitude as a Point type in PostgreSQL?
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    elevation = Column(Numeric)
    height = Column(Numeric)
    weight = Column(Numeric)
    bmi = Column(Numeric)
    # TODO acute disease (at time of sampling), but we could put all acute
    # diseases in perturbation_parameters?
    # Also explicitly describe healthy condition if known!
    disease = Column(Text)
    # diet here refers to normal diet (as opposed to diet perturbation)
    diet = Column(Text)
    # TODO These three variables could be parsed into values for diet!
    breastmilk = Column(Boolean)
    cowmilk = Column(Boolean)
    formula = Column(Boolean)

    counts = relationship('Count',
                          back_populates='sample')

    @property
    def equality_attrs(self):
        return (self.source, self.orig_study_id,
                self.orig_subject_id, self.orig_sample_id)

    def __init__(self, id=None, timepoint=None, sample_date=None,
                 sample_time=None, age=None, latitude=None, longitude=None,
                 elevation=None, height=None, weight=None, bmi=None,
                 disease=None, diet_id=None, breastmilk=None, cowmilk=None,
                 formula=None, counts=[],
                 source=None, orig_study_id=None, orig_subject_id=None,
                 orig_sample_id=None, age_units=None, weight_units=None,
                 height_units=None, sampling_site=None, sampling_time=None,
                 subject=None, experiments=set(), preparations=set()):
        self.id = id
        self.timepoint = timepoint
        self.age = age
        self.latitude = latitude
        self.longitude = longitude
        self.elevation = elevation
        self.height = height
        self.weight = weight
        self.bmi = bmi
        self.disease = disease
        self.diet_id = diet_id
        self.breastmilk = breastmilk
        self.cowmilk = cowmilk
        self.formula = formula
        self.counts = counts
        # Non-Column attributes
        self.source = source
        self.orig_study_id = orig_study_id
        self.orig_subject_id = orig_subject_id
        self.orig_sample_id = orig_sample_id
        self.sample_date = sample_date
        self.sample_time = sample_time
        self.age_units = age_units
        self.weight_units = weight_units
        self.height_units = height_units
        self.sampling_site = sampling_site
        self.sampling_time = sampling_time
        self._subject = None
        self._experiments = set()
        self._preparations = set()
        self.subject = subject
        self.experiments = experiments
        self.preparations = preparations

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)

    @property
    def experiments(self):
        return self._experiments

    @experiments.setter
    def experiments(self, experiments):
        if not experiments:
            del self.experiments
            return
        try:
            new_experiments = experiments.difference(self.experiments)
            old_experiments = self.experiments.difference(experiments)
        except AttributeError:
            try:
                new_experiments = set(experiments).difference(self.experiments)
                old_experiments = self.experiments.difference(set(experiments))
            except TypeError:
                new_experiments = {experiments}.difference(self.experiments)
                old_experiments = self.experiments.difference({experiments})
        for experiment in old_experiments:
            self.remove_experiment(experiment)
        for experiment in new_experiments:
            self.add_experiment(experiment)

    @experiments.deleter
    def experiments(self):
        for experiment in self.experiments.copy():
            self.remove_experiment(experiment)

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        if not subject:
            if self.subject:
                del self.subject
            return
        elif isinstance(subject, Subject):
            if self.subject:
                del self.subject
            self._subject = subject
            subject._samples.add(self)
        else:
            raise TypeError(f'Given subject must be of type '
                            '{type(Subject())!r}.')

    @subject.deleter
    def subject(self):
        self.subject.remove_sample(self)

    @property
    def preparations(self):
        return self._preparations

    @preparations.setter
    def preparations(self, preparations):
        if not preparations:
            del self.preparations
            return
        try:
            new_preparations = preparations.difference(self.preparations)
            old_preparations = self.preparations.difference(preparations)
        except AttributeError:
            try:
                new_preparations = set(preparations).difference(self.preparations)
                old_preparations = self.preparations.difference(set(preparations))
            except TypeError:
                new_preparations = {preparations}.difference(self.preparations)
                old_preparations = self.preparations.difference({preparations})
        for preparation in old_preparations:
            self.remove_preparation(preparation)
        for preparation in new_preparations:
            self.add_preparation(preparation)

    @preparations.deleter
    def preparations(self):
        for preparation in self.preparations.copy():
            self.remove_preparation(preparation)

    def add_experiment(self, experiment):
        if not self.subject:
            raise AttributeError('Cannot add a sample to an experiment if the '
                                 'sample is not yet associated with a subject.')
        if isinstance(experiment, Experiment):
            self.experiments.add(experiment)
            experiment.samples.add(self)
            experiment.subjects.add(self.subject)
            self.subject.experiments.add(experiment)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')

    def remove_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            if experiment not in self.experiments:
                raise KeyError(f'Given experiment {experiment!r} is not '
                               f'associated with this sample {self!r}.')
            self.experiments.remove(experiment)
            experiment.samples.remove(self)
            if not (experiment.samples).intersection(self.subject.samples):
                experiment.subjects.remove(self.subject)
                self.subject.experiments.remove(experiment)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')

    def add_preparation(self, preparation):
        if isinstance(preparation, Preparation):
            self.preparations.add(preparation)
            preparation.samples.add(self)
        else:
            raise TypeError(f'Given preparation must be of type '
                            f'{type(Preparation())!r}.')

    def remove_preparation(self, preparation):
        if isinstance(preparation, Preparation):
            if preparation not in self.preparations:
                raise KeyError(f'Given preparation {preparation!r} is not '
                               f'associated with this sample {self!r}.')
            self.preparations.remove(preparation)
            preparation.samples.remove(self)
        else:
            raise TypeError(f'Given preparation must be of type '
                            f'{type(Preparation())!r}.')


# This relation assumes we will not store all Uberon terms in our database,
# but rather parse them and take for granted from studies.
class SamplingSite(Base):
    __tablename__ = 'sampling_sites'

    id = Column(Integer, primary_key=True)
    uberon_habitat_term = Column(Text)
    uberon_site_term = Column(Text)
    uberon_product_term = Column(Text)
    env_biom_term = Column(Text)
    env_feature_term = Column(Text)

    counts = relationship('Count',
                          back_populates='sample_site')

    @property
    def equality_attrs(self):
        return (self.uberon_habitat_term, self.uberon_product_term,
                self.uberon_site_term, self.env_biom_term,
                self.env_feature_term)

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)


# Preparation

class Preparation(Base):
    __tablename__ = 'preparations'

    id = Column(Integer, primary_key=True)
    seq_center = Column(Text)
    seq_run_name = Column(Text)
    seq_date = Column(Date)
    seq_instrument_id = Column(Integer, ForeignKey('seq_instruments.id'))
    fwd_pcr_primer = Column(Text)
    rev_pcr_primer = Column(Text)
    target_gene = Column(Text)
    target_subfragment = Column(Text)
    # Other attributes that might be nice:
#    dna_extraction_kit = Column(Text)
#    dna_extraction_date = Column(Date)
#    seq_count = Column(Integer)  # After QC/processing. Compute automatically.

    seq_instrument = relationship('SeqInstrument',
                                  back_populates='preparations')
    counts = relationship('Count',
                          back_populates='preparation')

    def __init__(self, id=None, seq_center=None, seq_run_name=None,
                 seq_date=None, seq_instrument_id=None, fwd_pcr_primer=None,
                 rev_pcr_primer=None, target_gene=None, target_subfragment=None,
                 counts=[], seq_instrument=None, samples=set(), experiments=set(),
                 workflows=set()):
        self.id = id
        self.seq_center = seq_center
        self.seq_run_name = seq_run_name
        self.seq_date = seq_date
        self.seq_instrument_id = seq_instrument_id
        self.fwd_pcr_primer = fwd_pcr_primer
        self.rev_pcr_primer = rev_pcr_primer
        self.target_gene = target_gene
        self.target_subfragment = target_subfragment
        self.counts = counts
        self.seq_instrument = seq_instrument
        # Non-Column attributes
        self._samples = set()
        self.samples = samples
        self._experiments = set()
        self.experiments = experiments
        self._workflows = set()
        self.workflows = workflows

    @property
    def samples(self):
        return self._samples

    @samples.setter
    def samples(self, samples):
        if not samples:
            del self.samples
            return
        try:
            new_samples = samples.difference(self.samples)
            old_samples = self.samples.difference(samples)
        except AttributeError:
            try:
                new_samples = set(samples).difference(self.samples)
                old_samples = self.samples.difference(set(samples))
            except TypeError:
                new_samples = {samples}.difference(self.samples)
                old_samples = self.samples.difference({samples})
        for sample in old_samples:
            self.remove_sample(sample)
        for sample in new_samples:
            self.add_sample(sample)

    @samples.deleter
    def samples(self):
        for sample in self.samples.copy():
            self.remove_sample(sample)

    @property
    def experiments(self):
        return self._experiments

    @experiments.setter
    def experiments(self, experiments):
        if not experiments:
            del self.experiments
            return
        try:
            new_experiments = experiments.difference(self.experiments)
            old_experiments = self.experiments.difference(experiments)
        except AttributeError:
            try:
                new_experiments = set(experiments).difference(self.experiments)
                old_experiments = self.experiments.difference(set(experiments))
            except TypeError:
                new_experiments = {experiments}.difference(self.experiments)
                old_experiments = self.experiments.difference({experiments})
        for experiment in old_experiments:
            self.remove_experiment(experiment)
        for experiment in new_experiments:
            self.add_experiment(experiment)

    @experiments.deleter
    def experiments(self):
        for experiment in self.experiments.copy():
            self.remove_experiment(experiment)

    @property
    def workflows(self):
        return self._workflows

    @workflows.setter
    def workflows(self, workflows):
        if not workflows:
            del self.workflows
            return
        try:
            new_workflows = workflows.difference(self.workflows)
            old_workflows = self.workflows.difference(workflows)
        except AttributeError:
            try:
                new_workflows = set(workflows).difference(self.workflows)
                old_workflows = self.workflows.difference(set(workflows))
            except TypeError:
                new_workflows = {workflows}.difference(self.workflows)
                old_workflows = self.workflows.difference({workflows})
        for workflow in old_workflows:
            self.remove_workflow(workflow)
        for workflow in new_workflows:
            self.add_workflow(workflow)

    @workflows.deleter
    def workflows(self):
        for workflow in self.workflows.copy():
            self.remove_workflow(workflow)

    def add_sample(self, sample):
        if isinstance(sample, Sample):
            self.samples.add(sample)
            sample.preparations.add(self)
        else:
            raise TypeError(f'Given sample must be of type '
                            f'{type(Sample())!r}.')

    def remove_sample(self, sample):
        if isinstance(sample, Sample):
            if sample not in self.samples:
                raise KeyError(f'Given sample {sample!r} is not '
                               f'associated with this preparation {self!r}.')
            self.samples.remove(sample)
            sample.preparations.remove(self)
        else:
            raise TypeError(f'Given sample must be of type '
                            f'{type(Sample())!r}.')

    def add_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            self.experiments.add(experiment)
            experiment.preparations.add(self)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')

    def remove_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            if experiment not in self.experiments:
                raise KeyError(f'Given experiment {experiment!r} is not '
                               f'associated with this preparation {self!r}.')
            self.experiments.remove(experiment)
            experiment.preparations.remove(self)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')

    def add_workflow(self, workflow):
        if isinstance(workflow, Workflow):
            self.workflows.add(workflow)
            workflow.preparations.add(self)
        else:
            raise TypeError(f'Given workflow must be of type '
                            f'{type(Workflow())!r}.')

    def remove_workflow(self, workflow):
        if isinstance(workflow, Workflow):
            if workflow not in self.workflows:
                raise KeyError(f'Given workflow {workflow!r} is not '
                               f'associated with this preparation {self!r}.')
            self.workflows.remove(workflow)
            workflow.preparations.remove(self)
        else:
            raise TypeError(f'Given workflow must be of type '
                            f'{type(Workflow())!r}.')


class SeqInstrument(Base):
    __tablename__ = 'seq_instruments'

    id = Column(Integer, primary_key=True)
    platform = Column(Text)
    model = Column(Text)
    name = Column(Text)

    preparations = relationship('Preparation',
                                back_populates='seq_instrument')


class SequencingVariant(Base):
    __tablename__ = 'sequencing_variants'

    id = Column(Integer, primary_key=True)
    sequencing_variant = Column(Text)

    counts = relationship('Count',
                          back_populates='seq_variant')


class Lineage(Base):
    __tablename__ = 'lineages'

    id = Column(Integer, primary_key=True)
    kingdom_ = Column('kingdom', Text)
    phylum_ = Column('phylum', Text)
    class_ = Column('class', Text)
    order_ = Column('order', Text)
    family_ = Column('family', Text)
    genus_ = Column('genus', Text)
    species_ = Column('species', Text)

    counts = relationship('Count',
                          back_populates='lineage')


class Time(Base):
    __tablename__ = 'times'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    uncertainty = Column(Integer)
    date = Column(Date)
    time = Column(Time)
    year = Column(SmallInteger, CheckConstraint('1980 < year AND year < 3000'))
    month = Column(SmallInteger, CheckConstraint('1 <= month AND month <= 12'))
    day = Column(SmallInteger, CheckConstraint('1 <= day AND day <= 31'))
    hour = Column(SmallInteger, CheckConstraint('0 <= hour AND hour <= 23'))
    minute = Column(SmallInteger, CheckConstraint('0 <= minute AND minute <= 59'))
    second = Column(Numeric, CheckConstraint('0 <= second AND second < 60'))
    season = Column(Enum('winter', 'spring', 'summer', 'autumn',
                         name='season'))

    counts = relationship('Count',
                          back_populates='sample_time')

    # TODO: Maybe replace with an OrderedDict if we are bothered about the
    # order in which the attributes are listed in the repr string.
    @property
    def _equality_dict(self):
        return {'timestamp': self.timestamp, 'date': self.date,
                'time': self.time, 'uncertainty': self.uncertainty,
                'year': self.year, 'month': self.month, 'day': self.day,
                'hour': self.hour, 'minute': self.minute,
                'second': self.second, 'season': self.season}

    @property
    def equality_attrs(self):
        return tuple(self._equality_dict.values())

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)

    def __repr__(self):
        return get_repr(self.__class__.__name__, self._equality_dict)

    @classmethod
    def from_datetime(cls, timestamp):
        """Parse a datetime.Datetime object into a Time object.


        Parameters
        ----------
        timestamp : datetime.Datetime, required
            If the given timestamp has a year of 1, then all date components
            (date, year, month, day, season) of the Time object are initialized
            to None. If the given timestamp has a time 00:00:00, then all time
            components (time, hour, minute, second) of the Time object are
            initialized to None.

        Raises
        ------
        AttributeError
            If the given timestamp cannot be parsed into a Time object.
        """
        time = cls()
        time.timestamp = timestamp
        try:
            time.date = timestamp.date()
            time.time = timestamp.time()
            if timestamp.year == 1:
                time.date = None
            else:
                time.year = timestamp.year
                time.month = timestamp.month
                time.day = timestamp.day
                time.season = Time.get_season(time.date)
            if (timestamp.hour, timestamp.minute, timestamp.second == 0, 0, 0):
                time.time = None
            else:
                time.hour = timestamp.hour
                time.minute = timestamp.minute
                time.second = timestamp.second
        except AttributeError:
            raise AttributeError(f'The given timestamp {timestamp!r} does not '
                                 'have attributes required for parsing into '
                                 f'{type(time)!r} object.')
        return time

    # Adapted (minimally from https://stackoverflow.com/a/28688724)
    @staticmethod
    def get_season(timestamp, default_datetime=datetime(2000, 1, 1, 0, 0, 0)):
        """Parse a timestamp into a season.

        Parameters
        ----------
        timestamp : object
            An object that has a string representation (str) that can be parsed
            by dateutil.parser.parse into a datetime.datetime object.
        default_datetime : datetime.datetime
            A datetime object used to fill in missing date/time elements during
            parsing. E.g. if '22/11' is provided as a timestamp and the
            default_datetime is given as datetime.datetime(2000,1,1,0,0,0),
            then the missing date/time elements will be filled to produce
            datetime.datetime(2000,11,22,0,0,0).
        """
        try:
            timestamp = parser.parse(str(timestamp), default=default_datetime)
        except TypeError:
            raise TypeError(f'The given timestamp {timestamp!r} could not be '
                            f'converted to {str!r} object.')
        except ValueError:
            raise ValueError('String representation of the given timestamp, '
                             f'"{timestamp!s}", could not be parsed into a '
                             f'{datetime!r} object.')
        Y = 2000  # dummy leap year to allow input X-02-29 (leap day)
        seasons = [('winter', date(Y, 1, 1), date(Y, 3, 20)),
                   ('spring', date(Y, 3, 21), date(Y, 6, 20)),
                   ('summer', date(Y, 6, 21), date(Y, 9, 22)),
                   ('autumn', date(Y, 9, 23), date(Y, 12, 20)),
                   ('winter', date(Y, 12, 21), date(Y, 12, 31))]
        timestamp_date = timestamp.date().replace(year=Y)
        return next(season for (season, start, end) in seasons
                    if start <= timestamp_date <= end)


# Processing relations

workflow_processings = Table('workflow_processings',
                             Base.metadata,
                             Column('workflow_id',
                                    ForeignKey('workflows.id'),
                                    primary_key=True),
                             Column('processing_id',
                                    ForeignKey('processings.id'),
                                    primary_key=True)
                             )


class Workflow(Base):
    __tablename__ = 'workflows'

    id = Column(Integer, primary_key=True)

    counts = relationship('Count',
                          back_populates='workflow')
    processings = relationship('Processing',
                               secondary=workflow_processings,
                               back_populates='workflows')

    def __init__(self, id=None, counts=[], processings=[], preparations=set(),
                 experiments=set()):
        self.id = id
        self.counts = counts
        self.processings = processings
        # Non-Column attributes
        self._preparations = set()
        self.preparations = preparations
        self._experiments = set()
        self.experiments = experiments

    @property
    def preparations(self):
        return self._preparations

    @preparations.setter
    def preparations(self, preparations):
        if not preparations:
            del self.preparations
            return
        try:
            new_preparations = preparations.difference(self.preparations)
            old_preparations = self.preparations.difference(preparations)
        except AttributeError:
            try:
                new_preparations = set(preparations).difference(self.preparations)
                old_preparations = self.preparations.difference(set(preparations))
            except TypeError:
                new_preparations = {preparations}.difference(self.preparations)
                old_preparations = self.preparations.difference({preparations})
        for preparation in old_preparations:
            self.remove_preparation(preparation)
        for preparation in new_preparations:
            self.add_preparation(preparation)

    @preparations.deleter
    def preparations(self):
        for preparation in self.preparations.copy():
            self.remove_preparation(preparation)

    @property
    def experiments(self):
        return self._experiments

    @experiments.setter
    def experiments(self, experiments):
        if not experiments:
            del self.experiments
            return
        try:
            new_experiments = experiments.difference(self.experiments)
            old_experiments = self.experiments.difference(experiments)
        except AttributeError:
            try:
                new_experiments = set(experiments).difference(self.experiments)
                old_experiments = self.experiments.difference(set(experiments))
            except TypeError:
                new_experiments = {experiments}.difference(self.experiments)
                old_experiments = self.experiments.difference({experiments})
        for experiment in old_experiments:
            self.remove_experiment(experiment)
        for experiment in new_experiments:
            self.add_experiment(experiment)

    @experiments.deleter
    def experiments(self):
        for experiment in self.experiments.copy():
            self.remove_experiment(experiment)

    def add_preparation(self, preparation):
        if isinstance(preparation, Preparation):
            self.preparations.add(preparation)
            preparation.workflows.add(self)
        else:
            raise TypeError(f'Given preparation must be of type '
                            f'{type(Preparation())!r}.')

    def remove_preparation(self, preparation):
        if isinstance(preparation, Preparation):
            if preparation not in self.preparations:
                raise KeyError(f'Given preparation {preparation!r} is not '
                               f'associated with this preparation {self!r}.')
            self.preparations.remove(preparation)
            preparation.workflows.remove(self)
        else:
            raise TypeError(f'Given preparation must be of type '
                            f'{type(Preparation())!r}.')

    def add_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            self.experiments.add(experiment)
            experiment.workflows.add(self)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')

    def remove_experiment(self, experiment):
        if isinstance(experiment, Experiment):
            if experiment not in self.experiments:
                raise KeyError(f'Given experiment {experiment!r} is not '
                               f'associated with this workflow {self!r}.')
            self.experiments.remove(experiment)
            experiment.workflows.remove(self)
        else:
            raise TypeError(f'Given experiment must be of type '
                            f'{type(Experiment())!r}.')


class Processing(Base):
    __tablename__ = 'processings'

    id = Column(Integer, primary_key=True)
    parent_proc_id = Column(Integer, ForeignKey('processings.id'))
    parameter_values = Column(JSONB, nullable=False)

    workflows = relationship('Workflow',
                             secondary=workflow_processings,
                             back_populates='processings')
    parent = relationship('Processing', remote_side=[id])

    @property
    def equality_attrs(self):
        return (self.orig_study_id, self.orig_prep_id,
                self.orig_prep_id, self.parent)

    def __init__(self, id=None, parent_proc_id=None, parameter_values=None,
                 workflows=[], parent=None,
                 orig_study_id=None, orig_prep_id=None, orig_proc_id=None,
                 **kwds):
        self.id = id
        self.parent_proc_id = parent_proc_id
        self.parameter_values = parameter_values
        self.workflows = workflows
        self.parent = parent
        # Non-Column attributes
        self.orig_study_id = orig_study_id
        self.orig_prep_id = orig_prep_id
        self.orig_proc_id = orig_proc_id

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.equality_attrs == other.equality_attrs)

    def __hash__(self):
        return hash(self.equality_attrs)


# Perturbation relations

class Perturbation(Base):
    __tablename__ = 'perturbations'

    id = Column(Integer, primary_key=True)
    parameter_values = Column(JSONB, nullable=False)

    perturbation_facts = relationship('PerturbationFact',
                                      back_populates='perturbation')


# Fact tables

class Count(Base):
    __tablename__ = 'count_facts'

    experiment_id = Column(Integer, ForeignKey('experiments.id'), primary_key=True)
    subject_id = Column(Integer, ForeignKey('subjects.id'), primary_key=True)
    sample_id = Column(Integer, ForeignKey('samples.id'), primary_key=True)
    sample_time_id = Column(Integer, ForeignKey('times.id'), primary_key=True)
    sample_site_id = Column(Integer, ForeignKey('sampling_sites.id'), primary_key=True)
    preperation_id = Column(Integer, ForeignKey('preparations.id'), primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'), primary_key=True)
    lineage_id = Column(Integer, ForeignKey('lineages.id'), primary_key=True)
    seq_var_id = Column(Integer, ForeignKey('sequencing_variants.id'))
    # Only taxa with non-zero counts stored!
    count = Column(Integer, nullable=False)

    experiment = relationship('Experiment',
                              back_populates='counts')
    subject = relationship('Subject',
                           back_populates='counts')
    sample = relationship('Sample',
                          back_populates='counts')
    sample_time = relationship('Time',
                               back_populates='counts')
    sample_site = relationship('SamplingSite',
                               back_populates='counts')
    preparation = relationship('Preparation',
                               back_populates='counts')
    workflow = relationship('Workflow',
                            back_populates='counts')
    lineage = relationship('Lineage',
                           back_populates='counts')
    seq_variant = relationship('SequencingVariant',
                               back_populates='counts')


class PerturbationFact(Base):
    __tablename__ = 'perturbation_facts'

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey('experiments.id'), nullable=False)
    subject_id = Column(Integer, ForeignKey('subjects.id'), nullable=False)
    perturbation_id = Column(Integer, ForeignKey('perturbations.id'), nullable=False)
    start_time_id = Column(Integer, ForeignKey('times.id'))
    end_time_id = Column(Integer, ForeignKey('times.id'))

    subject = relationship('Subject',
                           back_populates='perturbation_facts')
    perturbation = relationship('Perturbation',
                                back_populates='perturbation_facts')
    # For time relationship, there is no perturbation_facts back_ref i.e. we
    # will have to add start and end times to perturbation facts rather than
    # adding perturbation facts to times.
    start_time = relationship('Time',
                              foreign_keys=[start_time_id])
    end_time = relationship('Time',
                            foreign_keys=[end_time_id])


## Uncomment the following if you want to use as script to create database tables.
# if __name__ == '__main__':
#     from creator import engine
#     from creator.transact import create_tables
#     create_tables(engine)
