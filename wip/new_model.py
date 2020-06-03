# -*- coding: utf-8 -*-
"""
Database model (using SQLAlchemy)

Created on Mon Mar 18 16:28:49 2019
@author: William
"""

# Standard library imports
from datetime import datetime, date

# Third-party imports
from dateutil import parser
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Table, Column, ForeignKey,
						UniqueConstraint, CheckConstraint,
						Integer, SmallInteger, Text, Boolean, Numeric, Enum,
						DateTime, Date, Time, Interval)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


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

# Table to handle m-n relationship between workflows and processings
workflow_processings = \
    Table('workflow_processings',
          Base.metadata,
          Column('workflow_id',
                 ForeignKey('workflows.id'),
                 primary_key=True),
          Column('processing_id',
                 ForeignKey('processings.id'),
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
        return f'<Article(pmid={self.pmid!r})>'


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
        return (f'<Author(last_name={self.last_name!r}, '
                f'first_initial={self.first_initial!r}, '
                f'middle_intials={self.middle_initials!r})>')


class CollectiveAuthor(Base):
    __tablename__ = 'collective_authors'

    id = Column(Integer, primary_key=True)
    name = Column(Text)

    articles = relationship('Article',
                            secondary=article_collective_authors,
                            back_populates='collective_authors')

    def __repr__(self):
        return f'CollectiveAuthor(name={self.name!r})>'


class Source(Base):
    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    url = Column(Text)
    # TODO Automatically generate this timestamp when the object is inserted
    # into the database!
    timestamp = Column(DateTime)

    experiments = relationship('Experiment',
                               back_populates='source')


class Experiment(Base):
    __tablename__ = 'experiments'

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('Source.id'))
    duration = Column(Interval)
    count_subjects = Column(Integer)
    count_samples = Column(Integer)

    articles = relationship('Article',
                            secondary=article_experiments,
                            back_populates='experiments')
    counts = relationship('Count',
                          back_populates='experiment')
    source = relationship('Source',
                          back_populates='experiments')


class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True)
    # Strings used for 'sex' originate from ISO/IEC 5218:2004
    sex = Column(Enum('not known', 'male', 'female', 'not applicable',
                      name='sex'))
    country = Column(Text)
    nationality = Column(Text)
    race = Column(Text)
    dob = Column(Date)
    csection = Column(Boolean)
    # If subject is healthy or diseased throughout the study.
    # They may also be temporarily diseased at some point in the study.
    # The disease details will be provided by a disease Purturbation.
    disease_state = Column(Enum('healthy', 'disease', 'temporary disease',
                           name='disease_state'))

    counts = relationship('Count',
                          back_populates='subject')
    perturbation_facts = relationship('PerturbationFact',
                                      back_populates='subject')


class Sample(Base):
    __tablename__ = 'samples'

    id = Column(Integer, primary_key=True)
    timepoint = Column(Integer)
    age = Column(Numeric)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    elevation = Column(Numeric)
    height = Column(Numeric)
    weight = Column(Numeric)
    bmi = Column(Numeric)

    counts = relationship('Count',
                          back_populates='sample')


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

    seq_instrument = relationship('SeqInstrument',
                                  back_populates='preparations')
    counts = relationship('Count',
                          back_populates='preparation')


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


class Workflow(Base):
    __tablename__ = 'workflows'

    id = Column(Integer, primary_key=True)

    counts = relationship('Count',
                          back_populates='workflow')
    processings = relationship('Processing',
                               secondary=workflow_processings,
                               back_populates='workflows')


class Processing(Base):
    __tablename__ = 'processings'

    id = Column(Integer, primary_key=True)
    parent_proc_id = Column(Integer, ForeignKey('processings.id'))
    parameter_values = Column(JSONB, nullable=False)

    workflows = relationship('Workflow',
                             secondary=workflow_processings,
                             back_populates='processings')
    parent = relationship('Processing', remote_side=[id])


class Perturbation(Base):
    __tablename__ = 'perturbations'

    id = Column(Integer, primary_key=True)
    parameter_values = Column(JSONB, nullable=False)

    perturbation_facts = relationship('PerturbationFact',
                                      back_populates='perturbation')


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


# Run as script to create database tables.
if __name__ == '__main__':
    from creator import engine
    from creator.transact import create_tables
    create_tables(engine)
