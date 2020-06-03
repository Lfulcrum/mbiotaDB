# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 15:25:22 2019

@author: William
"""

# Standard library imports
from collections import defaultdict, Counter
import csv
import json
import os
import os.path
import re
import xml.etree.ElementTree as ET
from contextlib import contextmanager

# Third-party imports
from sqlalchemy import create_engine, exists, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.engine.url import URL

# Local application imports
import model
from model import Article, Author, CollectiveAuthor
from . import session_scope


# Parse Bibliographic Data
def parse_name(author_name):
    """Parse the given, valid authors name.

    Return: tuple containing first name, a tuple of middle initials and last
    name of the author.
    Raises: ValueError if given an invalid author_name.
    """
    name_re = re.compile(r'(?:\w[-\'\w]* *)+,(?: *\w[-\'\w]*\.?)+')
    if not name_re.match(author_name):
        raise ValueError(f'Invalid author name {author_name!r}.')
    last_name, initials = author_name.split(',')
    initials = [i for i in re.split(r'[\.\s]+', initials) if i != '']
    first_initial, middle_initials = initials[0][0], initials[1:]
    middle_initials = tuple(initial[0] for initial in middle_initials)
    return first_initial, middle_initials, last_name


def is_valid_author_name(author_name):
    """Return True if author_name is valid, False if not.
    """
    name_re = re.compile(r'(?:\w[-\'\w]* *)+,(?: *\w[-\'\w]*\.?)+')
    if name_re.match(author_name):
        return True
    else:
        return False


# TODO What happens in exceptional cases? Redo docstring.
# This function is pretty pointless really. When are you going to want to
# extract all the initials from several people's forenames into a single list?
# It also uses recursion (which is cool), but it is pointless because it will
# only ever need to go to a recursion depth of 2 and can easily be coded without
# it. Doing so will also make the code more understandable!
def extract_initials(*forenames):
    """Return a list of initials for each name in the given forenames.
    """
    initials = []
    # To handle varargs
    for forename in forenames:
        # Allow polymorphism: forenames can be str or sequence of str
        try:
            names = forename.split()
        except TypeError:
            names = [name.strip() for name in forename]
        for name in names:
            name = name.strip('.')
            # If names are separated by dots only (no whitespace)
            if '.' in name:
                new_names = ' '.join(name.split('.'))
                new_initials = extract_initials(new_names)
                initials += new_initials
                continue
            # If names are hyphenated
            if '-' in name:
                subnames = name.split('-')
                initial = '-'.join([name[0].upper() for name in subnames])
            else:
                initial = name[0].upper()
            initials.append(initial)
    return initials


def parse_author(author_xml_element):
    """Parse an author from an author xml element.

    The author xml element has been extracted from an xml file exported from
    Pubmed.

    Parameters
    ----------
    xml.etree.ElementTree.Element corresponding to an author.

    Returns
    -------
    Author object.
    None if no author could be parsed from the xml element.
    """
    last_name = author_xml_element.findtext('LastName')
    fore_name = author_xml_element.findtext('ForeName')
    # Return None is an author element has no last name
    if last_name is None:
        return None
    # Accept authors that have only last name, but no fore names/initials
    if fore_name is None:
        first_name = None
        first_initial = None
        middle_initials = None
    else:
        names = fore_name.split()
        first_name = names[0]
        initials = extract_initials(fore_name)
        first_initial, middle_initials = initials[0], initials[1:]
    # Initialize an Author
    # print(first_initial, first_name, middle_initials, last_name)  # DEBUG
    author = Author(first_initial=first_initial,
                    first_name=first_name,
                    middle_initials=middle_initials,
                    last_name=last_name
                    )
    return author


def parse_collective_author(author_xml_element):
    """Parse a collective author from an author xml element.

    The author xml element has been extracted from an xml file exported from
    Pubmed.

    Parameters
    ----------
    xml.etree.ElementTree.Element corresponding to a collective author.

    Returns
    -------
    CollectiveAuthor object.
    None if no collective author could be parsed from the xml element.
    """
    collective_author = author_xml_element.findtext('CollectiveName')
    # TODO: Do we need this if statement?
    if collective_author:
        # TODO: Should we convert all collective author names to lowercase?
        collective_author = ' '.join(collective_author.strip().lower().split())
        collective_author = CollectiveAuthor(name=collective_author)
        return collective_author
    return None


def parse_title(record):
#    title = record.findtext('.//Article/ArticleTitle')
    title = record.find('.//Article/ArticleTitle')
    title = ''.join(title.itertext())
    if title:
        title = title.strip('.')
        return title
    raise Exception('Article has no title!')


def parse_journal(record):
    journal = record.findtext('.//Journal/Title')
    if journal:
        journal = journal.lower()
        return journal
    raise Exception('Article has no journal!')


# TODO Delete this function if not needed!
def extract_year_from_text(string):
    match = re.search(r'\d{4}', string)
    if match:
        return match.group()
    return None


def parse_pub_year(record):
    # TODO Should we check pub_year or epub_year first?
    pub_year = record.findtext('.//JournalIssue/PubDate/Year')
    if pub_year:
        return int(pub_year)
    epub_year = record.findtext('.//Article/ArticleDate/Year')
    if epub_year:
        return int(epub_year)
    medline_pub_date = record.findtext('.//JournalIssue/PubDate/MedlineDate')
    if medline_pub_date:
        med_pub_year = re.search(r'\d{4}', medline_pub_date)
        if med_pub_year:
            return int(med_pub_year.group())
        return None
    raise Exception('Publication year not found!')


def parse_article(record):
    # For some attributes, directly extract text from XML element
    pmid = record.findtext('.//PMID')
    print(pmid)
    vol = record.findtext('.//JournalIssue/Volume')
    issue = record.findtext('.//JournalIssue/Issue')
    journal_iso = record.findtext('.//Journal/ISOAbbreviation')
    doi = record.findtext('.//ArticleIdList/ArticleId[@IdType="doi"]')
    pages = record.findtext('.//Pagination/MedlinePgn')

    # For other attributes, process XML elements
    title = parse_title(record)
    pub_year = parse_pub_year(record)
    journal = parse_journal(record)
    # Parse authors and collective authors
    authors = record.find('.//AuthorList')
    parsed_authors = []
    collective_authors = []
    # NOTE The current method below uses total programming. Might consider
    # using Defensive programming. At the moment, no distinction is made
    # between an author that was not parsed correctly (for some reason) and
    # an author that is not an Author but rather a CollectiveAuthor.
    for author in authors:
        collective_author = parse_collective_author(author)
        author = parse_author(author)
        # TODO Should we get rid of duplicate authors/collective authors at
        # this stage, or after creating an Author/CollectiveAuthor model
        # object?
        if collective_author and (collective_author not in collective_authors):
            collective_authors.append(collective_author)
        if author:
            parsed_authors.append(author)

    # Initialize an Article
    article = Article(
                title=title,
                pub_year=pub_year,
                journal=journal,
                journal_iso=journal_iso,
                vol=vol,
                issue=issue,
                pages=pages,
                doi=doi,
                pmid=pmid,
                authors=parsed_authors,
                collective_authors=collective_authors
    )
    return article


def generate_records(xml_file):
    """Generate an xml element corresponding to an article.

    Parameters
    ----------
    Path to an xml file exported from PubMed search results.

    Yields
    ------
    xml.etree.ElementTree.Element
    """
    tree = ET.parse(xml_file)
    records = tree.getroot()
    for record in records:
        yield record


# There are 2 alternatives to inserting authors/collective authors:
# 1) Treat each author for each article as a distinct individual (even if they
# happen to be the same person). Rationale: We can never be certain that
# authors with the same first_name, middle_initials and last_name are in fact
# the same person, especially when only a first initial is given (no full
# first name available). Additionally, we can consider each tuple in the table
# to represent an author at a particular point in time. Some of the author's
# attributes might also change with time. If, in the future, we want to extend
# the database to include more information about the author e.g. affiliation
# at the time of publication, then that is more easily done.
# 2) Treat each author merely as a tuple of first_name, middle_initials and
# last_name. Each author in the database can potentially correspond to more
# than one physical author because they share the same name. If we are only
# ever going to be interested in looking up particular author's names
# associated with particular articles, or submitting queries to return all
# articles authored by an author with a particular name, then this will make
# no difference. Advantage: Store fewer author names.
# TODO Update docstring
def update_bib_from_xml(xml_file, session):
    """Insert bibliographic information found in the given xml citation file
    into the database to which the given session is connected to.

    Parameters
    ----------
    xml_file: path to an xml citation file exported from PubMed search results,
    or with the same format.
    """
    for record in generate_records(xml_file):
        article = parse_article(record)
        # TODO Show Karoline the alternative approaches to storing the bibliographic
        # data - discuss which is best.
        # UNCOMMENT code below and indent session.add() to show alternative!
#        authors = article.authors
#        collective_authors = article.collective_authors
#        # Reset the article's authors and collective authors to prevent
#        # undesirable objects being added to a session
#        article.authors = []
#        article.collective_authors = []
#        if not database_contains_article(article, session):
#            for author in authors:
#                existing_author = get_author(author, session)
#                if existing_author and existing_author not in article.authors:
#                    article.authors.append(existing_author)
#                else:
#                    article.authors.append(author)
#            for collective_author in collective_authors:
#                existing_collective_author = get_collective_author(collective_author, session)
#                if (existing_collective_author and
#                    existing_collective_author not in article.collective_authors):
#                    article.collective_authors.append(existing_collective_author)
#                else:
#                    article.collective_authors.append(collective_author)
        session.add(article)


# FUNCTIONS ONLY REQUIRED IF ALTERNATIVE (2) FOR AUTHORS/COLLECTIVE AUTHORS
# IS IMPLEMENTED:
def get_article(article, session):
    """Return an Article from the database whose PMID is the same as the
    given article, and None if no such article can be found.
    """
    try:
        result = session.query(Article).filter(
                Article.pmid == article.pmid
                ).one()
    except NoResultFound:
        return None
    return result

def get_author(author, session):
    """Return an Author from the database whose first_name, middle_initials
    and last_name are the same as the given author, or None if no such author
    can be found.
    """
    try:
        result = session.query(Author).filter(and_(
                Author.first_name == author.first_name,
                Author.middle_initials == author.middle_initials,
                Author.last_name == author.last_name
                )).one()
    except NoResultFound:
        return None
    return result

def get_collective_author(collective_author, session):
    """Return an CollectiveAuthor from the database whose name is the same as
    the given collective_author, or None if no such author can be found.
    """
    try:
        result = session.query(CollectiveAuthor).filter(
                CollectiveAuthor.name == collective_author.name
                ).one()
    except NoResultFound:
        return None
    return result

def database_contains_article(article, session):
    """Return True if an Article exists in the database with the same PMID as
    the given article, or False if no such Article exists.
    """
    result = session.query(exists().where(
                    Article.pmid == article.pmid
                    )).scalar()
    return result


def database_contains_author(author, session):
    """Return True if an Author exists in the database with the same
    first_name, middle_initials and last_name as the given author, or False
    if no such Author exists.
    """
    result = session.query(exists().where(and_(
            Author.first_name == author.first_name,
            Author.middle_initials == author.middle_initials,
            Author.last_name == author.last_name
            ))).scalar()
    return result


def database_contains_collective_author(collective_author, session):
    """Return True if an CollectiveAuthor exists in the database with the same
    name as the given collective_author, or False if no such CollectiveAuthor
    exists.
    """
    result = session.query(exists().where(
                    CollectiveAuthor.name == collective_author.name
                    )).scalar()
    return result

if __name__ == '__main__':
    pass
