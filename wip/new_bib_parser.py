# -*- coding: utf-8 -*-
"""
Created on Fri May  8 15:50:03 2020

@author: William
"""

# Standard library imports
import datetime
import re
import uuid
import xml.etree.ElementTree as ET


class Article:
    """Class representing an article."""

    def __init__(self, pmid=None, vol=None,
                 issue=None, journal=None, journal_iso=None, doi=None,
                 pages=None, title=None, pub_year=None, authors=[],
                 collective_authors=[]):
        self.article_id = uuid.uuid4()
        self.pmid = pmid
        self.vol = vol
        self.issue = issue
        self.journal = journal
        self.journal_iso = journal_iso
        self.doi = doi
        self.pages = pages
        self.title = title
        self.pub_year = pub_year
        self.authors = []
        for author in authors:
            self.add_author(author)
        self.collective_authors = []
        for collective_author in collective_authors:
            self.add_collective_author(collective_author)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return all((self.pmid == other.pmid,
                    self.vol == other.vol,
                    self.issue == other.issue,
                    self.journal == other.journal,
                    self.journal_iso == other.journal_iso,
                    self.doi == other.doi,
                    self.pages == other.pages,
                    self.title == other.title,
                    self.pub_year == other.pub_year,
                    self.authors == other.authors,
                    self.collective_authors == other.collective_authors))

    def __repr__(self):
        return (f'Article('
                f'title={self.title!r}, '
                f'pub_year={self.pub_year!r}, '
                f'journal={self.journal!r}, '
                f'journal_iso={self.journal_iso!r}, '
                f'vol={self.vol!r}, '
                f'issue={self.issue!r}, '
                f'pages={self.pages!r}, '
                f'journal={self.journal!r}, '
                f'pmid={self.pmid!r}, '
                f'doi={self.doi!r})')

    def add_author(self, author):
        if author in self.authors:
            return
        self.authors.append(author)
        author._add_article(self)

    def add_collective_author(self, author):
        if author in self.collective_authors:
            return
        self.collective_authors.append(author)
        author._add_article(self)


class Author:
    """Class representing an author."""

    def __init__(self, first_name=None, first_initial=None,
                 middle_initials=[], last_name=None, articles=[]):
        self.author_id = uuid.uuid4()
        self.first_name = first_name
        self.first_initial = first_initial
        self.middle_initials = middle_initials
        self.last_name = last_name
        self.articles = []
        for article in articles:
            article.add_author(self)

    def __repr__(self):
        return (f'Author(first_name={self.first_name!r}, '
                f'first_initial={self.first_initial!r}, '
                f'middle_initials={self.middle_initials!r}, '
                f'last_name={self.last_name!r})')

    def _add_article(self, article):
        self.articles.append(article)


class CollectiveAuthor:
    """Class representing a collective author."""

    def __init__(self, name=None, articles=[]):
        self.author_id = uuid.uuid4()
        self.name = name
        self.articles = []
        for article in articles:
            article.add_collective_author(self)

    def __repr__(self):
        return (f'CollectiveAuthor(name={self.name!r})')

    def _add_article(self, article):
        self.articles.append(article)


class ArticleCollection:
    """Class representing a collection of articles."""
    def __init__(self, articles=[]):
        self.articles = []
        for article in articles:
            self.add_article(article)

    def get_article(self, attribute, value):
        matches = []
        for article in self.articles:
            try:
                if getattr(article, attribute) == value:
                    matches.append(article)
            except AttributeError:
                raise ValueError('An article does not have the given '
                                 f'attribute {attribute!r}')
        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception('More than one article in this collection '
                            f'with value {value!r} for attribute '
                            f'{attribute!r}.')
        else:
            return matches[0]

    def add_article(self, article):
        self.articles.append(article)

    def remove_article(self, article):
        self.articles.remove(article)


def parse_articles(file):
    """Parse a collection of articles from an XML file."""
    records = generate_children(file)
    articles = []
    for record in records:
        article = parse_article(record)
        for author in parse_authors(record):
            article.add_author(author)
        for author in parse_collective_authors(record):
            article.add_collective_author(author)
        articles.append(article)
    return articles


def parse_article(record):
    attributes = {
        # For some attributes, get XML element text directly
        'pmid': record.findtext('.//PMID'),
        'vol': record.findtext('.//JournalIssue/Volume'),
        'issue': record.findtext('.//JournalIssue/Issue'),
        'journal_iso': record.findtext('.//Journal/ISOAbbreviation'),
        'doi': record.findtext('.//ArticleIdList/ArticleId[@IdType="doi"]'),
        'pages': record.findtext('.//Pagination/MedlinePgn'),
        # For other elements requiring special processing
        'title': parse_title(record),
        'journal': parse_journal(record),
        'pub_year': parse_pub_year(record),
    }
    return Article(**attributes)


def parse_title(record):
    pmid = record.findtext('.//PMID')
    title = record.find('.//Article/ArticleTitle')
    try:
        # To join text of any nested elements (e.g. italics and superscripts)
        title = ''.join(title.itertext())
        title = title.strip('.')
        return title
    except AttributeError:
        raise ValueError(f'Title not found for article with PMID {pmid}.')


def parse_journal(record):
    pmid = record.findtext('.//PMID')
    journal = record.findtext('.//Journal/Title')
    if journal:
        journal = journal.lower()
        return journal
    raise ValueError(f'Journal not found for article with PMID {pmid}.')


def parse_pub_year(record):
    # TODO Should we check pub_year or epub_year first?
    pmid = record.findtext('.//PMID')
    pub_year = record.findtext('.//JournalIssue/PubDate/Year')
    epub_year = record.findtext('.//Article/ArticleDate/Year')
    medline_pub_date = record.findtext('.//JournalIssue/PubDate/MedlineDate')
    # Define the order for year checking
    possible_years = (pub_year, epub_year, medline_pub_date)
    for year in possible_years:
        try:
            return validate_year(year)
        except ValueError:
            pass
    raise ValueError(f'Publication year not found for article with PMID {pmid}.')


def validate_year(datestr):
    try:
        datestr = datestr.strip()
        year = re.search(r'\d{4}', datestr).group()
        year = int(year)
    except (ValueError, TypeError, AttributeError):
        raise ValueError('Invalid year.')
    if year < 1900 or year > datetime.date.today().year:
        raise ValueError('Invalid year.')
    else:
        return year


def get_authors_from_record(record):
    return record.findall('.//AuthorList/Author')


def parse_authors(record):
    parsed_authors = []
    authors = get_authors_from_record(record)
    for author in authors:
        author = parse_author(author)
        if author:
            parsed_authors.append(author)
    return parsed_authors


def parse_collective_authors(record):
    parsed_authors = []
    authors = get_authors_from_record(record)
    for author in authors:
        author = parse_collective_author(author)
        if author:
            parsed_authors.append(author)
    return parsed_authors


def parse_collective_author(author_elem):
    """Parse a collective author from an author XML element.

    Parameters
    ----------
    author_elem : xml.etree.ElementTree.Element
        XML element corresponding to an author.

    Returns
    -------
    CollectiveAuthor or None
        CollectiveAuthor if `author_elem` is an XML element that has a child
        element with tag name 'CollectiveName', whose text content is not empty
        (empty string or whitespace).
        None otherwise.
    """
    try:
        author_name = author_elem.findtext('CollectiveName')
    except AttributeError:
        raise ValueError('Invalid `author_elem`. Please supply an '
                         'xml.etree.ElementTree.Element object.')
    if not author_name or author_name.strip() == '':
        return None
    author_name = ' '.join(author_name.split())
    attributes = {
        'name': author_name
    }
    return CollectiveAuthor(**attributes)


def parse_author(author_elem):
    """Parse an author from an author XML element.

    Parameters
    ----------
    author_elem : xml.etree.ElementTree.Element
        XML element corresponding to an author.

    Returns
    -------
    Author or None
        Author if `author_elem` is an XML element that has a child element with
        tag name 'LastName', whose text content is not empty (empty string or
        whitespace).
        None otherwise.
    """
    try:
        lastname = author_elem.findtext('LastName')
        forenames = author_elem.findtext('ForeName')
    except AttributeError:
        raise ValueError('Invalid `author_elem`. Please supply an '
                         'xml.etree.ElementTree.Element object.')
    # Return None if an author element has no last name
    if not lastname or lastname.strip() == '':
        return None
    # Accept authors that have only last name, but no fore names/initials
    # TODO Should we ignore lone forenames? A forename is still information.
    # Would reduce logic/complexity if we don't worry about it.
    if not forenames or forenames.strip() == '':
        first_name = None
        first_initial = None
        middle_initials = None
    else:
        names = forenames.split()
        first_name = names[0]
        initials = extract_initials(forenames)
        first_initial, middle_initials = initials[0], initials[1:]
    # print(first_initial, first_name, middle_initials, lastname)  # DEBUG
    return Author(first_initial=first_initial,
                  first_name=first_name,
                  middle_initials=middle_initials,
                  last_name=lastname)


def extract_initials(names):
    """Extract a list of initials from names.

    Parameters
    ----------
    names : str or list of str
        Names (in one long string, or divided into elements of a list) from
        which to extract initials. Each name in names can be a full name or
        initial. In each string, the names can be separated by whitespace or
        artibrary numbers of dots and commas (possibly surrounded by
        whitespace). Hyphenated names are also valid (hyphens may not be
        surrounded by whitespace). If a hyphenated name is used, the hypen is
        retained e.g. 'Smith-Jones' becomes 'S-J'.

    Returns
    -------
    initials : list
        List of initials of names in the given `names`.
    """
    initials = []
    try:
        names = names.split()
    except AttributeError:
        try:
            names = ' '.join(names).split()
        except AttributeError:
            raise ValueError('Invalid `names`. Please supply either a str or '
                             'list of str.')
    for name in names:
        name = re.sub(r'^[.,]+|[.,]+$', '', name)
        if name == '':
            # name contained only dots or commas
            continue
        # If names are separated by dots only (no whitespace)
        if '.' in name:
            name = ' '.join(name.split('.'))
            initials += extract_initials(name)
        elif ',' in name:
            name = ' '.join(name.split(','))
            initials += extract_initials(name)
        # If names are hyphenated
        elif '-' in name:
            subnames = name.split('-')
            try:
                initial = '-'.join([name[0].upper() for name in subnames])
            except IndexError:
                raise ValueError('Invalid hypenation in `names`.')
            initials.append(initial)
        else:
            initial = name[0].upper()
            initials.append(initial)
    return initials


# Note: Originally I wanted to raise a ValueError if the `parent` XML tag
# does not exist in the given file, but this seems to be impossible (See PEP288
# "There is no existing work-around for triggering an exception inside a
# generator. It is the only case in Python where active code cannot be
# excepted to or through.")
def generate_children(xml_file, parent=None):
    """Generate the child XML elements for a chosen parent element.

    Parameters
    ----------
    xml_file : str, os.PathLike or file
        Path to an XML file, or a file object corresponding to an XML file.

    parent : str
        Tag name or XPath of the parent XML element (containing child elements
        to be generated). If no parent is specified, the root node is assumed
        to be the parent.

    Yields
    ------
    xml.etree.ElementTree.Element
        A child element of the first XML element with the given tag or XPath
        `parent` (if the `xml_file` can be parsed and an element with the
        given tag or XPath `parent` can be found). If no element with the
        given tag or XPath `parent` can be found, a StopIteration is raised.
        If the xml_file cannot be parsed, a ValueError is raised.
    """
    try:
        tree = ET.parse(xml_file)
    except ET.ParseError:
        raise ValueError('`xml_file` cannot be parsed.')
    if parent:
        # Search by XPath or tag of immediate child of root element
        records = tree.find(parent)
        if not records:
            try:
                # Search for first occurence of tag
                records = next(tree.iter(tag=parent))
            except StopIteration:
                return
    else:
        records = tree.getroot()
    for record in records:
        yield record


if __name__ == '__main__':
    xml_file = r'../data/test_data/bibliographic/pubmed/pubmed_results.xml'
    a = ArticleCollection(parse_articles(xml_file))
