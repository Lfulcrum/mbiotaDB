# -*- coding: utf-8 -*-
"""
Created on Sun May 10 09:47:28 2020

@author: William
"""

import pytest
import io
import uuid
import xml.etree.ElementTree as ET

import wip.new_bib_parser as bib


class TestAuthor:
    @pytest.fixture
    def author1_attrs(self):
        author_attrs = {
            'first_name': 'Alice',
            'first_initial': 'A',
            'middle_initials': ['B', 'C'],
            'last_name': 'Smith',
            'articles': [],
        }
        return author_attrs

    @pytest.fixture
    def author2_attrs(self, article1):
        author_attrs = {
            'first_name': 'Bob',
            'first_initial': 'B',
            'middle_initials': ['C', 'D'],
            'last_name': 'Jones',
            'articles': [],
        }
        return author_attrs

    @pytest.fixture
    def author3_attrs(self):
        author_attrs = {
            'first_name': 'Alice',
            'first_initial': 'A',
            'middle_initials': ['C', 'D'],
            'last_name': 'Smith',
            'articles': '[]'
        }
        return author_attrs

    @pytest.fixture
    def author4_attrs(self, article1, article2):
        author_attrs = {
            'first_name': 'Alice',
            'first_initial': 'A',
            'middle_initials': ['C', 'D'],
            'last_name': 'Smith',
            'articles': [article1, article2],
        }
        return author_attrs

    @pytest.fixture
    def collective_author1_attrs(self):
        author_attrs = {
            'name': 'Group_1',
            'articles': [],
        }
        return author_attrs

    @pytest.fixture
    def collective_author2_attrs(self, article1, article2):
        author_attrs = {
            'name': 'Group_2',
            'articles': [article1, article2],
        }
        return author_attrs

    @pytest.fixture
    def article1_attrs(self):
        author_attrs = {
            'pmid': '123',
            'vol': '1',
            'issue': '2',
            'journal': 'Journal of Lunacy',
            'journal_iso': 'journ. lunacy',
            'doi': '10.1000/123',
            'pages': '1-10',
            'title': 'How not to go mad',
            'pub_year': 2020,
            'authors': [],
            'collective_authors': []
        }
        return author_attrs

    @pytest.fixture
    def article2_attrs(self):
        author_attrs = {
            'pmid': '234',
            'vol': '1',
            'issue': '2',
            'journal': 'Journal of Lunacy',
            'journal_iso': 'journ. lunacy',
            'doi': '10.1000/234',
            'pages': '11-20',
            'title': 'How to stay sane',
            'pub_year': 2020,
            'authors': [],
            'collective_authors': []
        }
        return author_attrs

    @pytest.fixture
    def author1(self, author1_attrs):
        return bib.Author(**author1_attrs)

    @pytest.fixture
    def author2(self, author2_attrs):
        return bib.Author(**author2_attrs)

    @pytest.fixture
    def author3(self, author3_attrs):
        return bib.Author(**author3_attrs)

    @pytest.fixture
    def collective_author1(self, collective_author1_attrs):
        return bib.CollectiveAuthor(**collective_author1_attrs)

    @pytest.fixture
    def collective_author2(self, collective_author2_attrs):
        return bib.CollectiveAuthor(**collective_author2_attrs)

    @pytest.fixture
    def article1(self, article1_attrs):
        return bib.Article(**article1_attrs)

    @pytest.fixture
    def article2(self, article2_attrs):
        return bib.Article(**article2_attrs)

    def test_init_author(self, author1_attrs):
        author = bib.Author(**author1_attrs)
        author.__dict__.pop('author_id')
        assert author.__dict__ == author1_attrs

    def test_equal_authors(self, author1_attrs, author1):
        author2 = bib.Author(**author1_attrs)
        assert author1 == author2

    def test_unequal_authors(self, author1, author2):
        assert author1 != author2

    def test_init_author_with_articles(self, author4_attrs):
        author = bib.Author(**author4_attrs)
        author.__dict__.pop('author_id')
        assert author.__dict__ == author4_attrs
        for article in author.articles:
            assert author in article.authors

    def test_article_add_author(self, article1, author1):
        article1.add_author(author1)
        assert author1 in article1.authors
        assert article1 in author1.articles

    def test_init_collective_author(self, collective_author1_attrs):
        author = bib.CollectiveAuthor(**collective_author1_attrs)
        author.__dict__.pop('author_id')
        assert author.__dict__ == collective_author1_attrs

    def test_equal_collective_authors(self, collective_author1_attrs, collective_author1):
        collective_author2 = bib.CollectiveAuthor(**collective_author1_attrs)
        assert collective_author1 == collective_author2

    def test_unequal_collective_authors(self, collective_author1, collective_author2):
        assert collective_author1 != collective_author2

    def test_init_collective_author_with_articles(self, collective_author2_attrs):
        collective_author = bib.CollectiveAuthor(**collective_author2_attrs)
        collective_author.__dict__.pop('author_id')
        assert collective_author.__dict__ == collective_author2_attrs
        for article in collective_author.articles:
            assert collective_author in article.collective_authors

    def test_article_add_collective_author(self, article1, collective_author1):
        article1.add_collective_author(collective_author1)
        assert collective_author1 in article1.collective_authors
        assert article1 in collective_author1.articles


class TestParseAuthor:
    def test_parse_author_forename_and_lastname(self):
        author_xml = io.StringIO(
            '<author>'
            '<ForeName>Alice Bob C.</ForeName>'
            '<LastName>Smith</LastName>'
            '</author>'
        )
        author_elem = ET.parse(author_xml).getroot()
        author = bib.parse_author(author_elem)
        assert author.first_initial == 'A'
        assert author.middle_initials == ['B', 'C']
        assert author.last_name == 'Smith'
        assert isinstance(author.author_id, uuid.UUID)  # Necessary?

    def test_parse_author_no_forename(self):
        author_xml = io.StringIO(
            '<author>'
            '<LastName>Smith</LastName>'
            '</author>'
        )
        author_elem = ET.parse(author_xml).getroot()
        author = bib.parse_author(author_elem)
        assert author.first_initial is None
        assert author.middle_initials is None
        assert author.last_name == 'Smith'
        assert isinstance(author.author_id, uuid.UUID)  # Necessary?

    def test_parse_author_no_lastname(self):
        author_xml = io.StringIO(
            '<author>'
            '<ForeName>Alice Bob C.</ForeName>'
            '</author>'
        )
        author_elem = ET.parse(author_xml).getroot()
        author = bib.parse_author(author_elem)
        assert author is None

    def test_parse_author_no_forename_or_lastname(self):
        author_xml = io.StringIO(
            '<author>'
            '</author>'
        )
        author_elem = ET.parse(author_xml).getroot()
        author = bib.parse_author(author_elem)
        assert author is None

    def test_parse_author_invalid_author_elem(self):
        invalid_author_elem = object()
        with pytest.raises(ValueError):
            bib.parse_author(invalid_author_elem)


@pytest.fixture
def bib_data():
    file_contents = (
        '<Article>'
        '<PMID>27871135</PMID>'
        '<Article>'
            '<Journal>'
                '<JournalIssue>'
                    '<Volume>19</Volume>'
                    '<Issue>1</Issue>'
                    '<PubDate>'
                        '<Year>2017</Year>'
                        '<Month>01</Month>'
                    '</PubDate>'
                '</JournalIssue>'
                '<Title>Environmental microbiology</Title>'
                '<ISOAbbreviation>Environ. Microbiol.</ISOAbbreviation>'
            '</Journal>'
            '<ArticleTitle>Fire modifies the phylogenetic structure of soil bacterial co-occurrence networks.</ArticleTitle>'
            '<Pagination>'
                '<MedlinePgn>317-327</MedlinePgn>'
            '</Pagination>'
            '<ELocationID EIdType="doi">10.1111/1462-2920.13609</ELocationID>'
        '</Article>'
    )
    return io.Stringio(file_contents)

def test_parse_article(bib_data):
    pass


def test_extract_initials():
    valid_names = {
        'Alice': ['A'],
        'Alice Charlie': ['A', 'C'],
        'Alice Bob Charlie': ['A', 'B', 'C'],
        'A. Charlie': ['A', 'C'],
        'Alice, Charlie': ['A', 'C'],
        'A., B., Charlie': ['A', 'B', 'C'],
        'A.B.': ['A', 'B'],
        'A,B,': ['A', 'B'],
        '.A.B.': ['A', 'B'],
        ',A,B,': ['A', 'B'],
        'A . B': ['A', 'B'],
        'A , B': ['A', 'B'],
        'Alice,.,,. .,..,Bob': ['A', 'B'],
        'Alice  .,,.,.  Bob': ['A', 'B'],
        'Alice-Bob': ['A-B'],
        ',Alice-Bob,Charlie,': ['A-B', 'C'],
        '.Alice-Bob.Charlie.': ['A-B', 'C'],
        '..,.,Alice-Bob,,.,. ,.Charlie.,': ['A-B', 'C'],
    }
    invalid_names = {
        'A - B',
        'Alice - Bob',
        'Alice--Bob',
    }
    for names, exp_initials in valid_names.items():
        initials = bib.extract_initials(names)
        assert initials == exp_initials
    bib.extract_initials(names)
    for names in invalid_names:
        with pytest.raises(ValueError):
            bib.extract_initials(names)


class Test_Generate_Children:
    @pytest.fixture
    def valid_xml_file(self):
        valid_xml_file = io.StringIO(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<root><records>'
            '<record>1</record>'
            '<record>2</record>'
            '</records></root>'
        )
        return valid_xml_file

    def test_generate_children_no_args(self, valid_xml_file):
        root_children = bib.generate_children(valid_xml_file)
        root_first_child = next(root_children)
        assert root_first_child.tag == 'records'

    def test_generate_children_parent_arg(self, valid_xml_file):
        parent_children = bib.generate_children(valid_xml_file,
                                                parent='records')
        parent_first_child = next(parent_children)
        assert parent_first_child.text == '1'

    def test_generate_children_parent_not_found(self, valid_xml_file):
        with pytest.raises(StopIteration):
            next(bib.generate_children(valid_xml_file, parent='gibberish'))

    def test_generate_children_invalid_file(self):
        invalid_xml_file = io.StringIO('')
        with pytest.raises(ValueError):
            next(bib.generate_children(invalid_xml_file))
