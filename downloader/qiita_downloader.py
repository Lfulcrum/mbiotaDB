# -*- coding: utf-8 -*-
"""
Download files/data associated with particulars study in Qiita

Created on Thu Mar 28 13:56:02 2019
@author: William
"""

# Standard library imports
import re
import time
import threading
import concurrent.futures
import csv
import json
import os
import os.path
import shutil
import tempfile
from ast import literal_eval
from collections import namedtuple
from glob import glob
from argparse import ArgumentParser
from getpass import getpass
from configparser import ConfigParser

# Third-party imports
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Global variables:
# namedtuple factory is similar to an object specification
Study = namedtuple('Study', ['study_id', 'title', 'num_samples',
                             'num_artifacts', 'pmids', 'dois',
                             'study_link'])


def create_firefox_profile(dl_dir='.', last_dl_dir=2, show_dl_manager=False):
    """Create a firefox profile suitable for download automation."""
    profile = webdriver.FirefoxProfile()
    # Profile settings:
    profile.set_preference('browser.download.dir', dl_dir)
    profile.set_preference('browser.download.folderList', last_dl_dir)
    profile.set_preference('browser.download.manager.showWhenStarting', show_dl_manager)
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk',
                           '''application/octet-stream,
                              application/zip,
                              application/nbib,
                              text/plain,
                              text/csv,
                              text/xml''')
    return profile


def login_to_qiita(driver, username, password):
    """Navigate to Qiita and login."""
    driver.get("https://qiita.ucsd.edu/")
    assert "Qiita" in driver.title
    username_elem = driver.find_element_by_id("username")
    username_elem.send_keys(username)
    password_elem = driver.find_element_by_id("password")
    password_elem.send_keys(password)
    password_elem.send_keys(Keys.RETURN)
    wait = WebDriverWait(driver, 10)
    elem = ('link text', f'Welcome {username}')
    wait.until(EC.presence_of_element_located(elem))
    return driver


# TODO Update docstring
# TODO Should we allow a longer WebDriverWait - allow weak internet connections?
# TODO Use wait.until_not(EC.presence_of_element(no_studies_elem)) instead of
# wait.until(EC.staleness_of(no_studies_elem))
def search_qiita(driver, search_term):
    """Search Qiita for particular string and collect data about each study."""
    # Navigate to study viewer
    driver.get("https://qiita.ucsd.edu/study/list/")
    search_elem = driver.find_element_by_id("study-search-input")
    search_elem.clear()
    # Enter search term
    search_elem.send_keys(search_term)
    search_elem.send_keys(Keys.RETURN)
    # Wait for search results to populate
    study_table = driver.find_element_by_id('studies-table')
    wait = WebDriverWait(driver, 10)
    studies_loading_elem = ('css selector', '#search-waiting')
    wait.until(EC.invisibility_of_element_located(studies_loading_elem))
    no_studies_elem = study_table.find_element_by_css_selector('td.dataTables_empty')
    try:
        wait.until(EC.staleness_of(no_studies_elem))
    except TimeoutException:
        print("No studies were found.")
        return None
    # Display all studies in results (not just first 5)
    select_elem = Select(driver.find_element_by_name('user-studies-table_length'))
    select_elem.select_by_visible_text('All')
    # Get elements from the results table
    study_elems = driver.find_elements_by_css_selector('table#studies-table tbody tr')
    return study_elems


def get_pubs_from_study_elem(study_elem):
    pmids = []
    dois = []
    pubs = study_elem.find_elements_by_css_selector('td:nth-child(7) a')
    for pub in pubs:
        pub_id = pub.text
        if pub_id.strip().lower() == 'to be assigned':
            continue  # Skip "To Be Assigned" link text
        try:
            int(pub_id)
            pmids.append(pub_id)
        except ValueError:
            dois.append(pub_id)
    return pmids, dois


# TODO: Update docstring
def get_study_info(study_elem):
    """Return an object storing study metadata from a Qiita search result.

    Parameters
    ----------
    study_elem :
        <tr> element of the HTML page corresponding to a study in Qiita
        search results.
    """
    # Navigate through a row in the results table
    study_id = study_elem.find_element_by_css_selector('td:nth-child(3)').text
    title = study_elem.find_element_by_css_selector('td:nth-child(2) a:nth-child(2)').text
    num_samples = study_elem.find_element_by_css_selector('td:nth-child(4)').text
    artifacts = study_elem.find_element_by_css_selector('td:nth-child(1)')
    if artifacts.text == 'No BIOMs':
        num_artifacts = 0
    else:
        num_artifacts = artifacts.find_element_by_css_selector(
                'div div div:nth-child(2)').text
    pmids, dois = get_pubs_from_study_elem(study_elem)
    study_link = study_elem.find_element_by_css_selector(
            'td:nth-child(2) a:nth-child(2)').get_attribute('href')
    study = Study(study_id, title, num_samples, num_artifacts,
                  pmids, dois, study_link)
    return study


# TODO: Maybe make a save_studies_to_json() too?
def save_studies_to_csv(studies, output_file='./studies.csv', header=None):
    """Write a collection of studies to a csv file.

    Parameters
    ----------
    studies : collection
        Studies to write.
    output_file : str
        Path to the output csv file.
    header : list
        Headings to be written as header to output_file.
    """
    with open(output_file, 'w', newline='') as studies_file:
        writer = csv.writer(studies_file)
        if header:
            writer.writerow(header)
        for study in studies:
            writer.writerow(study)


# TODO Better to implement using csv.DictReader rather than csv.reader?
# TODO Should we only skip a header if it exists, or should we use it
# to specify the fields of the corresponding Study's namedtuple?
# If former, need to update docstring and change for loop!
def read_studies_from_csv(input_file, header=True):
    """Return a list of studies from a given csv file.

    Parameters
    ----------
    input_file : str
        Path to a csv file.
    header : bool, optional
        Boolean indicating whether a header is present in the csv file.
        The header, if present in the csv file, should consist of the exact
        attribute names of the `Study` namedtuple, in any order.
    """
    studies = []
    with open(input_file) as studies_file:
        reader = csv.reader(studies_file)
        if header:
            header = next(reader)
        for line in reader:
            # Convert all empty strings to None.
            for index, value in enumerate(line):
                if not value:
                    line[index] = None
            # If order of columns differ from Study namedtuple specification.
            if header:
                study = dict(zip(header, line))
            else:
                study = Study(*line)
                study = study._asdict()
            # Convert string repr of list of pmids/dois to python lists
            study['pmids'] = literal_eval(study['pmids'])
            study['dois'] = literal_eval(study['dois'])
            parsed_study = Study(**study)
            studies.append(parsed_study)
    return studies


# TODO: Specifying multiple download attempts seems to fix behaviour for
# some studies, where a click didn't seem to update the webpage with sample
# info. Perhaps there is a better solution?
def download_sample_metadata(driver, max_attempts=1):
    """Download a sample metadata file of a study on Qiita.

    This function attempts to download the metadata file to the download dir
    specified in the firefox profile attached to the given driver.
    Precondition: driver has navigated to the study information page of form
    https://qiita.ucsd.edu/study/description/<STUDY_ID>.

    Parameters
    ----------
    max_attempts : int, optional
        Max number of times to attempt the download before re-raising
        selenium.common.exceptions.TimeoutException.
    """
    wait = WebDriverWait(driver, 5)  # Max 5 second waits
    attempt = 1
    while attempt <= max_attempts:
        try:
            # Wait for and click on 'Sample Information' button
            samp_info = ('css selector', 'div.col-md-3 button:nth-child(2)')
            samp_info = wait.until(EC.presence_of_element_located(samp_info))
            samp_info.click()
            # Wait for and click on 'Sample Info' download button
            samp_elem = ('css selector', 'span#title-h3 a:nth-child(1)')
            samp_elem = wait.until(EC.element_to_be_clickable(samp_elem))
            samp_elem.click()
            break
        except TimeoutException:
            if attempt == max_attempts:
                raise TimeoutException
            else:
                attempt += 1


def download_qiime_and_biom(driver):
    """Download all Qiime maps and BIOM files of a study on Qiita.

    Files are downloaded to the download dir specified in the firefox
    profile attached to the given driver.
    Precondition: driver has navigated to the study information page of form
    https://qiita.ucsd.edu/study/description/<STUDY_ID>.
    """
    wait = WebDriverWait(driver, 10)
    # Wait for and click on 'Sample Information' button
    qiime_biom = ('css selector', 'div.col-md-3 a:nth-child(3)')
    qiime_biom = wait.until(EC.presence_of_element_located(qiime_biom))
    qiime_biom.click()


def generate_processing_elems(driver):
    """Navigates through 16S, 18S and ITU sample preparation pages.

    Generates
    ---------
    list of selenium.webdriver.remote.webelement.WebElement
    """
    wait = WebDriverWait(driver, 1)  # Max 1 second waits
    # Wait for and click on '16S' Data Type button
    panel = ('css selector', 'div.panel.panel-default')
    wait.until(EC.presence_of_element_located(panel))
    try:
        # Find 16S processing elems
        driver.find_element_by_css_selector('div#heading16S h4 a').click()
        panel_16S = ('css selector', 'div#collapse16S div.panel-body div.panel-body-element a')
        # Loop to wait for 16S element to become available
        while True:
            try:
                wait.until(EC.element_to_be_clickable(panel_16S))
                break
            except TimeoutException:
                driver.find_element_by_css_selector('div#heading16S h4 a').click()
        yield wait.until(EC.presence_of_all_elements_located(panel_16S))
    except NoSuchElementException:
        yield []
    try:
        # Find 18S processing elems
        driver.find_element_by_css_selector('div#heading18S h4 a').click()
        panel_18S = ('css selector', 'div#collapse18S div.panel-body div.panel-body-element a')
        yield wait.until(EC.presence_of_all_elements_located(panel_18S))
    except NoSuchElementException:
        yield []
    try:
        # Find ITS processing elems
        driver.find_element_by_css_selector('div#headingITS h4 a').click()
        panel_18S = ('css selector', 'div#collapseITS div.panel-body div.panel-body-element a')
        yield wait.until(EC.presence_of_all_elements_located(panel_18S))
    except NoSuchElementException:
        yield []


# TODO: This seems to be temperamental. How to make robust?
def download_sample_prep_data(driver):
    """Download a preparation metadata file and Qiime map file of a study on Qiita.

    Precondition: driver has navigated to the study information page of form
    https://qiita.ucsd.edu/study/description/<STUDY_ID> and a particular sample
    data type (16S, 18S, ITS) and sample prep has been clicked.
    """
    wait = WebDriverWait(driver, 10)
    prep_info = ('css selector', 'div.col-md-12 h4 a:nth-child(3)')
    prep_info = wait.until(EC.presence_of_element_located(prep_info))
    prep_info.click()
    qiime_map = ('css selector', 'div.col-md-12 h4 a:nth-child(4)')
    qiime_map = wait.until(EC.presence_of_element_located(qiime_map))
    qiime_map.click()


# TODO: This seems to be temperamental. How to make robust?
# TODO: Can we avoid passing arifacts as an argument to parse_processing_tree?
# i.e. Are objects returned from the parse_processing_tree passed on to the
# threading executor and returned from its method call? I don't think so.
def download_processing_params_and_bioms(driver, process_elem):
    """Get parameters for data processing jobs for a study on Qiita.

    When this function is called with appropriate arguments, it should populate
    the webpage with data that includes a processing network (HTML canvas
    element), as if the user had clicked on a particular sample preparation
    (under 16S, 18S or ITS tab, which at the time of writing, is located on
    in an expandable pane on the left of the study information page). The
    function will then wait for user interaction with artifacts in this
    processing network. With each click, the function will attempt to extract
    processing parameters and their values, as well as download available
    BIOM files associated with the artifacts. These files are downloaded to the
    download dir specified in the firefox profile attached to the given driver.

    The function coordinates threading (2 processes) to enable user interaction.

    Precondition: driver has navigated to the study information page of form
    https://qiita.ucsd.edu/study/description/<STUDY_ID>.

    Returns
    -------
    dict
        Dictionary whose key is the preparation identifier for the given
        processing element. The value is another dictionary whose keys are
        parameter names and values are corresponding parameter values for the
        processing job.

    Parameters
    ----------
    process_elem : selenium.webdriver.remote.webelement.WebElement
        The element of the list generated by generate_processing_elems.
    """
    wait = WebDriverWait(driver, 10)
    process_elem.click()
    canvas = ('css selector', 'div#processing-network-div div.vis-network canvas')
    canvas = wait.until(EC.presence_of_element_located(canvas))
    # Dictionary to store job parameters for each artifact
    artifacts = {}
    # Get the preparation id (qiita_prep_id column in sample prep file)
    prep_name = process_elem.find_element_by_tag_name('span').text
    prep_id_re = re.compile(r'ID (\d+)')
    prep_id = prep_id_re.search(prep_name).group(1)
    # Use two threads to implement "listener" for semi-automated collection
    # of data from processing network.
    event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(wait_for_user, event)
        future = executor.submit(parse_processing_tree, driver, event, artifacts)
    try:
        raise future.exception()
    # TODO: Under what conditions do we expect a TypeError to be raised and
    # why should we ignore it?
    except TypeError:
        pass
    return {prep_id: artifacts}


def wait_for_user(event):
    input('Press Enter when you have finished parsing the processing network...\n')
    event.set()


# TODO: Still uncertain about the strategy used here to wait for the presence
# of a button. Waiting using time.sleep is not recommended for AJAX, but was
# found to be less temperamental for this purpose than Selenium's explicit/
# implicit waits.
# Note: This function is never really called by the user, but is used within
# download_processing_params_and_bioms, along with wait_for_user.
def parse_processing_tree(driver, event, artifacts):
    """Extract processing parameters from artifacts in a processing tree.

    Returns
    -------
    dict
        Dictionary whose keys are identifiers of artifacts that a user has
        clicked on (in the processing network/tree). The values are
        dictionaries containing parameter names and their associated values.

    Parameters
    ----------
    event : threading.Event
        Same threading event supplied to wait_for_user.

    """
    id_re = re.compile(r'ID: (\d+)')
    biom_re = re.compile(r'\(biom\)')
    wait = WebDriverWait(driver, 2)  # Max 2 second waits
    while not event.is_set():
        time.sleep(0.1)  # Prevent CPU working too hard
        # Find part of the webpage showing the artifact idenfier (it will be
        # detected after the artifact is clicked in the processing network)
        try:
            driver.find_element_by_css_selector(
                'div#processing-results div.row div.col-md-12 h4'
            )
        except NoSuchElementException:
            continue
        try:
            # Extract the specific artifact identifier
            identifier = ''
            identifier = driver.find_element_by_css_selector(
                'div#processing-results div.row div.col-md-12 h4 i:nth-child(2)'
            ).text
            identifier = id_re.search(identifier).group(1)
            # Check if data has already been extracted from this artifact
            # If it has, we increment a counter (preventing an extraction
            # success message from being printed more than once) and move to
            # the next iteration of the while loop.
            if identifier not in artifacts:
                counter = 1  # Record number of attempted extractions for an artifact
                artifacts[identifier] = {}
                # Collect processing parameter info
                try:
                    buttonless = False  # Assume a button can be found
                    # Wait for button (AJAX must populate webpage)
                    # Note: Don't want to wait too long as user may click on
                    # next artifact
                    time.sleep(0.5)
                    # Find button for 'Show processing information'
                    button = driver.find_element_by_css_selector(
                        'div#processing-results div.row div.col-md-12 h4 button'
                    )
                    button.click()
                except NoSuchElementException:
                    # Check whether new identifier available.
                    # TODO: Not sure if necessary. User would have to be very very fast.
                    new_identifier = driver.find_element_by_css_selector(
                        'div#processing-results div.row div.col-md-12 h4 i:nth-child(2)'
                    ).text
                    new_identifier = id_re.search(new_identifier).group(1)
                    if identifier != new_identifier:
                        # User clicked too quickly on another element in
                        # processing tree (before a button was detected)
                        raise Exception
                    else:
                        # Assume that if a button couldn't be found after the
                        # wait, then there is no button on the page and that
                        # this is normal for the artifiact in question (e.g.
                        # root artifact).
                        buttonless = True
                if not buttonless:
                    # Continue to extract available processing parameter data
                    process_info = ('css selector', 'div#processing-info div.row.form-group')
                    process_info = wait.until(
                            EC.presence_of_all_elements_located(process_info)
                    )
                    for element in process_info:
                        key = element.find_element_by_tag_name('label').text
                        # Process the parameter name
                        key = key.strip().replace(':', '').lower()
                        try:
                            # No processing is performed on a parameter value
                            value = element.find_element_by_css_selector('div.col-sm-5').text
                        except NoSuchElementException:
                            continue
                        artifacts[identifier][key] = value
                    # Download BIOM files (if present)
                    process_files = ('css selector', 'div#available-files-div a')
                    process_files = wait.until(
                        EC.presence_of_all_elements_located(process_files)
                    )
                    for file_elem in process_files:
                        if biom_re.search(file_elem.text):
                            file_elem.click()
            else:
                counter += 1
        except Exception:
            # If an error is encountered during extraction, remove any data
            # extracted for that processes and start the extraction process anew
            if identifier:
                del artifacts[identifier]
                continue
        else:
            # Print out a message indicating successful extraction.
            if counter == 1:
                print(f'Successfully added identifier: {identifier}')
    return artifacts


def write_processing_data(prep, write_dir):
    """Write processing data collected for a Qiita study's preparation to a JSON file.

    Parameters
    ----------
    prep : dict (or any python data structure that can be converted to JSON)
        The dictionary contains processing information for each preparation.
        The keys of the dictionary are prep identifiers, while its values are
        dictionaries whose keys are processing artifact identifiers and whose
        values are yet another dictionary. This dictionary, in turn, contains
        information on the process that generated the artifact (processing
        parameter names and their associated values).
    write_dir : str
        Path to which the JSON file is written.
    """
    write_string = json.dumps(prep, sort_keys=True, indent=4)
    output_filename = os.path.join(write_dir, 'prep_data.json')
    with open(output_filename, 'w') as file:
        file.write(write_string)


# TODO might want to generalize to check a list of directories
def wait_for_full_download(dl_dir):
    """Wait until files with extension .part are not found in dl_dir."""
    tmpfile_path = os.path.join(dl_dir, '*.part')
    while glob(tmpfile_path):
        time.sleep(0.1)  # Check in 0.1s intervals


# TODO Perhaps combine this function with delete_files (below), by having an
# `action` parameter that describes whether files should be moved or deleted.
# Is this too risky - can the user easily make the mistake of choosing the
# wrong action. I suppose it would have to be a positional parameter (no default
# value).
# TODO Reimplement using os.scandir
def move_files(source_dir, dest_dir):
    """Move files (but no directories) from the source_dir into dest_dir."""
    filenames = os.listdir(source_dir)
    for file in filenames:
        file = os.path.join(source_dir, file)
        if not os.path.isdir(file):
            shutil.move(file, dest_dir)


# TODO Reimplement using os.scandir
def delete_files(source_dir):
    """Delete all files (but no directories) in the source_dir."""
    filenames = os.listdir(source_dir)
    for file in filenames:
        file = os.path.join(source_dir, file)
        if not os.path.isdir(file):
            os.remove(file)


def get_config(filename='config.ini', section='qiita'):
    """Read a config file for Qiita login credentials."""
    parser = ConfigParser()
    parser.read(filename)
    if parser.has_section(section):
        config = parser.items(section)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return dict(config)


def get_citations_from_pubmed(driver, pmids, file_format='nbib'):
    """Download citations for PMIDs, in a given file format, from Pubmed.

    Given a sequence of Pubmed identifiers (PMIDs), this function navigates to
    Pubmed and downloads corresponding citations as a file of a given
    file format. The file will be downloaded to the download dir specified in
    the firefox profile attached to the given driver.

    Parameters
    ----------
    pmids : Sequence
        A sequence of Pubmed identifiers (PMIDs).
    file_format : {'nbib', 'summary', 'abstract', 'medline', 'xml', 'pmid', 'csv'}
        File format of the citations file to be downloaded from Pubmed.
    """
    # Dictionary mapping supported file_format arguments and Pubmed field values
    file_format_map = {
            'summary': 'docsum',
            'abstract': 'abstract',
            'medline': 'medline',
            'xml': 'xml',
            'pmid': 'uilist',
            'csv': 'csv',
            'nbib': None
            }
    if file_format not in file_format_map:
        raise Exception(f"""Error: '{file_format}' is an invalid keyword
                        argument for file_format.""")
    wait = WebDriverWait(driver, 10)
    # Search Pubmed for PMIDs
    search_string = ' '.join(list(pmids))
    driver.get('https://www.ncbi.nlm.nih.gov/pubmed/')
    search_field = driver.find_element_by_id('term')
    # Note: Executing JS statement is faster than using Selenium's send_keys
    # Enter (long) search string into search field
    js_search = """document.getElementById('term').value = {!r};""".format(search_string)
    driver.execute_script(js_search)
    search_field.send_keys(Keys.RETURN)
    if file_format in ('summary', 'abstract', 'medline', 'xml', 'pmid', 'csv'):
        # Click 'Send to' link
        select_download = wait.until(
                EC.element_to_be_clickable(('css selector', '#sendto a'))
        )
        select_download.click()
        # Click 'File' radio button
        select_download = wait.until(
                EC.presence_of_element_located(('id', 'send_to_menu'))
        )
        select_file = wait.until(
                EC.visibility_of_element_located(('id', 'dest_File'))
        )
        select_file.click()
        # Select a download format
        select_format = Select(select_download.find_element_by_id('file_format'))
        select_format.select_by_value(file_format_map[file_format])
        # time.sleep(2)  # TODO: Still necessary?
        wait.until(EC.element_to_be_clickable(
                ('css selector', 'div#submenu_File button')
        )).click()
    if file_format == 'nbib':
        # Get the total number of search results
        num_results = wait.until(EC.presence_of_element_located(
                ('css selector', 'h3.result_count.left')
        )).text
        num_results = int(num_results.split()[-1])
        for result_index in range(1, num_results, 200):
            # Click 'Send to' link
            select_download = wait.until(
                    EC.element_to_be_clickable(('css selector', '#sendto a'))
            )
            select_download.click()
            # Click 'Citation Manager' radio button
            select_download = wait.until(
                    EC.presence_of_element_located(('id', 'send_to_menu'))
            )
            # TODO Sometimes seems to be required, sometimes not. Better way?
            # time.sleep(5)
            citation_radio = wait.until(
                    EC.visibility_of_element_located(('id', 'dest_CitationManager'))
            )
            citation_radio.click()
            # Select the 'Citation manager' download option (for nbib file)
            try:
                num_cites = select_download.find_element_by_id('citman_count')
                cite_start = select_download.find_element_by_id('citman_start')
                # Export 200 citations at a time (max possible!)
                # Note: May need to concatenate files post-download!
                Select(num_cites).select_by_value('200')
                cite_start.clear()
                cite_start.send_keys(result_index)
            except NoSuchElementException:
                # When there are only a few citations, dropdowns are not in DOM
                pass
            wait.until(EC.element_to_be_clickable(
                    ('css selector', 'div#submenu_CitationManager button'))
            ).click()


def concatenate_files(in_files, out_file):
    """Concatenates files in order to generate an specified output file."""
    files = sorted(in_files)
    with open(out_file, 'w') as outfile:
        for file in files:
            with open(file) as infile:
                for line in infile:
                    outfile.write(line)


if __name__ == '__main__':
    # Command-line argument parsing
    parser = ArgumentParser(description='Download data from Qiita.')
    parser.add_argument('--headless', action='store_true',
                        help='For headless (invisible) browser.')
    parser.add_argument('--driver', action='store', default='.',
                        help='Specify a path to Gekodriver (for Firefox).')
    parser.add_argument('-s', '--search', action='store',
                        help='Specify a search term for Qiita database.')
    parser.add_argument('-l', '--load', action='store',
                        help='Specify a path to a file obtained previously using '
                        '--download option \'study\'.')
    parser.add_argument('-d', '--dir', default='./output',
                        help='Specify an output directory in which to save all data.')
    parser.add_argument('-c', '--config', action='store',
                        help='Supply path to a config file from which to read '
                        'Qiita login details.')
    parser.add_argument('--download', nargs='+',
                        choices=['all', 'biom', 'qiime', 'prep', 'tree',
                                 'sample', 'study', 'citation'],
                        default='study',
                        help='Specify what to download from Qiita.')
    # TODO: how to specify that this option is required if --download 'citation' is given?
    parser.add_argument('--citation-format', action='store',
                        choices=['summary', 'abstract', 'medline', 'xml',
                                 'pmid', 'csv', 'nbib'],
                        help="""Specify a citation file format (if 'citation'
                        given as an argument for --download).""")
    args = parser.parse_args()

    # Put the Geckodriver path in the PATH variable.
    # Geckodriver works for Firefox 47.0.1 and greater.
    driver_path = args.driver
    if driver_path not in os.environ['PATH']:
        if os.environ['PATH'][-1] == os.pathsep:
            os.environ['PATH'] += driver_path
        else:
            os.environ['PATH'] += os.pathsep + driver_path

    try:
        # Use headless browser or not
        opts = Options()
        if args.headless:
            opts.headless = True

        # Create a firefox profile and driver with appropriate preferences
        temp = tempfile.TemporaryDirectory()
        dl_path = args.dir
        # TODO: Is it more useful to make the directory at the specified path if
        # it doesn't exist?
        if not os.path.exists(dl_path):
            # TODO: use more appropriate exception
            raise Exception('Path does not exist.')
        profile = create_firefox_profile(dl_dir=temp.name)
        driver = webdriver.Firefox(firefox_profile=profile, options=opts)

        # Login to Qiita
        if args.config:
            config = get_config(filename=args.config, section='qiita')
            username = config['username']
            password = config['password']
        else:
            # User did not specify config file path using --config option
            username = input('Enter Qiita username: ')
            password = getpass('Enter Qiita password: ')
        driver = login_to_qiita(driver, username, password)

        # Search Qiita and download data for every study
        pmids = set()
        failed_dl = []
        # TODO: Is there a way to specify incompatible options i.e. we cannot specify
        # both a 'search' and 'load' option.
        if args.search:
            study_elems = search_qiita(driver, args.search)
            studies = []
            for study_elem in study_elems:
                study = get_study_info(study_elem)
                pmids.update(study.pmids)
                studies.append(study)
        elif args.load:
            studies = read_studies_from_csv(args.load, header=True)
            for study in studies:
                pmids.update(study.pmids)
        dl_opts = set(args.download)
        if 'all' in dl_opts:
            dl_opts = {'biom', 'qiime', 'prep', 'sample', 'study', 'citation',
                       'tree'}

        # Check for download options that require study directory creation
        if dl_opts.intersection({'biom', 'qiime', 'prep', 'sample', 'tree'}):
            for study in studies:
                driver.get(study.study_link)
                # TODO: Currently, functions are downloading multiple things.
                # Qiime maps are in both the zip downloaded by download_qiime_and_biom
                # and in the 16S processing. Not a problem really... Need to check that
                # the maps are not different.
                try:
                    if 'sample' in dl_opts:
                        download_sample_metadata(driver, max_attempts=2)
                    if 'biom' in dl_opts:
                        download_qiime_and_biom(driver)
                    if dl_opts.intersection({'prep', 'qiime', 'tree'}):
                        prep_params = []
                        for elems in generate_processing_elems(driver):
                            for elem in elems:
                                elem.click()
                                if 'prep' in dl_opts or 'qiime' in dl_opts:
                                    download_sample_prep_data(driver)
                                if 'tree' in dl_opts:
                                    params = download_processing_params_and_bioms(driver, elem)
                                    prep_params.append(params)
                        write_processing_data(prep_params, temp.name)
                    wait_for_full_download(temp.name)
                    # Populate the user-specified directory with subdirectories
                    # containing downloaded files
                    # TODO: Handle exception if a study directory already exists?
                    # Should the user always have to download into a clean dir?
                    study_dir = os.path.join(dl_path, study.study_id)
                    os.mkdir(study_dir)
                    move_files(temp.name, study_dir)
                    # TODO: Possible bugs when moving files. E.g. 0kb zip files
                except TimeoutException:
                    failed_dl.append(study.study_id)
                    delete_files(temp.name)
        # Download options that do not require study directory creation
        if 'study' in dl_opts:
            header = Study._fields
            studies_file = os.path.join(dl_path, 'studies.csv')
            save_studies_to_csv(studies, output_file=studies_file, header=header)
        if 'citation' in dl_opts:
            if not args.citation_format:
                raise Exception("Please provide a valid citation file format "
                                "using the --citation-format option.")
            get_citations_from_pubmed(driver, pmids,
                                      file_format=args.citation_format)
            wait_for_full_download(temp.name)
            if 'nbib' in args.citation_format:
                # Concatenate nbib files
                nbib_filenames = [os.path.join(temp.name, filename)
                                  for filename in os.listdir(temp.name)
                                  if filename.lower().endswith('.nbib')]
                output_path = os.path.join(dl_path, 'citations.nbib')
                concatenate_files(nbib_filenames, output_path)
                # Remove individual .nbib files before moving all citation files
                for file in nbib_filenames:
                    os.remove(file)
            move_files(temp.name, dl_path)
    except Exception as error:
        raise(error)
    finally:
        failed_dl_file = os.path.join(dl_path, 'failed_dl.txt')
        with open(failed_dl_file, 'w') as file:
            file.writelines('{}\n'.format(study_id) for study_id in failed_dl)
        temp.cleanup()
        driver.quit()
