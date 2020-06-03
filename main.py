# -*- coding: utf-8 -*-
"""
This script is used to informally test various parts of data cleaning, 
parsing and database functionality.

@author: William
"""

# Standard library imports
import os
import glob
import re
import datetime
import csv
import tempfile
import time
from collections import defaultdict

# Third-party imports
from selenium import webdriver

# Local application imports
from downloader.qiita_downloader import (
    create_firefox_profile, get_config, login_to_qiita, search_qiita,
    generate_processing_elems, get_study_info, download_sample_prep_data,
    download_processing_params_and_bioms, write_processing_data
)
from creator import session_scope, Session
from creator.sample_parser import infer_date_formats, parse_objects
from creator.prep_parser import parse_preparations, parse_workflows
from creator.count_parser import (get_dirs, get_prep_filenames, get_biom_filenames,
                                  get_proc_id_from_biom, get_counts)
from creator.bib_parser import update_bib_from_xml
from model import Count
from wip.new_sample_parser import (parse_file, convert_units,
                                       re_missing, convert_sex, convert_csection)
from wip.subject_sample_ideas import parse_samples, parse_subjects, form_relationship


def best_parser(session, sample_file, prep_files, proc_file, biom_files):
	"""Parse multiple prep and BIOM files when parsing a study."""
    # Sample metadata
    experiments, subjects, samples = parse_objects(
            sample_file,
            returning=['experiments', 'subjects', 'samples']
    )
    # Data processing metadata
    prep_workflows, terminal_workflows = parse_workflows(proc_file,
        index_by=['prep', 'terminal_proc'])
    # Sample prep metadata
    preparations = {}  # Contains preps from multiple prep files
    for prep_file in prep_files:
        dayfirst_dict = infer_date_formats(prep_file)
        preps, prep_samples = parse_preparations(prep_file,
                                                 dayfirst_dict,
                                                 index_by=['id','sample'])
        # Establish relationship: Sample to preps
        for prep_id, prep in preps.items():
            for sample_id in prep_samples[prep_id]:
                samples[sample_id].add_preparation(prep)
            preparations[prep_id] = prep
    # Establish relationship: Preps to workflows
    for prep_id, workflows in prep_workflows.items():
        preparations[prep_id].workflows = workflows
    # Biom/Count data
#    proc_bioms = {}
    for biom_file in biom_files:
        counts = get_counts(biom_file, session)
        biom_proc_id = get_proc_id_from_biom(biom_file)
        # Establish relationship: Workflows to counts.
        terminal_workflows[biom_proc_id].count_dict = counts
#        proc_bioms[biom_proc_id] = counts
    # Establish relationship: Workflows to counts.
    # Tried to do this in above loop!
#    for proc_id, workflow in terminal_workflows.items():
#        workflow.count_dict = proc_bioms[proc_id]

    # Start database session
    start = time.time()
    with session_scope() as session_2:
        for exp_id, experiment in experiments.items():
            for subject in experiment.subjects:
                for sample in subject.samples:
                    for prep in sample.preparations:
                        for workflow in prep.workflows:
                            try:
                                workflow.count_dict
                            except AttributeError:
                                continue
                            for count in workflow.count_dict[sample.orig_sample_id]:
                                fact = Count(experiment=experiment,
                                             subject=subject,
                                             sample=sample,
                                             sample_site=sample.sampling_site,
                                             sample_time=sample.sampling_time,
                                             preparation=prep,
                                             workflow=workflow,
                                             lineage=count.lineage,
                                             seq_variant=count.seq_var,
                                             count=count.count)
                                session_2.add(fact)
    end = time.time()
    print("Main loop took: ", end-start)


def parser(session):
	"""Parse individual prep and BIOM files when parsing a study.
	
	Note: This method will produce duplicates of sample, subject, and
	processing data each time a BIOM file is inserted. It also unfortunately
	duplicates count data (for each processing in the prep data file). This
	is a bug that is solved in the best_parser() function."""
    ### Test time series data ###
    # Study 101 BIOM file stats:
    exp_dir = './data/test_data/experiments/101'
    sample_file = os.path.join(exp_dir, '101_20171109-130044.txt')
    prep_file = os.path.join(exp_dir, '101_prep_237_qiime_20190428-053528.txt')
    proc_file = os.path.join(exp_dir, 'prep_data.json')
#    # non-zero entries: 9092
#    # shape: (2137, 61)
#    # filesize: 639 KB
#    # Time taken to parse: 67.9012
    biom_file = os.path.join(exp_dir, '44767_otu_table.biom')
#    # filesize: 493 KB
#    # Time taken to parse: 49.1731
#    biom_file = os.path.join(exp_dir, '44770_otu_table.biom')
#    # filesize: 672 KB
#    # Time taken to parse: 67.8308
#    biom_file = os.path.join(exp_dir, '44771_otu_table.biom')

    # .tre file associated
#    exp_dir = r'.\data\test_data\experiments\101'
#    sample_file = os.path.join(exp_dir, '101_20171109-130044.txt')
#    prep_file = os.path.join(exp_dir, '101_prep_237_qiime_20190428-053528.txt')
#    proc_file = os.path.join(exp_dir, 'prep_data.json')
#    biom_file = os.path.join(exp_dir, '56522_reference-hit.biom')

    # Processing data
    dayfirst_dict = infer_date_formats(prep_file)
    experiments, subjects, samples = parse_objects(
            sample_file,
            returning=['experiments', 'subjects', 'samples']
    )
    preps, prep_samples = parse_preparations(prep_file,
                                             dayfirst_dict,
                                             index_by=['id','sample'])
    prep_workflows, terminal_workflows = parse_workflows(proc_file,
        index_by=['prep', 'terminal_proc'])
    counts = get_counts(biom_file, session)
    # Establish relationships:
    # Sample to preps
    for prep_id, prep in preps.items():
        for sample_id in prep_samples[prep_id]:
            samples[sample_id].preparations = prep
    # Preps to workflows
    for prep_id, workflows in prep_workflows.items():
        preps[prep_id].workflows = workflows
    # Workflows to counts
    for proc_id, workflow in terminal_workflows.items():
        biom_proc_id = get_proc_id_from_biom(biom_file)
        if biom_proc_id:
            workflow.count_dict = counts

    # Start database session
    start = time.time()
    with session_scope() as session_2:
        for exp_id, experiment in experiments.items():
            for subject in experiment.subjects:
                for sample in subject.samples:
                    for prep in sample.preparations:
                        for workflow in prep.workflows:
                            for count in workflow.count_dict[sample.orig_sample_id]:
                                fact = Count(experiment=experiment,
                                             subject=subject,
                                             sample=sample,
                                             sample_site=sample.sampling_site,
                                             sample_time=sample.sampling_time,
                                             preparation=prep,
                                             workflow=workflow,
                                             lineage=count.lineage,
                                             seq_variant=count.seq_var,
                                             count=count.count)
                                session_2.add(fact)
    end = time.time()
    print("Main loop took: ", end-start)


def subject_sample_parser():
    sample_file = './data/sample_subject_metadata/sample0.txt'
    df = parse_file(sample_file, na_regex=re_missing, strip=True,
                    column_types={'numeric': ['age'],
                                  'timestamp': ['collection_timestamp'],
                                  # 'interval': ['original_date']
                                  })
    subject_attr_map = {'sex': 'sex',
                        'country': 'country',
                        'obesitycat': 'disease',
                        'qiita_study_id': 'orig_study_id',
                        'host_subject_id': 'orig_subject_id'}
    sample_attr_map = {'collection_timestamp_date': 'sample_date',
                       'collection_timestamp_time': 'sample_time',
                       'age': 'age',
                       'latitude': 'latitude',
                       'longitude': 'longitude',
                       'elevation': 'elevation',
                       'host_subject_id': 'orig_subject_id'}
    samples = parse_samples(df, 'sample_name', sample_attr_map)
    subjects = parse_subjects(df, 'host_subject_id', subject_attr_map)
    form_relationship(df, 'sample_name', 'host_subject_id', samples, subjects)
    return samples, subjects

	
def new_sample_parser():
    input_file = './data/sample_subject_metadata/sample4.txt'
    d = parse_file(input_file,
                   na_regex=re_missing,
                   column_types = {'unit': ['age_unit'],
#                                   'date': ['orig_collection_timestamp'],
                                   'interval': ['orig_collection_timestamp'],
                                   'time': ['collection_time'],
                                   'timestamp': ['weird', 'collection_timestamp'],
                                   'numeric': ['age']},
                   invalid_dates = {'collection_timestamp': {'before': datetime.date(2013,3,18),
                                                              'invalid_dates': [datetime.date(2013,8,18)],
                                                              'invalid_ranges': [(datetime.date(2013,6,6), datetime.date(2013,6,20))]}
                                    },
                   invalid_times = {'collection_time': {'invalid_times': [datetime.time(8,0,0)],
                                                        'invalid_ranges': [(datetime.time(10,0), datetime.time(12,0))]}
                                    },
                    converters={'sex': convert_sex, 'delivery_mode': convert_csection}
#                   use_columns=['age_unit', 'host_subject_id']
                   )
    c = convert_units(d, values_to_units={'age': 'age_unit', 'elevation': 'meters'},
                      to_units={'age': 'megaseconds', 'elevation': 'inches'},
                      remove_unit_columns=False, add_unit_columns=True,
                      decimal_places={'age': 2, 'gibberish': 4})
    # Keep only columns of interest
    keep_columns = ['sample_name', 'host_subject_id', 'age', 'collection_timestamp_date',
                    'collection_timestamp_time', 'delivery_mode', 'sex', 'elevation',
                    'latitude', 'longitude']
    c = c.loc[:, keep_columns]
    # Remove rows that are not of interest
    c = c.loc[c['sample_name'] != '10778.S1165']
    # Rename columns
    new_column_names = dict(zip(
        keep_columns,
        ['sample_name', 'subject_name', 'age', 'collection_date',
        'collection_time', 'csection', 'sex', 'elevation',
        'latitude', 'longitude']
    ))
    c.rename(columns=new_column_names, inplace=True)
    return c


def sample_prep_connector():
    sample_file = './data/sample_subject_metadata/sample1.txt'
    prep_file = './data/prep_metadata/prep0.txt'
    dayfirst_dict = infer_date_formats(prep_file)
    experiments, subjects, samples = parse_objects(
            sample_file,
            returning=['experiments', 'subjects', 'samples']
    )
    preps, prep_samples = parse_preparations(prep_file,
                                             dayfirst_dict,
                                             index_by=['id','sample'])
    for prep_id, prep in preps.items():
        for sample_id in prep_samples[prep_id]:
            samples[sample_id].preparation = prep
    return samples

def prep_parser_main():
    exp_dir = r'.\data\test_data\317'
    proc_file = os.path.join(exp_dir, 'prep_data.json')
    prep_workflows, proc_workflows = parse_workflows(proc_file,
        index_by=['prep', 'terminal_proc'])
    return prep_workflows, proc_workflows

def count_parser_main():
    root = r'C:\Users\William\Documents\OneDrive backup\Bioinformatics\Thesis\Qiita\human_test2'
    dir_generator = get_dirs(root)
    for dir_entry in dir_generator:
        study_id = dir_entry.name
        prep_files = get_prep_filenames(dir_entry.path)
        biom_files = get_biom_filenames(dir_entry.path)
        for file in biom_files:
            with session_scope() as session:
                counts = get_counts(os.path.join(dir_entry.path, file), session)
            break
#            biom_path = os.path.join(dir_entry.path, file)
#            table = biom.load_table(biom_path)
#            otu_ids = table.ids(axis='observation')
#            samp_ids = table.ids(axis='sample')
#            tree_file = get_tree_filename(dir_entry.path, file)
#            if tree_file:
#                tree_path = os.path.join(dir_entry.path, tree_file)
#                tree = Phylo.read(tree_path, 'newick')
#                taxa = get_tree_taxa(tree)
#            else:
#                tree = None
#                taxa = None
#            for otu_id in otu_ids:
#                lineage = get_lineage(table, otu_id, tree, taxa)
#                print(lineage)
        break
    print([count.__dict__ for count in counts])


    # This bit of code confirms that there are no common sample identifiers
    # between different prep files for study 10317, but that doesn't mean that
    # this will always be the case. In fact, I checked other studies...
    # Other studies with multiple preps:
    # 10532, 10581, 11358, 11550, 11740, 11874
    # Some samples in 11358 are exactly the same in both prep files
    # Upon manual inspection of the prep files, all sequencing attributes incl
    # sequence date were identical between the files (except the
    # experiment_design_description). We see that there are more samples in
    # prep with ID 4944 than 3753, but these observations suggest that the
    # same samples and accompanying sequencing data were included in two
    # different preps i.e. they didn't resequence the older physical samples,
    # they simply merged the data.
    # This tells us that we need to be able to identify when the same sample is
    # found in multiple preps and handle accordingly - try to work out if it is
    # a new sequencing run or the same sequencing run (best way is to check the
    # sequence date).
    # Qiita philosophy page tells us to use 'run_prefix' column to differentiate
    # between sequencing runs.
    path = r'C:\Users\William\Documents\OneDrive backup\Bioinformatics\Thesis\Qiita\human_test2\11874'
    os.chdir(path)
    prep_files = get_prep_filenames(path)
    sample_id_dict = defaultdict(set)
    for file in prep_files:
        with open(file) as file:
            reader = csv.DictReader(file, delimiter='\t')
            for row in reader:
                prep_id = row['qiita_prep_id']
                sample_id_dict[prep_id].add(row['sample_name'])
    common_samples = set.intersection(*sample_id_dict.values())


def bib_parser_main():
    studies = r'./data/test_data/bibliographic/pubmed/pubmed_result.xml'
    with session_scope() as session:
         update_bib_from_xml(studies, session)


def qiita_downloader_main():
    # SOME TEST CODE:
    # Add driver to PATH
	# Note to user: Change the below path to the directory
	# containing the geckodriver.
    driver_path = r'./downloader/geckodriver'
    path = os.environ['PATH'].split(';')
    path.append(driver_path)
    os.environ['PATH'] = ';'.join(path)
    # Setup variables, firefox profile and Selenium webdriver
    temp = tempfile.TemporaryDirectory()
    dl_path = r'C:\Users\William\Documents\OneDrive backup\Bioinformatics\Thesis\Qiita\test8'
    profile = create_firefox_profile(dl_dir=dl_path)
    driver = webdriver.Firefox(firefox_profile=profile)
    config = get_config(filename='database.ini', section='qiita')
    username = config['username']
    password = config['password']
    studies = []
    # Login, search
    login_to_qiita(driver, username, password)
    study_elems = search_qiita(driver, 'lean obese') # '10317 18S'
    list_prep = []
    for study_elem in study_elems:
        study = get_study_info(study_elem)
        studies.append(study)
    for study in studies:
        driver.get(study.study_link)
        for elems in generate_processing_elems(driver):
            for elem in elems:
                elem.click()
                download_sample_prep_data(driver)
                prep = download_processing_params_and_bioms(driver, elem)
                list_prep.append(prep)
                break
    write_processing_data(list_prep, dl_path)


    # Test PMID download:
#    pmids = ['27192542', '23077104']
#    pmids = ['26005845', '28822895', '27414495', '27392936', '28222117', '28971851', '28940049', '29166320', '28253277', '27383984', '22861806', '28107528', '26366711', '28258145', '26020247', '28492938', '27306663', '29030459', '25146375', '28517974', '26687338', '29316741', '29075620', '26455879', '28665684', '22683412', '25599982', '27866880', '27153496', '27111847', '28030377', '28939409', '28714965', '25766736', '27391224', '21624126', '28245856', '25148482', '29386298', '26412384', '28872698', '24943724', '28170403', '27142181', '28121709', '27364497', '28759053', '24370189', '28333294', '29161377', '27634868', '24460444', '29335555', '27609659', '25445201', '28658154', '26230901', '26113975', '25857665', '27232328', '28632755', '28811633', '28681204', '28085486', '27484959', '25650398', '27503374', '27019455', '28571577', '25676470', '31092590', '25865368', '26424567', '28968382', '26908163', '26865050', '30367675', '27228122', '24516647', '28358811', '27716140', '29621980', '29114218', '27792658', '26712950', '28512451', '29335554', '28968799', '28234976', '29238752', '25394613', '26430856', '26756784', '27874095', '28695118', '30520450', '28578302', '24646696', '25818066', '24584251', '29367728', '29397054', '30356187', '23638162', '24650829', '29518088', '26367776', '27580384', '28219862', '25638481', '28738889', '27684425', '29018426', '27815091', '26919743', '23363771', '30273364', '27860030', '29899505', '30974084', '25482875', '28011577', '25969737', '29402950', '27070903', '31058230', '24645635', '28182762', '29658820', '25012772', '28669514', '27757389', '29106513', '24890442', '24493506', '25551282', '30386306', '27488896', '24373208', '27935837', '27930727', '27306058', '30219104', '29157044', '25893458', '27562571', '23401405', '29769279', '23836644', '28966891', '26418220', '26647391', '24927234', '27375059', '27275789', '25853698', '27994110', '29983746', '30374341', '29798707', '30863868', '23178636', '30210479', '25805726', '27639806', '30001516', '29654837', '26440540', '30532043', '29546248', '26492486', '29802752', '27822521', '26290147', '30197635', '27482891', '30308161', '27548430', '30261008', '28937904', '24886284', '26016947', '30201048', '25305287', '23437114', '25905625', '30356183', '25690330', '24169577', '25061514', '27064174', '23664249', '27726947', '31107866', '27178256', '26377332', '29452151', '28576147', '26113976', '30590681', '30716085', '30339827', '24650346', '30975860', '30808380', '29439731', '21926193', '30679677', '23173619', '29546315', '27392757', '29971046', '30341386', '25087692', '2262580', '30173825', '29311644', '25427878', '30678738', '23269735', '25636927', '29935107', '30212648', '27180018', '29412743', '29453431', '24101743', '24702028', '27904883', '22936250', '27124735', '24933584', '21402766', '25741698', '29385143', '27043715', '22718773', '29858345', '24990471', '24529606', '22450732', '30055351', '28326071', '30419026', '26690933', '30180210', '15747442', '27716162', '9526910', '28399458', '25987611', '23671411', '25426290', '25631494', '25229421', '25485279', '28129336', '25222658', '28591831', '29087793', '30374198', '24944063', '26826577', '30541439', '24529620', '21386800', '30001517', '23670220', '20660593', '31015324', '19735274', '30510769', '25818499', '22432018', '27316915', '20585638', '30559407', '10588495', '25421430', '26296061', '26843797', '26275230', '21143523', '26817524', '30638420', '31066111', '29101397', '22152152', '27834300', '27567042', '30794280', '31089259', '26300323', '26897029', '30624175', '26140631', '25575752', '30448776', '30268397', '26125141', '30986251', '23088889', '23784124', '25040299', '1452808', '29388674', '24451062', '27362724', '8951630', '25774293', '1298430', '24468033', '27223096', '26743465', '11737662', '22876171', '22157239', '22934839', '21976140', '24376552', '17117997', '30191183', '9495607', '12043869', '30658995', '21875444', '15472319', '20829292', '29657362', '26273264', '23163886', '9249637', '20709584', '28072766', '29543807', '9037669', '17604093', '22900048', '2921370', '30365516', '24906952', '27532502', '1843465', '19107676', '22527995', '27002447', '11952732', '20864478', '20507380', '24118234', '23124244', '23064750', '29621590', '30417096', '9442424', '2768541', '24007571', '16301151', '8814590', '15142209', '21477358', '2254836', '21067371', '22367084', '8510978', '12944020', '16027144', '3509967', '11686813', '21923689', '2148945', '18583327', '17760545', '21820714', '21515549', '22031833', '3164332', '20181292', '15766363', '24607562', '15016024', '22678395', '8739165', '8715357', '21214629']
##    with open('pmids_test.txt') as test_file:
##        pmids = [line.strip() for line in test_file.readlines()]
#    get_citations_from_pubmed(driver, pmids, file_format='nbib')
#    nbib_filenames = [os.path.join(temp.name, filename) for filename in os.listdir(temp.name) if filename.lower().endswith('.nbib')]
#    output_path = os.path.join(dl_path, 'citations.nbib')
#    concatenate_files(nbib_filenames, output_path)
#    for file in nbib_filenames:
#        os.remove(file)
#    move_files(temp.name, dl_path)
#    driver.quit()


if __name__ == '__main__':
#    samples = sample_prep_connector()
#    bib_parser_main()
#    count_parser_main()
#    prep_workflows, proc_workflows = prep_parser_main()
#    start = time.time()
    # c = new_sample_parser()
#    end = time.time()
#    print('new_sample_parser:', end-start)

    samples, subjects = subject_sample_parser()

# NEW TEST
#    exp_dir = r'.\data\test_data\experiments\101'
#    sample_file = os.path.join(exp_dir, '101_20171109-130044.txt')
#    prep_files = [os.path.join(exp_dir, '101_prep_237_qiime_20190428-053528.txt')]
#    proc_file = os.path.join(exp_dir, 'prep_data.json')
#    biom_filenames = ['44767_otu_table.biom',
#                      '44770_otu_table.biom',
#                      '44771_otu_table.biom']
#    biom_files = []
#    for biom_file in biom_filenames:
#        biom_files.append(os.path.join(exp_dir, biom_file))
#    with session_scope() as session:
#        a = time.time()
#        best_parser(session, sample_file, prep_files, proc_file, biom_files)
#        b = time.time()
#        print('total_time: ', b-a)

