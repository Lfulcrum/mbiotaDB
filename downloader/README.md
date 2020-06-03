# Qiita Downloader - Documentation

## General information - What does it do?

Qiita downloader is a web scraping tool that can download data for studies returned by a search of http://qiita.ucsd.edu/.

## Requirements

- Python 3
- [Selenium](https://pypi.org/project/selenium/)
- Firefox
- Geckodriver ([Latest Release](https://github.com/mozilla/geckodriver/releases))

Details of the recommended method to install these components is outlined in the next section.

## Installation

### Via Anaconda
One way to install Python 3.X and the python package Selenium is to install Anaconda (a package and environment manager for Python). Download Anaconda 3.X [here](https://www.anaconda.com/distribution/).

Once installed, open the conda prompt (command-line tool for Anaconda). Create a new environment if you like (recommended):

```conda create -n my_env```

You may change the name of the environment `my_env` to something more relevant like `qiita_downloader`. Activate the newly created environment using:

```conda activate my_env```

Install Python and Selenium in this environment:

```conda install python selenium```

Note: Creation of the environment and installation of packages can be accomplished in one command using `conda create -n my_env python selenium`.

All that is left is to install Firefox (if you have not already done so) and to download the [latest Geckodriver](https://github.com/mozilla/geckodriver/releases) for your operating system architecture. Move the Geckodriver to the current working directory (where you intend to run the qiita_downloader.py script) or some other specifiable (and writable) path.

### Via PyPi

It is assumed that Python 3 and Firefox can be installed without further instructions here.

If you already have a Python 3.X version installed and do not wish to install the hefty (but useful) Anaconda package manager, you also install Selenium via `pip`. Before installing, you may wish to use `pipenv`s or `virtualenv`s ([read more](https://docs.python-guide.org/dev/virtualenvs/)). If pip is in a directory specified in your PATH environment variable, then you can install Python packages with pip from the command-line using:

```pip install selenium```

Note: As of Python 3.6, pip was included in the Python standard library, so you can install packages from a Python interpreter/IDE.

As with installing via Anaconda, Firefox must also be installed and the latest Geckodriver moved to the current working directory (or some other accessible path).

## Use

Assuming that all required software has been successfully installed and the Geckodriver is in an accessible path, you can save the qiita_downloader.py script in the current working directory, and run from the command-line:

```
python qiita_downloader.py --driver <GECKODRIVER_PATH> \
						   --config <CONFIG_FILE> \
						   --headless \
						   --search <SEARCH_TERM> \
						   --download <OPTION> <OPTION> ... \
						   --dir <OUTPUT_DIR> \
						   --citation-format <FORMAT>
```

#### Options:
**`--headless`**: A flag to specify that the browser should be invisible while the program is web scaping. Omit the option if you want to see how the scaper is navigating Qiita for debugging purposes.

`<GECKODRIVER_PATH>`: Path to the Geckodriver file (*default: '.'*).

`<CONFIG_FILE>`: Path to a configuration file, containing a section called 'qiita'. See below for format of the section in the config file:

```
[qiita]
username=<MY_QIITA_USERNAME>
password=<MY_QIITA_PASSWORD>
```

If the `--config` option is not specified, you will be prompted to type a Qiita username and password on the command-line.

`<SEARCH_TERM>`: The search term to search the Qiita database.

`<OPTION>`: One of `['all', 'biom', 'qiime', 'prep', 'sample', 'study']`. These options allow you to download different sets of data depending on your needs:
- `'all'` - Download all data (BIOM files, Qiime maps, 16S sample preparation data, sample metadata, search result study metadata).
- `'biom'` - Download a zip file containing BIOM files and Qiime maps.
- `'prep'` or `'qiime'` - Download 16S sample preparation data and Qiime maps.
- `'sample'` - Download sample metadata.
- `'study'` - Download metadata available from the search result page of the Qiita website.
- `'citation'` - Download a citation file for all publications that are associated (as Pubmed IDs) with Qiita search results. The format of the citation file must also be specified using the `--citation-format` option.

Note: 
More than one `<OPTION>` can be specified for the `--download` option.
If the `--download` option is not specified, the default download option `'study'` is used.

`<OUTPUT_DIR>`: The (clean) directory into which data from Qiita should be downloaded (*default: './output'*).

`<FORMAT>`: The citation file format to download from [Pubmed](https://www.ncbi.nlm.nih.gov/pubmed). This must be specified if the `'citation'` option is given for `--download`.

Remember to wrap the various string arguments for options in quotes ("" for Windows users, or '' for Unix users) to avoid expansions.

Downloaded files for each study will be moved into individual directories (labelled by Qiita study ID). If the `'study'` is given for the `--download` option, a `studies.csv` file is generated in the root of the specified `<OUTPUT_DIR>`.

## Report Bugs/Questions

Contact information: 

## Future development/Desired functionality

- Allow the program to work with more browsers(Chrome/IE/Edge/Safari).

## License
