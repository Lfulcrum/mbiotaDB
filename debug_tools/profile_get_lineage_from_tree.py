# -*- coding: utf-8 -*-
"""
Created on Mon May 18 13:47:00 2020

@author: William
"""

# Standard library imports
import re
import time
from collections import OrderedDict

# Third-party imports
import biom
from Bio import Phylo


taxon_name_re = re.compile(r'([\d.]+:)?'  # branch length
                           r'(?P<name>(?P<prefix>\w__)\[?[\w\d_-]*\]?)')


def get_tree(file, tree_format='newick'):
    tree = Phylo.read(file, 'newick')
    return tree


def generate_asv_clades(tree):
    # Assume sequence must contain at least one of A,T,G,C.
    # This seems to be strict enough for Qiita trees, but it may not
    # work for other trees (depends on clade name format!)
    # Assumes clade name is not None, but could be in some files - check!
    # Otherwise, we can add an extra 'if clade.name' before 'any(...)'
    return (clade for clade in tree.get_terminals() if
            any(x in clade.name for x in ('T','A','G','C')))

# Note: This function doesn't do much more than the tree.find_clades()
# But it is slightly faster (see profiling results in main()).
def generate_asv_clades_general(tree, terminal=True, contains=[], pattern=None):
    if terminal:
        if contains:
            return (clade for clade in tree.get_terminals() if
                    clade.name and
                    any(x in clade.name for x in contains))
        elif pattern:
            pattern = re.compile(pattern)
            return (clade for clade in tree.get_terminals() if
                    clade.name and
                    pattern.match(clade.name))
        else:
            return (clade for clade in tree.get_terminals())
    else:
        if contains:
            return (clade for clade in tree.find_clades() if
                    clade.name and
                    any(x in clade.name for x in contains))
        elif pattern:
            return (clade for clade in tree.find_clades(name=pattern))
        else:
            return (clade for clade in tree.find_clades())

def generate_asv_clades_short(tree, terminal=True, contains=[], pattern=None):
    if terminal:
        if contains:
            return (clade for clade in tree.get_terminals() if
                    clade.name and
                    any(x in clade.name for x in contains))
        elif pattern:
            return tree.find_clades(terminal=True, name=pattern)
        else:
            return tree.find_clades(terminal=True)
    else:
        if contains:
            return (clade for clade in tree.find_clades() if
                    clade.name and
                    any(x in clade.name for x in contains))
        elif pattern:
            return tree.find_clades(name=pattern)
        else:
            return tree.find_clades()

def find_clades_by_name(tree, terminal=True, contains=[], pattern=None):
    """Faster implementation than BioPython's Phylo package find_clades
    search functionality.
    """
    if pattern:
        pattern = re.compile(pattern)
    if terminal:
        if contains:
            return (clade for clade in tree.find_clades(terminal=True) if
                    clade.name and
                    any(x in clade.name for x in contains))
        elif pattern:
            return (clade for clade in tree.find_clades(terminal=True) if
                    clade.name and
                    pattern.match(clade.name))
        else:
            return (clade for clade in tree.find_clades(terminal=True))
    else:
        if contains:
            return (clade for clade in tree.find_clades() if
                    clade.name and
                    any(x in clade.name for x in contains))
        elif pattern:
            return (clade for clade in tree.find_clades() if
                    clade.name and
                    pattern.match(clade.name))
        else:
            return (clade for clade in tree.find_clades())

# LATEST IMPLEMENTATION
def all_parents(tree):
    parents = {}
    for clade in tree.find_clades(order='level'):
        for child in clade:
            parents[child] = clade
    return parents


def get_path(seq, parents, asv_clades):
    path = []
    if seq not in asv_clades:
        return None
    clade = asv_clades[seq]
    while True:
        try:
            path.append(clade)
            clade = parents[clade]
        except KeyError:
            break
    return path


def get_lineage_from_path(path):
    lineage = []
    if not path:
        return None
    for clade in path:
        if not clade.name:
            continue
        matches = taxon_name_re.finditer(clade.name)
        for match in matches:
            prefix = match.group('prefix')
            taxon = match.group('name')
            lineage.insert(0, taxon)
    return lineage


# INTERMEDIATE IMPLEMENTATION
def get_lineage_from_tree_v2(tree, seq, asv_clades):
    # clade = asv_clades[seq]
    if seq not in asv_clades:
        return None
    path = tree.get_path(asv_clades[seq])
    taxa = []
    for node in path:
        if not node.name:
            continue
        matches = taxon_name_re.finditer(node.name)
        for match in matches:
            try:
                prefix = match.group('prefix')
                taxon = match.group('name')
                taxa.append(taxon)
            except AttributeError:
                raise
    return taxa


# OLD IMPLEMENTATION!
def get_tree_taxa(tree):
    taxa = tree.find_elements(name=r'.*__.*')
    taxa = list(taxa)
    return taxa


def get_lineage_from_tree_v1(obs_id, tree, taxa):
    taxa_prefix = ['k__', 'p__', 'c__', 'o__', 'f__', 'g__', 's__']
    lineage = OrderedDict(zip(taxa_prefix, taxa_prefix))
    try:
        seq_var = asv_clades[obs_id]
    except KeyError:
        return None
    for taxon in taxa:
        if taxon.is_parent_of(seq_var):
            taxon_name_matches = taxon_name_re.finditer(taxon.name)
            for taxon_name_match in taxon_name_matches:
                try:
                    taxon_name = taxon_name_match.group('name')
                except AttributeError:
                    raise Exception('Encountered unrecognizable taxon name.')
                key = taxon_name[:3]
                lineage[key] = taxon_name
    return list(lineage.values())


if __name__ == '__main__':
    tree_file = r'../data/test_data/experiments/101/56522_insertion_tree.relabelled.tre'
    biom_file = r'../data/test_data/experiments/101/56522_reference-hit.biom'
    start = time.time()
    tree = get_tree(tree_file)
    end = time.time()
    print('Took {}s to parse tree.'.format(end-start))

    start = time.time()
    asv_clades = {clade.name: clade for clade in generate_asv_clades(tree)}
    end = time.time()
    print('Took {}s to get all ASV clades.'.format(end-start))

    print('General ASV finding function:')
    start = time.time()
    x = find_clades_by_name(tree, terminal=True, contains=['A', 'G', 'C', 'T'])
    list(x)
    end = time.time()
    print(f'Terminal, contains: {end-start}s')
    start = time.time()
    x = find_clades_by_name(tree, terminal=True, pattern=r'[ATGC]+')
    list(x)
    end = time.time()
    print(f'Terminal, pattern: {end-start}s')
    start = time.time()
    x = find_clades_by_name(tree, terminal=False, pattern=r'[ATGC]+')
    list(x)
    end = time.time()
    print(f'All, pattern: {end-start}s')
    start = time.time()
    x = find_clades_by_name(tree, terminal=False, contains=['A', 'G', 'C', 'T'])
    list(x)
    end = time.time()
    print(f'All, contains: {end-start}s')

    # "General" ASV finding function:
    # Terminal, contains: 7.297555923461914s
    # Terminal, pattern: 7.593747138977051s
    # All, pattern: 11.859349012374878s
    # All, contains: 9.42186164855957s

    # "Short" ASV finding function:
    # Terminal, contains: 9.028812408447266s
    # Terminal, pattern: 12.33241057395935s
    # All, pattern: 18.127376317977905s
    # All, contains: 9.91855788230896s

    # find_clades_by_name function:
    # Terminal, contains: 7.866394519805908s
    # Terminal, pattern: 8.020978689193726s
    # All, pattern: 9.910073518753052s
    # All, contains: 9.723053693771362s

    seqs = asv_clades

    # Profile lineage getter functions:
    # # This function is slow enough to not bother profiling...
    # # OLD IMPLEMENTATION:
    # lineages = []
    # start = time.time()
    # taxa = get_tree_taxa(tree)
    # mid = time.time()
    # for seq in seqs:
    #     lineages.append(get_lineage_from_tree_v1(seq, tree, taxa))
    # end = time.time()
    # print('OLD IMPLEMENTATION:')
    # print(f'Took {mid-start}s to get all taxa from tree (get_tree_taxa).')
    # print(f'Took {end-mid}s to get all lineages (get_lineage_from_tree).')
    # # - By first finding all tree taxa and then asking
    # # whether the taxon is a parent (as per original
    # # implementation): 1044.30s (1032.17s was spent in
    # # the get_lineage_from_tree_v1 function, while the
    # # remaining 12.14 seconds was sufficient to get
    # # all taxa-like nodes with get_tree_taxa).

    # # INTERMEDIATE IMPLEMENTATION:
    # lineages = []
    # start = time.time()
    # for seq in seqs:
    #     lineages.append(get_lineage_from_tree_v2(tree, seq, asv_clades))
    # end = time.time()
    # print('INTERMEDIATE IMPLEMENTATION:')
    # print(f'Took {end-start}s to get all lineages (get_lineage_from_tree).')
    # # - Without asv_clades dictionary lookup, this took 205.45s
    # # - With asv_clades dictionary lookup this took 147.98s

    # LATEST IMPLEMENTATION:
    lineages = []
    start = time.time()
    parents = all_parents(tree)
    mid = time.time()
    for seq in seqs:
        path = get_path(seq, parents, asv_clades)
        lineages.append(get_lineage_from_path(path))
    end = time.time()
    print('LATEST IMPLEMENTATION:')
    print(f'Took {end-start}s to get all lineages from tree.')
    print(f'Took {mid-start}s to get all parents dict from tree.')
    print(f'Took {end-mid}s to get all paths and lineages from paths.')
    # - Best implementation: 5.77s! This used a
    # recommendation in the BioPython cookbook to first
    # get all parents of all nodes in tree and use a
    # dictionary lookup to determine the path.
