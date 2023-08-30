import os
from enum import Enum


EDB_SOURCES = list(map(lambda x: x[4:], filter(lambda x: x.startswith('edb_'), os.listdir(os.path.dirname(__file__)))))


class EDBSource(Enum):
    # some example types but can be expanded
    pubchem = 'pubchem'
    chebi = 'chebi'
    hmdb = 'hmdb'
    kegg = 'kegg'
    lipmaps = 'lipmaps'


#EDBSource = Enum("EDBSource", {x: x for x in EDB_SOURCES})
EDB_SOURCES_OTHER = { 'cas', 'chemspider', 'metlin', 'swisslipids' }

"""
List of EDB_IDs that are not yet supported by MFDB, but are well known
Also includes non-metabolite refs, like protein DBs
"""
EDB_ID_OTHER = set(map(lambda x: x+'_id', iter(EDB_SOURCES_OTHER)))

EDB_ID = set(map(lambda x: x+'_id', iter(EDB_SOURCES)))


# 'chembl_id',
#   'metabolights_id',
# 'chebi_id_alt', 'hmdb_id_alt', 'pubchem_sub_id',
# #'pdb_id', 'uniprot_id',
# 'drugbank_id', 'kegg_drug_id',
# 'wiki_id',
# 'pubmed_id',

__all__ = ['EDB_SOURCES', 'EDBSource']

