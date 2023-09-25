import os
from enum import Enum


EDB_SOURCES = set(map(lambda x: x[4:], filter(lambda x: x.startswith('edb_'), os.listdir(os.path.dirname(__file__)))))


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

# todo: make support for these EDBs:
# 'chembl_id',
# 'metabolights_id',
# 'chebi_id_alt', 'hmdb_id_alt', 'pubchem_sub_id',
# #'pdb_id', 'uniprot_id',
# 'drugbank_id', 'kegg_drug_id',
# 'wiki_id',
# 'pubmed_id',

COMMON_ATTRIBUTES = {
    "names", "description",
    'formula', 'inchi', 'inchikey', 'smiles',
    'mass', 'mi_mass', "charge"
}


def is_edb(reftag: str | tuple[str, str]):
    if isinstance(reftag, tuple) and len(reftag)==2:
        reftag = reftag[0]

    reftag = reftag.lower().removesuffix('_id')

    return reftag in EDB_SOURCES


__all__ = ['EDB_SOURCES', 'EDBSource', 'is_edb']
