


# todo: move edp mapping here (idk where else could it be)
#       + move mapping config?
#       + try to automatize as much as possible using db_handlers/__init__.py

"""
Common non-ID attributes shared by all major EDBs
"""

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
