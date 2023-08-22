from .edb_formatting import depad_id
from .views.MetaboliteConsistent import MetaboliteConsistent
from .views.MetaboliteDiscovery import MetaboliteDiscovery



def get_mdb_id(mdb: dict | MetaboliteDiscovery | MetaboliteConsistent, level=3):
    """
    Gets MDB database ID from metabolite discovery run result

    :param mdb: metabolite dicovery object or dict representation
    :param level: required level to dive into. First inchikey is tried, if it's not found, then chebi ID is tried (lvl2), and so on until 4 levels

    :return: MDB ID
    """

    mids_candidates = []

    if isinstance(mdb, dict):
        mids_candidates.append((mdb['inchikey'], 'inchikey'))
        mids_candidates.append((mdb['chebi_id'], 'chebi_id'))
        mids_candidates.append((mdb['hmdb_id'], 'hmdb_id'))
        mids_candidates.append((mdb['pubchem_id'], 'pubchem_id'))
    else:
        mids_candidates.append((mdb.inchikey, 'inchikey'))
        mids_candidates.append((mdb.chebi_id, 'chebi_id'))
        mids_candidates.append((mdb.hmdb_id, 'hmdb_id'))
        mids_candidates.append((mdb.pubchem_id, 'pubchem_id'))

    for i in range(level):
        mids, attr = mids_candidates[i]
        if not isinstance(mids, (set, list, tuple)):
            mids = [mids]

        if mids:
            if i > 0: #inchikey is already non-digits
                mids = map(lambda x: get_digi_chars(depad_id(x, attr)), mids)

            yield from mids
            break


def get_digi_chars(text):
    return ''.join(chr(65+int(c)) for c in text)
