from decimal import Decimal

from mfdb_parsinglib.edb_formatting import try_flatten, depad_id
from mfdb_parsinglib import EDB_ID, EDB_ID_OTHER, COMMON_ATTRIBUTES, get_mdb_id

from import_local_ddb.utils import iter_sol


# what attributes to build DDB items and index from
MULABLE_ATTR = (EDB_ID | EDB_ID_OTHER | {'smiles', 'inchikey'}) - {'swisslipids_id'}
RANGE_ATTR = {'mass', 'mi_mass', 'charge', 'logp'}
OTHER_ATTR = {'inchi', "names", "formula", "pname"}
ATTR_OTHER_MAPPING = { "logp", "chebi_star", "state" }

attr_prefix = {
    'pubchem_id': 'P', 'chebi_id': 'C', 'hmdb_id': 'H', 'kegg_id': 'K', 'lipmaps_id': 'L',
    'inchikey': 'I', 'inchi': 'Iv', 'smiles': 'S', 'pname': 'N',
    'cas_id': 'A', 'chemspider_id': 'X', 'metlin_id': 'M', 'swisslipids_id': 'Sw'
}
# safe check if there are no duplicate prefix keys
assert len(set(attr_prefix.values())) == len(attr_prefix.values())
assert not (MULABLE_ATTR - set(attr_prefix.keys()))


def build_mdb_record(mdb: dict, mid = None):
    """
    Creates DynamoDB items and indexes
    :param mdb: metabolite discovery result
    :param mdb_id:
    :param omit_gsi: if True, swapped partkey/sortkey will be generated for search items
    :return:
    """
    mdb_item = {
        #'srt': 'mdb',
        'pname': get_primary_name(mdb),
    }

    if mid is not None:
        mdb_item['mid'] = mid

    # copy and flatten edb_ids and attributes if possible
    for k in MULABLE_ATTR:
        val = try_flatten(list(map(lambda x: depad_id(x, k), iter_sol(mdb, k))))
        if val:
            mdb_item[k] = val

    for k in OTHER_ATTR:
        val = mdb.get(k)
        if val:
            mdb_item[k] = try_flatten(val)

    # change float attributes to str
    for k in RANGE_ATTR:
        val = try_flatten(list(map(str, iter_sol(mdb, k))))
        if val:
            mdb_item[k] = val

    # copy other (edb specific) attributes
    for k in ATTR_OTHER_MAPPING:
        val = mdb['attr_other'].get(k)
        if val:
            mdb_item[k] = try_flatten(val)

    return mdb_item


def build_search_keys(mdb_item, omit_gsi=False):
    # create search keys except for inchi (if inchi is present, it's treated as MID)
    for k in (MULABLE_ATTR - {"inchikey"}):
        mdb_key, atr_key = ('mid', 'mrf') if not omit_gsi else ('mrf', 'mid')

        for val in iter_sol(mdb_item, k):
            yield {
                mdb_key: mdb_item['mid'],
                atr_key: f'{attr_prefix[k]}:{val}'
            }


def build_mid_search_key(referenced_mid: list | str, search_attributes: list):
    for mid_2nd in search_attributes:
        yield {
            'mid': mid_2nd,
            'mrf': referenced_mid
        }


def get_primary_name(mdb: dict, sort_by_length=False):
    # get primary name, prio: pubchem > chebi > shortest name
    if sort_by_length:
        names = sorted(mdb['names'], key=lambda x: len(x))
    else:
        names = mdb['names']

    name = mdb['attr_other'].get("pc_iupac_name", mdb['attr_other'].get("ch_iupac_name"))
    if name is None:
        return names[0] if len(names) > 0 else "Unnamed compound"
    return name
