from metabolite_index import EDB_SOURCES, EDB_ID_OTHER, COMMON_ATTRIBUTES

from .parsinglib import force_flatten, flatten


EDB_IDS = set(map(lambda x:x+'_id', EDB_SOURCES))

def map_to_edb_format(me: dict, important_attr: set, edb_format, exclude_etc: set):
    """
    Parses EDB dictionary (from bulk dump files) to universal EDB format accepted by MetaboliteExternal

    :param me: data dict to be transformed
    :param important_attr: attributes that are important to be marked for this EDB
    :param edb_format:
    :return: dict
    """
    out = {}
    attr_mul = {}
    attr_other = {}

    # these attributes are copied as-is, they're ok to be non-scalar
    out["names"] = me.pop("names", [])

    # force scalar and store redundant ids/attributes
    for _key in EDB_IDS | COMMON_ATTRIBUTES | EDB_ID_OTHER:
        if val := me.pop(_key, None):
            out[_key] = force_flatten(val, redundant_values := [])

            if redundant_values:
                attr_mul[_key] = redundant_values

    for _key in important_attr:
        if val := me.pop(_key, None):
            # todo: should we best-effort flatten other attributes OR force flatten them?
            attr_other[_key] = force_flatten(val, redundant_values := [])

            if redundant_values:
                attr_mul[_key] = redundant_values

    out["attr_mul"] = attr_mul
    out["attr_other"] = attr_other

    return out, dict(me)


def split_pubchem_ids(r):
    if 'pubchem_id' in r:
        p = r['pubchem_id']
        if isinstance(p, list):
            p = list(map(lambda p: p[5:], filter(lambda p: p.startswith('CID:'), p)))

            if len(p) > 1:
                pass
            elif len(p) == 0:
                del r['pubchem_id']
            else:
                # after filtering pubchem_id becomes scalar:
                p = p[0]

        r['pubchem_id'] = p

def flatten_hmdb_hierarchies(r):
    if 'synonyms' in r:
        synsyn = r.pop('synonyms')

        if synsyn and synsyn[0]:
            r['names'].extend(synsyn[0]['synonym'])
    elif 'names' in r:
        for i,syn in enumerate(r['names']):
            if isinstance(syn, dict):
                r['names'][i] = syn['synonym']

    if 'secondary_accessions' in r:
        accacc = r.pop('secondary_accessions')

        if accacc and accacc[0]:
            r['hmdb_id_alt'].extend(accacc[0]['accession'])
    elif 'hmdb_id_alt' in r:
        for i, syn in enumerate(r['hmdb_id_alt']):
            if isinstance(syn, dict):
                r['hmdb_id_alt'][i] = syn['accession']
