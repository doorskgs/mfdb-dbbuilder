from .padding import strip_prefixes
from .parsinglib import force_flatten, try_flatten, handle_names, flatten, handle_masses
from ..attributes import  EDB_SOURCES, EDB_ID_OTHER, COMMON_ATTRIBUTES
from .structs import MultiDict


EDB_IDS = set(map(lambda x:x+'_id', EDB_SOURCES))

def map_to_edb_format(me: dict, important_attr: set):
    """
    Parses EDB dictionary (from bulk dump files) to universal EDB format accepted by MetaboliteExternal

    :param me: data dict to be transformed
    :param important_attr: attributes that are important to be marked for this EDB
    :return: dict
    """
    out = {}
    attr_mul = MultiDict()
    attr_other = MultiDict()

    # these attributes are copied as-is, they're ok to be non-scalar
    out["names"] = me.pop("names", [])

    # force scalar and store redundant ids/attributes
    for _key in (EDB_IDS | COMMON_ATTRIBUTES | EDB_ID_OTHER) - {"names"}:
        if (val := me.pop(_key, None)) is not None:
            out[_key] = force_flatten(val, redundant_values := [])

            if redundant_values:
                attr_mul.extend(_key, redundant_values)

    for _key in important_attr:
        if (val := me.pop(_key, None)) is not None:
            # todo: should we best-effort flatten other attributes OR force flatten them?
            attr_other.extend(_key, try_flatten(val))

    out["attr_mul"] = dict(attr_mul)
    out["attr_other"] = dict(attr_other)

    return out, dict(me)


hmdb_id_formats = (
    # padded    - long / short
    (len('HMDB0000008'), len('HMDB00008')),
    # depadded  - long / short
    (len('0000008'), len('00008'))
)

def replace_obvious_hmdb_id(hmdb_id):
    if hmdb_id is None:
        return hmdb_id

    if hmdb_id.startswith('HMDB'):
        if len(hmdb_id) == hmdb_id_formats[0][1]:
            # keep prefix and pad with 00
            return hmdb_id[:4] + '00' + hmdb_id[4:]
        elif len(hmdb_id) != hmdb_id_formats[0][0]:
            raise Exception("Invalid HMDB ID format provided:" + str(hmdb_id))
    else:
        if len(hmdb_id) == hmdb_id_formats[1][1]:
            # pad with 00
            return '00' + hmdb_id
        elif len(hmdb_id) != hmdb_id_formats[1][0]:
            pass
            #raise Exception("Invalid HMDB ID format provided:" + str(hmdb_id))
    return hmdb_id


def preprocess(data: dict):
    """
    Executes general EDB parsing that are needed for all major DBs
    :param data:
    :return:
    """
    handle_names(data)

    strip_prefixes(data)

    flatten(data, 'description')

    handle_masses(data)

def flatten_hmdb_hierarchies2(r: MultiDict):
    # Secondary IDs
    secondary = r.pop('secondary_accessions', None)
    if secondary:
        r.extend('hmdb_id_alt', secondary['secondary_accessions.accession'])

    # Synonyms
    synonyms = r.pop('synonyms', None)
    if synonyms:
        r.extend('names', synonyms['synonyms.synonym'])

