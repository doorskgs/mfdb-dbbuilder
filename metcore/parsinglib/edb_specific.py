from .padding import strip_prefixes
from .parsinglib import force_flatten, try_flatten, handle_names, flatten, handle_masses
from .structs import MultiDict

from edb_handlers import EDB_SOURCES, EDB_ID_OTHER, COMMON_ATTRIBUTES

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
