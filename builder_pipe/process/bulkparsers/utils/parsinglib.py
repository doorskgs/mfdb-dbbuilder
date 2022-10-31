

def rlen(v):
    if isinstance(v, list):
        return len(v)
    elif v is None:
        return 0
    else:
        return 1


def _nil(var):
    if not bool(var):
        return True
    # accounts for xml newlines, whitespace & etc
    s = var.strip().replace('\r', '').replace('\n', '')
    return not bool(s)


def strip_attr(r, key, prefix):
    if key not in r or not r[key]:
        return

    if isinstance(r[key], list):
        r[key] = list(map(lambda v: v.lstrip(prefix), r[key]))
    else:
        r[key] = r[key].lstrip(prefix)


def flatten(v: dict, attr=None):
    """
    Flattens collections within dict if their lengths = 1
    :param v: dict
    :param attr: attribute to flatten
    :return:
    """
    if attr is not None:
        if isinstance(v,dict) and attr in v:
            v[attr] = flatten(v[attr])
        return

    if isinstance(v, (list, tuple, set)) and len(v) == 1:
        return list(v)[0]
    else:
        return v

def force_flatten(coll, store_in: list):
    """
    Flattens collections regardless of size
    :param coll: collection to flatten
    :param store_in: extra container to store stripped attributes in
    :return:
    """
    if isinstance(coll, (list, tuple, set)):
        if len(coll) > 1:
            store_in.append(list(coll)[1:])
        elif not coll:
            return None

        return list(coll)[0]

    else:
        return coll


def force_list(r, key, f=None):
    """
    Forces value to be a list of element 1
    """
    if key not in r:
        return

    v = r[key]

    if isinstance(v, (list, tuple, set)):
        if f is not None:
            r[key] = [f(e) for e in v]
        else:
            r[key] = v
    elif v is None:
        r[key] = None
    else:
        if f is not None:
            r[key] = [f(v)]
        else:
            r[key] = [v]

def remap_keys(v, _mapping: dict):
    for k in set(v) & set(_mapping):
        new_key = _mapping[k]
        val = v.pop(k)

        if new_key not in v:
            # best effort to keep things scalar
            v[new_key] = val
        else:
            # if multiple values found, extend it to be a list
            if not isinstance(v[new_key], list):
                v[new_key] = [v[new_key]]
            else:
                v[new_key].append(val)

"""

def force_flatten_extra_refs(r, _SPEC_REF_ATTR: set, ex_attr=None, _except: tuple=None):
    Processes extra references (beyond the 1st value for scalars)
    and adds them to a json dictionary
    => thus keeping ref attributes flat scalar

    :param r:
    :param ex_attr:
    :return:

    if ex_attr is None:
        ex_attr = 'ref_etc'
    if _except is None:
        _except = set()

    ref = r.setdefault(ex_attr, {})

    for attr in _SPEC_REF_ATTR:
        if attr not in r or (attr in _except):
            continue

        val = r[attr]

        if isinstance(val, (list, tuple, set)):
            if len(val) > 1:
                val = list(val)
                # forces scalar
                r[attr] = val[0]
                ref.setdefault(attr, [])
                ref[attr].extend(val[1:])

            else:
                # forces scalar anyway:
                r[attr] = flatten(r[attr])


def flatten_refs(r, _SPEC_REF_ATTR: set):
    Flattens all ref attributes that have a length of 1

    for attr in _SPEC_REF_ATTR:
        if attr not in r:
            continue
        r[attr] = flatten(r[attr])

"""