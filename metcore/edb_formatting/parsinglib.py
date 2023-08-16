from itertools import chain

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



def flatten(v: dict, attr):
    if attr in v and isinstance(v[attr], (list, tuple, set)):
        v[attr] = try_flatten(v[attr])


def try_flatten(arr: list):
    """
    Flattens collections within dict if their lengths = 1
    :param v: dict
    :param attr: attribute to flatten
    :return:
    """

    if isinstance(arr, (list, tuple, set)) and len(arr) <= 1:
        # scalar
        return next(chain(arr), None)
    return arr



def force_flatten(arr, store_in: list):
    """
    Flattens collections regardless of size
    :param coll: collection to flatten
    :param store_in: extra container to store stripped attributes in
    :return:
    """
    if isinstance(arr, (list, tuple, set)):
        if len(arr) > 1:
            store_in.append(list(arr)[1:])
            return arr[0]

        return next(chain(arr), None)
    # scalar
    return arr

def force_list(coll):
    """
    Forces value to be a list of element 1
    """
    if isinstance(coll, (list, tuple, set)):
        return list(coll)
    return [coll]

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

            if isinstance(val, list):
                v[new_key].extend(val)
            else:
                v[new_key].append(val)

_REPLACE_CHARS = {
    # normalized quotations chr(8221) chr(96)
    ord('"'): '”',
    8243: '”', #″
    8221: '”', #”
    8217: "'", #’
    8242: "'", #′
    8216: "'", #‘
    96: "'", #`
    # normalized dash
    173: '-', # ­
    8211: '-',  # –
    8209: '-',  # ‑
    # special representation that are converted back frontend side
    #ord('\\'): '<ESC>',
    # manual input errors that are post-corrected (+tab, NL characters)
    160: ' ',  #  
    65279: ' ',  # ﻿
    8203: ' ', #​
    65533: ' ',  #�
    8201: ' ',  #
} | {i: ' ' for i in range(1, 32)}

def strip_esc_ad(txt):
    if '\\d' in txt:
        txt = txt.replace('\\d', 'd')
    if '\\a' in txt:
        txt = txt.replace('\\a', 'a')
    return txt

def handle_names(data: dict):
    if not isinstance(data['names'], (tuple, list, set)):
        data['names'] = [handle_name(data['names'])]
    else:
        data['names'] = list(set(handle_name(n) for n in data['names'] if n is not None))

def handle_name(name: str):
    return strip_esc_ad(name).translate(_REPLACE_CHARS)

def handle_masses(me: dict):
    try:
        me['mass'] = float(me['mass'])
    except:
        me['mass'] = None
    try:
        me['mi_mass'] = float(me['mi_mass'])
    except:
        me['mi_mass'] = None
    try:
        me['charge'] = float(me['charge'])
    except:
        me['charge'] = None


def replace_esc(s: str):
    if '\\' in s:
        return s.replace('\\', '<ESC>')
    return s
