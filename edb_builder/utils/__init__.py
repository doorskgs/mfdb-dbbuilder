import os.path
from .ding import dingdingding
from .verify_edb_dict import assert_edb_dict

PIPECFG_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'config'))

__all__ = [
    'PIPECFG_PATH',
    'assert_edb_dict',
    'dingdingding'
]
