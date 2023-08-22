from edb_handlers import EDB_SOURCES, EDB_SOURCES_OTHER
from metcore.mapping import COMMON_ATTRIBUTES
from metcore.parsinglib import MultiDict


def assert_edb_dict(data: MultiDict | dict):
    _ids = set(map(lambda x: x + '_id', EDB_SOURCES | EDB_SOURCES_OTHER))
    _except = set(data.keys()) - COMMON_ATTRIBUTES - _ids - {'attr_mul', 'attr_other', "names"}
    assert not _except, "extra attributes found: " + str(_except)
