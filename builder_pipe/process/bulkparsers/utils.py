from metabolite_index.edb_formatting import MultiDict
from metabolite_index import COMMON_ATTRIBUTES, EDB_SOURCES
from metabolite_index.attributes import EDB_SOURCES_OTHER


def assert_edb_dict(data: MultiDict | dict):
    _ids = set(map(lambda x: x + '_id', EDB_SOURCES | EDB_SOURCES_OTHER))
    _except = set(data.keys()) - COMMON_ATTRIBUTES - _ids - {'attr_mul', 'attr_other', "names"}
    assert not _except, "extra attributes found: " + str(_except)
