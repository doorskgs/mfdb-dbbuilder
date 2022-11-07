from eme.pipe import Process
from metabolite_index import COMMON_ATTRIBUTES

from metabolite_index.edb_formatting import preprocess, remap_keys, flatten_hmdb_hierarchies2, map_to_edb_format, \
    MultiDict

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.utils import assert_edb_dict


class HMDBParser(Process):
    consumes = MultiDict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    async def produce(self, data: MultiDict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.hmdb_attr_etc', cast=set)
        flatten_hmdb_children = self.cfg.get('settings.flatten_hmdb_children', cast=bool)

        remap_keys(data, _mapping)

        # flattens multi-level HMDB specific XML attributes into a list
        if flatten_hmdb_children:
            flatten_hmdb_hierarchies2(data)

        preprocess(data)

        # flattens lists of len=1
        data, etc = map_to_edb_format(data, important_attr=important_attr)

        #remove_obvious_secondary_ids(data)
        # split_pubchem_ids(data)

        if self.app.debug:
            assert_edb_dict(data)

        data['edb_source'] = 'hmdb'
        yield MetaboliteExternal(**data)

