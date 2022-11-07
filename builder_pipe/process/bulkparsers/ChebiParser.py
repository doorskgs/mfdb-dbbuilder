from eme.pipe import Process
from metabolite_index import COMMON_ATTRIBUTES
from metabolite_index.edb_formatting import preprocess, remap_keys, split_pubchem_ids, map_to_edb_format, MultiDict

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.utils import assert_edb_dict


class ChebiParser(Process):
    consumes = MultiDict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    async def produce(self, data: dict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.chebi_attr_etc', cast=set)

        # strip molfile
        molfile = data.pop(None, None)

        remap_keys(data, _mapping)

        preprocess(data)
        sids = split_pubchem_ids(data)

        data, etc = map_to_edb_format(data, important_attr=important_attr, edb_format=None, exclude_etc={None})

        if self.app.debug:
            assert_edb_dict(data)

        data['edb_source'] = 'chebi'
        yield MetaboliteExternal(**data)
