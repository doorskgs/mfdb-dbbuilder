from eme.pipe import Process

from mfdb_parsinglib.edb_formatting import preprocess, remap_keys, map_to_edb_format, split_pubchem_ids, MultiDict

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.utils import assert_edb_dict


class LipidMapsParser(Process):
    consumes = MultiDict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    def initialize(self):
        self.generated = 0

    async def produce(self, data: MultiDict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.lipidmaps_attr_etc', cast=set)

        remap_keys(data, _mapping)

        # convert to lower keys
        for k in list(data.keys()):
            if k and k[1].isupper():
                data[k.lower()] = data.pop(k, None)

        # flattens lists of len=1
        data, etc = map_to_edb_format(data, important_attr=important_attr)
        if 'null' in data:
            data.drop('null', None)

        preprocess(data)

        sids = split_pubchem_ids(data)

        if self.app.debug:
            assert_edb_dict(data)

        self.generated += 1
        if self.generated % 1000 == 0:
            self.app.print_progress(self.generated)

        yield MetaboliteExternal(edb_source='lipmaps', **data)
