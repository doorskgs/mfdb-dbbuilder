import math
import os

from pipebro import Process
from mfdb_parsinglib import EDB_SOURCES, EDB_SOURCES_OTHER
from metcore.parsinglib import preprocess, remap_keys, split_pubchem_ids, map_to_edb_format, MultiDict, force_list, mapping_path

from edb_builder.dtypes.MetaboliteExternal import MetaboliteExternal
from edb_builder.process.bulkparsers.utils import assert_edb_dict


class KeggParser(Process):
    CFG_PATH = os.path.join(mapping_path, 'kegg.ini')

    consumes = MultiDict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    def initialize(self):
        self.generated = 0

    async def produce(self, data: dict):
        _mapping = {attr: attr+'_id' for attr in EDB_SOURCES | EDB_SOURCES_OTHER} | self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.kegg_attr_etc', cast=set, default=set())

        # strip molfile
        #molfile = data.pop(None, None)

        remap_keys(data, _mapping)
        preprocess(data)
        data, etc = map_to_edb_format(data, important_attr=important_attr)

        if self.generated % 1000 == 0:
            self.app.print_progress(self.generated)
        self.generated += 1

        yield MetaboliteExternal(edb_source='kegg', **data), self.produces
