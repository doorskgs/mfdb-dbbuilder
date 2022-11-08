from eme.pipe import Process
from metabolite_index.edb_formatting import preprocess, remap_keys, split_pubchem_ids, map_to_edb_format, MultiDict, \
    replace_esc

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.utils import assert_edb_dict

class PubchemParser(Process):
    consumes = MultiDict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    def initialize(self):
        self.generated = 0

    async def produce(self, data: MultiDict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.pubchem_attr_etc', cast=set)

        # strip molfile
        molfile = data.pop(None, None)

        remap_keys(data, _mapping)

        # some records lack names
        if 'names' not in data:
            data['names'] = []
        # replace \ character in smiles
        if isinstance(data['smiles'], list):
            data['smiles'] = list(map(replace_esc, data['smiles']))
        else:
            data['smiles'] = replace_esc(data['smiles'])

        preprocess(data)
        #sids = split_pubchem_ids(data)

        data, etc = map_to_edb_format(data, important_attr=important_attr)

        if self.app.debug:
            assert_edb_dict(data)

        self.generated += 1
        self.app.print_progress(self.generated)

        yield MetaboliteExternal(edb_source='pubchem', **data)
