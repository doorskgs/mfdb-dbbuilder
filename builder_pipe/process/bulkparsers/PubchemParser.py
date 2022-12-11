from eme.pipe import Process
from mfdb_parsinglib.edb_formatting import preprocess, remap_keys, split_pubchem_ids, map_to_edb_format, MultiDict, \
    replace_esc, handle_name

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

        # find IUPAC name
        iupac_name = data.get('PUBCHEM_IUPAC_NAME', data.get('PUBCHEM_IUPAC_TRADITIONAL_NAME', data.get('PUBCHEM_IUPAC_SYSTEMATIC_NAME')))

        remap_keys(data, _mapping)
        data['pc_iupac_name'] = handle_name(iupac_name) if iupac_name is not None else None

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

        if self.generated % 1000 == 0:
            self.app.print_progress(self.generated)
        self.generated += 1

        yield MetaboliteExternal(edb_source='pubchem', **data)
