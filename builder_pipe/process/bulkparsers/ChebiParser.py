import math

from eme.pipe import Process
from mfdb_parsinglib import COMMON_ATTRIBUTES
from mfdb_parsinglib.edb_formatting import preprocess, remap_keys, split_pubchem_ids, map_to_edb_format, MultiDict
from mfdb_parsinglib.edb_formatting.parsinglib import force_list, handle_name, force_flatten

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.dtypes.SecondaryID import SecondaryID
from builder_pipe.process.bulkparsers.utils import assert_edb_dict


class ChebiParser(Process):
    consumes = MultiDict, "edb_obj"
    produces = (
        (MetaboliteExternal, "edb_record"),
        (SecondaryID, "2ndid")
    )

    def initialize(self):
        self.generated = 0

    async def produce(self, data: dict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.chebi_attr_etc', cast=set)

        # strip molfile
        molfile = data.pop(None, None)
        iupac_names = force_list(data.get('IUPAC Names'))

        remap_keys(data, _mapping)
        data['ch_iupac_name'] = [handle_name(name) for name in iupac_names if name is not None]

        preprocess(data)
        sids = split_pubchem_ids(data)

        data, etc = map_to_edb_format(data, important_attr=important_attr)

        if self.app.debug:
            assert_edb_dict(data)

        if 'mass' in data and math.isnan(data['mass']):
            data['mass'] = None
        if 'mi_mass' in data and math.isnan(data['mi_mass']):
            data['mi_mass'] = None
        if 'charge' in data and math.isnan(data['charge']):
            data['charge'] = None

        if 'chebi_id_alt' in etc and etc['chebi_id_alt']:
            id2nd = list(map(lambda x: x.removeprefix("CHEBI:"), force_list(etc['chebi_id_alt'])))

            yield SecondaryID(edb_id=data['chebi_id'], secondary_ids=id2nd, edb_source='chebi'), self.produces[1]

        if self.generated % 1000 == 0:
            self.app.print_progress(self.generated)
        self.generated += 1

        yield MetaboliteExternal(edb_source='chebi', **data), self.produces[0]
