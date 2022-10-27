from eme.pipe import Process

from .utils.parsinglib import strip_attr, force_list, flatten
from .utils.edb_specific import split_pubchem_ids, map_to_edb_format, EDB_ATTR, flatten_hmdb_hierarchies
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class LipidMapsParser(Process):
    consumes = dict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    async def produce(self, data: dict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.hmdb_attr_etc', cast=set)

        for k in list(data):
            new_k = _mapping[k] if k in _mapping else k.lower()
            data[new_k] = data.pop(k)

        # flattens lists of len=1
        data, etc = map_to_edb_format(data, important_attr=important_attr, edb_format=None)

        strip_attr(data, 'chebi_id', 'CHEBI:')
        strip_attr(data, 'chebi_id_alt', 'CHEBI:')
        strip_attr(data, 'hmdb_id', 'HMDB')
        strip_attr(data, 'lipidmaps_id', 'LM')
        strip_attr(data, 'inchi', 'InChI=')

        force_list(data, 'chebi_id_alt')
        force_list(data, 'names')

        split_pubchem_ids(data)

        if self.app.debug:
            _except = set(data.keys()) - EDB_ATTR - {'attr_etc', 'attr_mul', 'edb_id_etc', 'attr_other', "names"}
            assert not _except, "extra attributes found: "+str(_except)

        data['edb_source'] = 'lipmaps'
        yield MetaboliteExternal(**data)

