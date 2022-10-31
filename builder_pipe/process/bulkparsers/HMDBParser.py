from eme.pipe import Process

from .utils.parsinglib import strip_attr, force_list, flatten, remap_keys
from .utils.edb_specific import split_pubchem_ids, map_to_edb_format, EDB_ATTR, flatten_hmdb_hierarchies
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class HMDBParser(Process):
    consumes = dict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    async def produce(self, data: dict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.hmdb_attr_etc', cast=set)

        remap_keys(data, _mapping)

        # flattens multi-level HMDB specific XML attributes into a list
        flatten_hmdb_hierarchies(data)

        # flattens lists of len=1
        data, etc = map_to_edb_format(data, important_attr=important_attr, edb_format=None)

        strip_attr(data, 'chebi_id', 'CHEBI:')
        strip_attr(data, 'hmdb_id', 'HMDB')
        strip_attr(data, 'lipidmaps_id', 'LM')
        strip_attr(data, 'inchi', 'InChI=')

        force_list(data, 'names')
        #remove_obvious_secondary_ids(data)

        # split_pubchem_ids(data)

        # flattens everything else that wasn't flattened before
        # (len>1 lists are processed into extra refs JSON attribute)
        #force_flatten_extra_refs(data)

        if 'hmdb_id_alt' in data and data['hmdb_id_alt']:
            force_list(data, 'hmdb_id_alt')
            data['ref_etc']['hmdb_id'] = data.pop('hmdb_id_alt')

        if self.app.debug:
            _except = set(data.keys()) - EDB_ATTR - {'attr_etc', 'attr_mul', 'edb_id_etc', 'attr_other', "names"}
            assert not _except, "extra attributes found: "+str(_except)

        data['edb_source'] = 'hmdb'
        yield MetaboliteExternal(**data)

