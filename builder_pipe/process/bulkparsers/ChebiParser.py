from eme.pipe import Process
from metabolite_index import COMMON_ATTRIBUTES


from .utils.parsinglib import strip_attr, force_list, flatten, remap_keys, handle_quotes
from .utils.edb_specific import split_pubchem_ids, map_to_edb_format
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class ChebiParser(Process):
    consumes = dict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def produce(self, data: dict):
        _mapping = self.cfg.conf['attribute_mapping']
        important_attr = self.cfg.get('settings.chebi_attr_etc', cast=set)

        # strip molfile
        molfile = data.pop(None, None)

        remap_keys(data, _mapping)

        #force_list(data, 'chebi_id_alt')
        force_list(data, 'names')
        handle_quotes(data, 'names')

        split_pubchem_ids(data)

        # flatten_refs(data, )
        data, etc = map_to_edb_format(data, important_attr=important_attr, edb_format=None, exclude_etc={None})

        strip_attr(data, 'chebi_id', 'CHEBI:')
        #strip_attr(data, 'chebi_id_alt', 'CHEBI:')
        strip_attr(data, 'hmdb_id', 'HMDB')
        strip_attr(data, 'lipidmaps_id', 'LM')
        strip_attr(data, 'inchi', 'InChI=')

        #flatten(data, 'chebi_star')
        flatten(data, 'description')

        # force_flatten_extra_refs(data, SPEC_REF_ATTR)
        #
        # if 'chebi_id_alt' in data and data['chebi_id_alt']:
        #     force_list(data, 'chebi_id_alt')
        #     data['ref_etc']['chebi_id'] = data.pop('chebi_id_alt')

        if self.app.debug:
            _except = set(data.keys()) - COMMON_ATTRIBUTES - {'attr_mul', 'attr_other', "names"}
            assert not _except, "extra attributes found: "+str(_except)

        data['edb_source'] = 'chebi'

        yield MetaboliteExternal(**data)
