from eme.pipe.elems.AbstractData import AbstractData

from core.views.MetaboliteConsistent import MetaboliteConsistent


class MetaboliteExternal(MetaboliteConsistent, AbstractData):

    @property
    def __DATAID__(self):
        return self.edb_id

    @classmethod
    def to_serialize(cls):
        return [
            'edb_id', 'edb_source',
            'names', 'description',
            'attr_mul', 'edb_id_etc', 'attr_other', 'attr_etc',
            'pubchem_id', 'chebi_id', 'kegg_id', 'hmdb_id', 'lipidmaps_id',
            'cas_id', 'chemspider_id', 'metlin_id',
            'smiles', 'inchi', 'inchikey', 'formula',
            'charge', 'mass', 'mi_mass',
        ]

    @classmethod
    def to_json(cls):
        return ['names', 'attr_mul', 'edb_id_etc', 'attr_other', 'attr_etc']
