from pipebro.elems.AbstractData import AbstractData

from metcore.views import MetaboliteConsistent
from edb_builder.dtypes.CSVSerializable import CSVSerializable


class MetaboliteExternal(MetaboliteConsistent, CSVSerializable, AbstractData):

    @property
    def __DATAID__(self):
        return self.edb_id

    @classmethod
    def to_serialize(cls):
        """
        Lists the order of attributes to be serialized
        This follows the CREATE SQL statement of edb_table.sql
        :return:
        """
        return [
            'edb_id', 'edb_source',
            'pubchem_id', 'chebi_id', 'hmdb_id', 'kegg_id', 'lipmaps_id',
            'cas_id', 'chemspider_id', 'metlin_id', 'swisslipids_id',
            'smiles', 'inchi', 'inchikey', 'formula',
            'charge', 'mass', 'mi_mass',
            'names', 'description',
            'attr_mul', 'attr_other',
        ]

    @classmethod
    def to_json(cls):
        return ['names', 'attr_mul', 'attr_other']
