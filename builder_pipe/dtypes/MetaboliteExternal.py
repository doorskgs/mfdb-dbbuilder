from dataclasses import dataclass

from eme.pipe.elems.AbstractData import AbstractData

from metabolite_index.consistency import MetaboliteConsistent

from builder_pipe.dtypes.CSVSerializable import CSVSerializable

@dataclass
class MetaboliteExternal(MetaboliteConsistent, CSVSerializable, AbstractData):
    edb_source: str = None

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

    @property
    def edb_id(self):
        """
        Pkey for EDB table and MetaboliteExternal ID
        :return:
        """
        if self.edb_source == 'chebi':
            return self.chebi_id
        elif self.edb_source == 'hmdb':
            return self.hmdb_id
        elif self.edb_source == 'lipmaps' or self.edb_source == 'lipidmaps':
            return self.lipmaps_id
        elif self.edb_source == 'kegg':
            return self.kegg_id
        elif self.edb_source == 'pubchem':
            return self.pubchem_id
        elif self.edb_source == 'metlin':
            return self.metlin_id
        elif self.edb_source == 'chemspider':
            return self.chemspider_id
        else:
            raise Exception("unsupported source format")
