from sqlalchemy import Column, String

from .MetaboliteMixin import MetaboliteMixin
from .sqlbase import EntityBase


class ExternalDBEntity(MetaboliteMixin, EntityBase):
    __tablename__ = 'edb'

    edb_id = Column(String(20), primary_key=True)
    edb_source = Column(String(20), primary_key=True)

    def __init__(self, **kwargs):
        self.edb_source = kwargs.get('edb_source')
        self.edb_id = kwargs.get('edb_id')

        self.chebi_id = kwargs.get('chebi_id')
        self.kegg_id = kwargs.get('kegg_id')
        self.lipmaps_id = kwargs.get('lipmaps_id')
        self.pubchem_id = kwargs.get('pubchem_id')
        self.hmdb_id = kwargs.get('hmdb_id')
        self.cas_id = kwargs.get('cas_id')
        self.chemspider_id = kwargs.get('chemspider_id')
        self.metlin_id = kwargs.get('metlin_id')

        self.smiles = kwargs.get('smiles')
        self.inchi = kwargs.get('inchi')
        self.inchikey = kwargs.get('inchikey')
        self.formula = kwargs.get('formula')

        self.charge = kwargs.get('charge')
        self.mass = kwargs.get('mass')
        self.mi_mass = kwargs.get('mi_mass')

        self.names = kwargs.get('names')
        self.description = kwargs.get('description')

        self.attr_mul = kwargs.get('attr_mul')
        self.attr_other = kwargs.get('attr_other')