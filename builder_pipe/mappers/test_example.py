from sqlalchemy import Column, String, Float, Text,  Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

EntityBase = declarative_base()



class Metabolite(EntityBase):
    __tablename__ = 'metabolites'

    # Primary Ids
    meta_id = Column(String(20), primary_key=True)

    chebi_id = Column(String(20))
    kegg_id = Column(String(24))
    lipidmaps_id = Column(String(32))
    pubchem_id = Column(String(24))
    hmdb_id = Column(String(24))
    cas_id = Column(String(24))

    def __init__(self, **kwargs):
        super().__init__()
        self.meta_id = kwargs.get('meta_id')
        self.chebi_id = kwargs.get('chebi_id')
        self.kegg_id = kwargs.get('kegg_id')
        self.lipidmaps_id = kwargs.get('lipidmaps_id')
        self.pubchem_id = kwargs.get('pubchem_id')
        self.hmdb_id = kwargs.get('hmdb_id')
        self.cas_id = kwargs.get('cas_id')


class MetaboliteTesomsz:
    yeyeye: str

    def __init__(self, **kwargs):
        self.edb_source = kwargs.get('edb_source')
        self.chebi_id = kwargs.get('chebi_id')
        self.kegg_id = kwargs.get('kegg_id')
        self.lipidmaps_id = kwargs.get('lipidmaps_id')
        self.pubchem_id = kwargs.get('pubchem_id')
        self.hmdb_id = kwargs.get('hmdb_id')
        self.yeyeye = kwargs.get('yeyeye')

    @property
    def edb_id(self):
        if self.edb_source == 'chebi':
            return self.chebi_id
        elif self.edb_source == 'hmdb':
            return self.hmdb_id
        # etc..
