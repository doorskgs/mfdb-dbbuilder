from sqlalchemy.dialects.postgresql import ARRAY

from .sqlbase import EntityBase

from sqlalchemy import Column, String


class SecondaryID(EntityBase):
    __tablename__ = 'secondary_id'

    edb_id = Column(String(12), primary_key=True)
    edb_source = Column(String(20), primary_key=True)
    secondary_ids = Column(ARRAY(String(20)))

    def __init__(self, **kwargs):
        self.edb_id = kwargs.get('edb_id')
        self.edb_source = kwargs.get('edb_source')
        self.secondary_ids = kwargs.get('secondary_ids')
