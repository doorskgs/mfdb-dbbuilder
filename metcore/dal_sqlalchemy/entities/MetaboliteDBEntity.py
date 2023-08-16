from .MetaboliteMixin import MetaboliteMixin
from .sqlbase import EntityBase


class MetaboliteDBEntity(MetaboliteMixin, EntityBase):
    __tablename__ = 'mdb'
