from eme.pipe.elems.AbstractData import AbstractData


class MetaboliteExternal(AbstractData):

    def __init__(self, edb_id, edb_source):
        self.edb_id = edb_id
        self.edb_source = edb_source

    @property
    def __DATAID__(self):
        return self.edb_id
