from eme.pipe.elems.AbstractData import AbstractData


class MetaboliteInconsistent(AbstractData):

    def __init__(self, meta_id):
        self.meta_id = meta_id

    @property
    def __DATAID__(self):
        return self.meta_id
