from eme.pipe.elems.AbstractData import AbstractData

from core.views.MetaboliteConsistent import MetaboliteConsistent


class MetaboliteExternal(MetaboliteConsistent, AbstractData):

    @property
    def __DATAID__(self):
        return self.edb_id
