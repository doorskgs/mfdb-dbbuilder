from . import ctx

from .entities.ExternalDBEntity import ExternalDBEntity
#from .entities.MetaboliteDBEntity import MetaboliteDBEntity
from .entities.SecondaryID import SecondaryID

from .entities.sqlbase import EntityBase

from .repositories.EDBRepository import EDBRepository
#from .repositories.MDBRepository import MDBRepository
from .repositories.SecondaryIDRepository import SecondaryIDRepository


def drop_order():
    # determine a list of entities in which order they'll be dropped.
    # otherwise they are dropped in discovery order
    return None
