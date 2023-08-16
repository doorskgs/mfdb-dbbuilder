from eme.data_access import Repository, RepositoryBase

from ..entities.MetaboliteDBEntity import MetaboliteDBEntity


@Repository(MetaboliteDBEntity)
class MDBRepository(RepositoryBase):
    pass
