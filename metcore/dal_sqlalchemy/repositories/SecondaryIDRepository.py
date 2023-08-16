from sqlalchemy import and_, func
from sqlalchemy.dialects.postgresql import array, dialect

from ..entities.SecondaryID import SecondaryID


@Repository(SecondaryID)
class SecondaryIDRepository(RepositoryBase):

    def get_primary_id(self, eid2nd, edb_source):

        return self.session.query(SecondaryID.edb_id)\
            .filter(SecondaryID.edb_source==edb_source)\
            .filter(SecondaryID.secondary_ids.any(eid2nd))\
            .first()
