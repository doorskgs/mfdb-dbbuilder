from typing import Generator, Iterable

from sqlalchemy import and_, func, select, text

from ..ctx import get_engine
from ..entities.ExternalDBEntity import ExternalDBEntity


@Repository(ExternalDBEntity)
class EDBRepository(RepositoryBase):

    def list_chebi_iter(self, stop_at=None) -> Iterable[ExternalDBEntity]:
        q = self.session.query(self.T)\
            .filter(ExternalDBEntity.edb_source == 'chebi')

        if stop_at:
            q = q.limit(stop_at)

        return q.yield_per(1000)

    def list_iter(self, start_from=None, stop_at=None) -> Iterable[tuple[str, str]]:
        q = self.session.query(ExternalDBEntity.edb_id, ExternalDBEntity.edb_source)\
            .order_by(ExternalDBEntity.edb_id)

        if start_from:
            q = q.offset(start_from)

        if stop_at:
            q = q.limit(stop_at)

        return q.yield_per(2000)
