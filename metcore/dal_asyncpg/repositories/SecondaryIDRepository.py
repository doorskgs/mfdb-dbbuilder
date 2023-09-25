import asyncpg

from edb_builder.dtypes import SecondaryID
from ..ctx import Repository
from .RepositoryBase import RepositoryBase


@Repository(SecondaryID)
class SecondaryIDRepository(RepositoryBase):
    table_name = "secondary_id"

    async def get_primary_id(self, edb_source, edb_id):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            record: asyncpg.Record = await conn.fetchrow("""
                SELECT edb_id
                FROM secondary_id
                WHERE edb_source = $2 AND $1 = ANY(secondary_ids)
            """, edb_id, edb_source)

        return record['edb_id'] if record else None
