import time
from typing import Generator, Iterable, AsyncGenerator
import collections
import asyncpg

from ..ctx import Repository
from ...attributes import EDB_SOURCES
from .RepositoryBase import RepositoryBase
from ...views.MetaboliteConsistent import MetaboliteConsistent


@Repository(MetaboliteConsistent)
class EDBRepository(RepositoryBase):
    table_name = "edb"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._s_edb_attributes = {
            'inchikey',
            'cas',
            'metlin',
            'swisslipids',
            'chemspider',
        }

        # Attributes that lack the _id suffix in their DB column names
        self._s_not_edb_id = {
            'inchi',# not supported tho
            'inchikey',
            'smiles',# not supported tho
        }

        self.iamspeed = collections.defaultdict(float)

    async def get(self, edb_source, edb_id) -> MetaboliteConsistent:
        """
        Gets EDB table record by primary key
        :param edb_source:
        :param edb_id:
        :return:
        """
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            data = dict(await conn.fetchrow("""
                SELECT *
                FROM edb
                WHERE edb_id = $1 and edb_source = $2
            """, edb_id, edb_source))

            data.pop("edb_id")
            return MetaboliteConsistent(**data)

    async def get_by(self, attribute, edb_id) -> list[MetaboliteConsistent]:
        """
        Gets record from EDB table by fetching either as pkey (edb_id) or foreign key (both edb_ids and attributes) or multiple cardinality attributes (attr_mul's keys).
        \nThe following edb_sources are supported for this query:\n
        - hmdb_id\n
        - chebi_id\n
        - pubchem_id\n
        - kegg_id\n
        - lipidmaps_id\n
        - inchikey\n
        - cas_id\n
        - metlin_id\n
        - swisslipids_id\n
        - chemspider_id\n

        The following attributes are not yet supported, but are planned to be supported as EDB ids in the future:\n
        - metlin_id\n
        - swisslipids_id\n
        - chemspider_id\n

        The following attributes will work but they're not supported due to lack of foreign keys:\n
        - inchi\n
        - smiles\n
        - mass\n
        - mi_mass\n
        - charge\n
        - mol\n

        :param attribute: external database source or attribute
        :param edb_id: external database ID
        :return: MetaboliteConsistent
        """

        # add _id prefix for non-attributes
        attr_db_name = attribute if attribute in self._s_not_edb_id else attribute + '_id'
        t1 = time.time_ns()

        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            self.iamspeed['pool_conn'] += time.time_ns() - t1

            if attribute in self._s_edb_attributes:
                # query by attribute (or non-mapped edb id)
                _query_args = ' '.join([
                    "SELECT * FROM edb",
                    f"WHERE {attr_db_name} = $1",
                    f"OR attr_mul->>'{attr_db_name}' = $1"
                ]), edb_id
            else:
                # query by EDB_ID
                _query_args = ' '.join([
                    "SELECT * FROM edb",
                    f"WHERE {attr_db_name} = $1",
                    f"OR (edb_id = $1 AND edb_source = '{attr_db_name}')"
                    f"OR attr_mul->>'{attr_db_name}' = $1"
                ]), edb_id
            # else:
            #     raise Exception(f"Attribute or EDB id not supported: {attribute}")

            result = []
            for row in await conn.fetch(*_query_args):
                data = dict(row)
                data.pop("edb_id")

                result.append(MetaboliteConsistent(**data))

            self.iamspeed[_query_args[0]] += time.time_ns() - t1

            return result

    async def list_ids_iter(self, edb_sources: list[str], stop_at=None):
        conn: asyncpg.Connection
        async with self.pool.acquire() as conn:
            _query = """
                SELECT edb_id, edb_source
                FROM edb
                WHERE """ + ' OR '.join(map(lambda x: f"edb_source = '{x}'", edb_sources))

            if stop_at:
                _query += f"LIMIT {stop_at}"

            for row in await conn.fetch(_query):
                yield row['edb_id'], row['edb_source']
