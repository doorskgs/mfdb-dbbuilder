import asyncpg


class RepositoryBase:
    T: type
    conn: asyncpg.Connection
    pool: asyncpg.Pool
    table_name: str
