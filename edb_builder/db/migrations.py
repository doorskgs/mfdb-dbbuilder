import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


from ..dtypes import MetaboliteExternal
from .provision import check_db_counts

_p = os.path.dirname(__file__)


_SQL_FKEY = """
ALTER TABLE {table_name} ADD CONSTRAINT fk_{prefix}{fk} FOREIGN KEY({fk}_id) REFERENCES {table_name} (edb_id);"""


def copy_from_tmp_table(table_from, table_to):
    sr = MetaboliteExternal.to_serialize()

    # i = sr.index("names")
    # sr.remove("names")
    # sr.insert(i, "")

    SQL = f"""INSERT INTO {table_to}
SELECT {','.join(sr)}
FROM {table_from}
    """
    return SQL


def sql_add_indexes(table_name, tables, suffix='', impl='btree', add_jsonb_idx=True):
    """
    Creates an SQL to add indices and foreign keys for EDB source table or Consistent metabolites
    :table_name: table name
    :param tables: EDB sources to generate constraints for
    :return:
    """
    SQL = []
    _SQL_IDX = """CREATE INDEX {idx_name} ON {table_name} USING {impl} ({fk});"""

    for key in tables:
        SQL.append(_SQL_IDX.format(
            idx_name=key+suffix,
            fk=key+suffix,
            impl=impl,
            table_name=table_name
        ))

        if impl == 'btree' and add_jsonb_idx:
            # add btree index within JSONB (multiple cardinality) ids as well
            SQL.append(_SQL_IDX.format(
                idx_name=key+suffix,
                fk=f"(attr_mul->>'{key}{suffix}'::text) COLLATE pg_catalog.\"default\"",
                impl=impl,
                table_name=table_name
            ))

    return "\n".join(SQL)


def sql_add_foreignkeys(table_name, tables):
    SQL = []

    for key in tables:
        SQL.append(_SQL_FKEY.format(
            fk=key,
            table_name=table_name,
            prefix=""
        ))
    return "\n".join(SQL)


def delete_table(table_name):
    return f"DROP TABLE {table_name}"


def create_table(table_name, opts=None):
    # Create tmp table
    SQL = "DROP TABLE IF EXISTS {table_name};\n"
    with open(_p+'/sql/edb_table.sql') as fh:
        SQL += fh.read()

    try:
        with open(f'{_p}/sql/{table_name}_extra_attr.sql') as fh:
            extra_cols = fh.read()
    except IOError:
        extra_cols = ""

    SQL = SQL.format(
        table_name=table_name,
        table_opts=opts or "",
        extra_columns=extra_cols
    )
    return SQL


def create_secondary():
    SQL = "DROP TABLE IF EXISTS {table_name};\n"
    with open(_p+'/sql/add_secondary.sql') as fh:
        SQL += fh.read()
    SQL = SQL.format(
        table_name="secondary_id",
    )
    return SQL


def execute(cur, sql, debug=False):
    if not sql:
        raise ValueError("ERR: empty sql statement!")

    if debug:
        print("Executing")
        print(sql)
    cur.execute(sql)


def create_db(**kwargs):
    db_name = kwargs.pop('database')
    conn = psycopg2.connect(**kwargs)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);

    cur = conn.cursor()

    execute(cur, f"CREATE DATABASE {db_name}", debug=True)

    cur.close()
    conn.close()


def migrate_db(*args, close=True, debug=True, **kwargs):
    if 'conn' in kwargs:
        conn = kwargs.pop('conn')
    else:
        conn = psycopg2.connect(**kwargs)

    cur = conn.cursor()
    tables = set(args or ['mdb', 'edb', 'edb_tmp'])

    if 'edb_tmp' in tables:
        execute(cur, create_table("edb_tmp", opts="UNLOGGED "), debug=debug)
        execute(cur, create_secondary(), debug=debug)

    if 'edb' in tables:
        execute(cur, create_table("edb"), debug=debug)

    if 'mdb' in tables:
        execute(cur, create_table("mdb"), debug=debug)

    conn.commit()

    if close:
        cur.close()
        conn.close()

    return conn


def copy_database(*args, close=True, clean_tmp=True, debug=True, **kwargs):
    if 'conn' in kwargs:
        conn = kwargs.pop('conn')
    else:
        conn = psycopg2.connect(**kwargs)

    cur = conn.cursor()

    # if not args:
    #     raise Exception("Provide args as ")
    #EDB_SOURCES | EDB_SOURCES_OTHER
    # VALIDATE - GET GREEN LIGHT BEFORE OVERRIDING 'edb'
    if not check_db_counts('edb_tmp', cur):
        print("Please run all the EDB import scripts before running copy database.")
        exit()

    # INSERT:
    print("Copying tables...")
    execute(cur, copy_from_tmp_table("edb_tmp", "edb"), debug=debug)

    # INDEXES:
    print("Adding indexes and foreign keys...")
    execute(cur,
            sql_add_indexes("edb", args, impl='btree', suffix='_id', add_jsonb_idx=True) +
            sql_add_indexes("edb", {'inchikey'}, impl='btree', add_jsonb_idx=True) +
            sql_add_indexes("edb", {'inchi', 'smiles'}, impl='hash', add_jsonb_idx=False) +
            sql_add_indexes("secondary_id", {'secondary_ids'}, impl='gin', add_jsonb_idx=False)
            #sql_add_indexes("edb", {'smiles'}, impl='gin') +
            #sql_add_indexes("edb", {'attr_mul'}, impl='gin')
        , debug=debug)
    SUPPORTED_BULK = [
        'chebi', 'hmdb', 'lipidmaps', 'pubchem'
    ]
    execute(cur, sql_add_foreignkeys("edb", SUPPORTED_BULK), debug=debug)

    # CLEANUP
    if clean_tmp:
        print("Cleaning up")
        execute(cur, delete_table("edb_tmp"), debug=debug)

    if close:
        cur.close()
        conn.close()


if __name__ == "__main__":
    migrate_db()
