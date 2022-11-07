import os

from builder_pipe.dtypes.Metabolite import Metabolite
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.db import connect_db, disconnect_db
from eme.entities import load_settings

_SQL_FKEY = """
ALTER TABLE {table_name} ADD CONSTRAINT fk_{prefix}{fk} FOREIGN KEY({fk}_id) REFERENCES {table_name} (edb_id);"""

_SQL_IDX = """CREATE INDEX ON {table_name} USING btree ({fk}_id);
"""

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

def fill_secondary_table():
    from time import time

    # from pyproto import ctx
    # from pyproto.entities.ChEBIData import CHEBIData
    # from pyproto.utils import parse_iter_sdf, compile_names, force_list, compile_extra_refs
    #
    # if not ctx.table_exists('secondary_id'):
    #     ctx.create_database()
    # else:
    #     print("Truncated data")
    #     ctx.truncate('secondary_id')
    # sess = ctx.get_session()
    #
    # sess.execute("""
    # INSERT INTO secondary_id
    #   SELECT 'chebi_id' as "db_tag", unnest(chebi_id_alt) as "secondary_id", chebi_id as "primary_id"
    #   FROM chebi_data
    # """)
    #
    # sess.execute("""
    # INSERT INTO secondary_id
    #   SELECT 'hmdb_id' as "db_tag", unnest(hmdb_id_alt) as "secondary_id", hmdb_id as "primary_id"
    #   FROM hmdb_data
    # """)
    pass


def create_db(table_name):
    SQL = "DROP TABLE IF EXISTS {table_name};\n"

    with open('sql/edb_table.sql') as fh:
        SQL += fh.read()
    with open(f'sql/{table_name}_extra_attr.sql') as fh:
        extra_cols = fh.read()

    SQL = SQL.format(
        table_name=table_name,
        table_opts="",
        extra_columns=extra_cols
    )

    return SQL

def create_secondary():
    SQL = "DROP TABLE IF EXISTS {table_name};\n"
    with open('sql/add_secondary.sql') as fh:
        SQL += fh.read()
    SQL = SQL.format(
        table_name="secondary_id",
    )
    return SQL


def sql_add_indexes(table_name, tables):
    """
    Creates an SQL to add indices and foreign keys for EDB source table or Consistent metabolites
    :table_name: table name
    :param tables: EDB sources to generate constraints for
    :return:
    """
    SQL = []

    for key in tables:
        SQL.append(_SQL_IDX.format(
            fk=key,
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

DEBUG = False
def execute(cur, sql):
    if not sql:
        print("ERR: empty sql statement!")
        return

    if DEBUG:
        print("Executing")
        print(sql)
    cur.execute(sql)


def main():
    cfg = load_settings(os.path.dirname(__file__)+'/config/db_dump.ini')
    conn, cur = connect_db(cfg)

    # SCHEMA:
    print("Creating tables...")
    execute(cur, create_db("mdb"))

    # CLEANUP
    print("Cleaning up")

    conn.commit()
    disconnect_db(conn, cur)

if __name__ == "__main__":
    main()