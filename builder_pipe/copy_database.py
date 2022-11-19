import os

from metabolite_index import EDB_SOURCES
from metabolite_index.attributes import EDB_SOURCES_OTHER

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.db import connect_db, disconnect_db
from eme.entities import load_settings

_SQL_FKEY = """
ALTER TABLE {table_name} ADD CONSTRAINT fk_{prefix}{fk} FOREIGN KEY({fk}_id) REFERENCES {table_name} (edb_id);"""

_SQL_IDX = """CREATE INDEX ON {table_name} USING {impl} ({fk}{suffix});"""

_p = os.path.dirname(__file__)


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



def create_db(table_name):
    SQL = "DROP TABLE IF EXISTS {table_name};\n"

    with open(_p+'/../sql/edb_table.sql') as fh:
        SQL += fh.read()
    with open(f'{_p}/../sql/{table_name}_extra_attr.sql') as fh:
        extra_cols = fh.read()

    SQL = SQL.format(
        table_name=table_name,
        table_opts="",
        extra_columns=extra_cols
    )

    return SQL


def sql_add_indexes(table_name, tables, suffix='', impl='btree'):
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
            impl=impl,
            suffix=suffix,
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
    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))
    cur = conn.cursor()

    # SCHEMA:
    print("Creating tables...")
    execute(cur, create_db("edb"))
    #execute(cur, create_db("mdb"))

    # INSERT:
    print("Copying tables...")
    execute(cur, copy_from_tmp_table("edb_tmp", "edb"))

    # INDEXES:
    print("Adding indexes and foreign keys...")
    execute(cur,
            sql_add_indexes("edb", EDB_SOURCES | EDB_SOURCES_OTHER, impl='btree', suffix='_id') +
            sql_add_indexes("edb", {'inchikey'}, impl='btree') +
            sql_add_indexes("edb", {'inchi', 'smiles'}, impl='hash')
            #sql_add_indexes("edb", {'smiles'}, impl='gin')
            )
    #execute(cur, sql_add_foreignkeys("edb", SUPPORTED_BULK))

    # CLEANUP
    print("Cleaning up")
    execute(cur, delete_table("edb_tmp"))

    conn.commit()
    disconnect_db(conn, cur)

if __name__ == "__main__":
    main()