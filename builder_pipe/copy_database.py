import os
from math import isclose

from mfdb_parsinglib import EDB_SOURCES
from mfdb_parsinglib.attributes import EDB_SOURCES_OTHER

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.db import connect_db, disconnect_db
from eme.entities import load_settings

_SQL_FKEY = """
ALTER TABLE {table_name} ADD CONSTRAINT fk_{prefix}{fk} FOREIGN KEY({fk}_id) REFERENCES {table_name} (edb_id);"""

_SQL_IDX = """CREATE INDEX {idx_name} ON {table_name} USING {impl} ({fk});"""
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


def sql_add_indexes(table_name, tables, suffix='', impl='btree', add_jsonb_idx=True):
    """
    Creates an SQL to add indices and foreign keys for EDB source table or Consistent metabolites
    :table_name: table name
    :param tables: EDB sources to generate constraints for
    :return:
    """
    SQL = []

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

DEBUG = False
def execute(cur, sql):
    if not sql:
        print("ERR: empty sql statement!")
        return

    if DEBUG:
        print("Executing")
        print(sql)
    cur.execute(sql)

def check_tmp_db(table_name, cur):
    sql = f"""SELECT edb_source, COUNT(*) as count
    FROM {table_name}
    GROUP BY edb_source
    """
    cur.execute(sql)
    edb = {edb_source: count for edb_source, count in cur.fetchall()}

    assert isclose(150300, edb['chebi'], rel_tol=0.1), "chebi not present: "+str(edb['chebi'])
    assert isclose(218000, edb['hmdb'], rel_tol=0.1), "hmdb not present: "+str(edb['hmdb'])
    assert isclose(47200, edb['lipmaps'], rel_tol=0.1), "lipmaps not present: "+str(edb['lipmaps'])
    assert isclose(250322, edb['pubchem'], rel_tol=0.1), "pubchem not present: "+str(edb['pubchem'])
    assert isclose(10881, edb['kegg'], rel_tol=0.1), "kegg not present: "+str(edb['kegg'])
    return True


def main():
    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))
    cur = conn.cursor()

    # # VALIDATE - GET GREEN LIGHT BEFORE OVERRIDING 'edb'
    # if not check_tmp_db('edb_tmp', cur):
    #     print("Please run all the EDB import scripts before running copy database.")
    #     exit()
    #
    # # SCHEMA:
    # print("Creating tables...")
    # execute(cur, create_db("edb"))
    # #execute(cur, create_db("mdb"))
    #
    # # INSERT:
    # print("Copying tables...")
    # execute(cur, copy_from_tmp_table("edb_tmp", "edb"))

    # INDEXES:
    print("Adding indexes and foreign keys...")
    execute(cur,
            sql_add_indexes("edb", EDB_SOURCES | EDB_SOURCES_OTHER, impl='btree', suffix='_id', add_jsonb_idx=True) +
            sql_add_indexes("edb", {'inchikey'}, impl='btree', add_jsonb_idx=True) +
            sql_add_indexes("edb", {'inchi', 'smiles'}, impl='hash', add_jsonb_idx=False) +
            sql_add_indexes("secondary_id", {'secondary_ids'}, impl='gin', add_jsonb_idx=False)
            #sql_add_indexes("edb", {'smiles'}, impl='gin') +
            #sql_add_indexes("edb", {'attr_mul'}, impl='gin')
        )

    # execute(cur, sql_add_foreignkeys("edb", SUPPORTED_BULK))

    # # CLEANUP
    # print("Cleaning up")
    # execute(cur, delete_table("edb_tmp"))

    conn.commit()
    disconnect_db(conn, cur)

if __name__ == "__main__":
    main()