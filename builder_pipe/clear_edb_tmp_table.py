import os.path

from builder_pipe.db import connect_db, disconnect_db
from eme.entities import load_settings

_p = os.path.dirname(__file__)

def recreate_tables(table_name):
    # Create tmp table
    SQL = "DROP TABLE IF EXISTS {table_name};\n"
    with open(_p+'/../sql/edb_table.sql') as fh:
        SQL += fh.read()
    SQL = SQL.format(
        table_name=table_name,
        table_opts="UNLOGGED ",
        extra_columns=""
    )
    return SQL


def create_secondary():
    SQL = "DROP TABLE IF EXISTS {table_name};\n"
    with open(_p+'/../sql/add_secondary.sql') as fh:
        SQL += fh.read()
    SQL = SQL.format(
        table_name="secondary_id",
    )
    return SQL


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
    cfg = load_settings(_p+'/db.ini')
    conn = connect_db(cfg)
    cur = conn.cursor()

    execute(cur, recreate_tables("edb_tmp"))

    execute(cur, create_secondary())

    conn.commit()
    disconnect_db(conn, cur)

if __name__ == "__main__":
    main()