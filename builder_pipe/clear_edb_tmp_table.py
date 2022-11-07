from core.dal.dbconn import connect_db
from eme.entities import load_settings

def recreate_tables(conn, cur, table_name):
    # Create tmp table
    SQL = "DROP TABLE IF EXISTS {table_name};\n"
    with open('core/sql/edb_table.sql') as fh:
        SQL += fh.read()
    SQL = SQL.format(
        table_name=table_name,
        table_opts="UNLOGGED ",
        extra_columns=""
    )
    cur.execute(SQL)
    conn.commit()

def main():
    cfg = load_settings('builder_pipe/config/db_dump.ini')
    conn, cur = connect_db(cfg)

    table_name = "edb_tmp"

    recreate_tables(conn, cur, table_name)

if __name__ == "__main__":
    main()