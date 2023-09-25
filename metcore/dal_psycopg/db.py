import psycopg2

from pipebro import SettingWrapper


def connect_db(cfg: SettingWrapper):
    assert cfg is not None
    conncfg = cfg['conn_'+cfg.get('db.conn')]
    conn = psycopg2.connect(**conncfg)

    conn.autocommit = cfg.get('db.autocommit', cast=bool)
    #cur = conn.cursor()

    return conn


def disconnect_db(conn, cur=None):
    if cur:
        cur.close()
    conn.close()


def try_connect(dbcfg: SettingWrapper, migrate_tables=None):
    from . import migrations

    if not migrate_tables:
        migrate_tables = []

    try:
        conn = connect_db(dbcfg)
    except psycopg2.OperationalError as e:
        if 'does not exist' not in e.args[0]:
            raise e

        print(f"Database not found. Create new database with DSN: {dbcfg.get('db.dsn')}? Y/n")
        if input().lower() == 'y':
            conncfg = dbcfg[f'conn_{dbcfg.get("db.conn")}']
            migrations.create_db(**conncfg)

            conn = migrations.migrate_db(*migrate_tables, close=False, **conncfg)
        else:
            print("Exiting. Please create database")
            return exit()
    return conn


def clear_database(conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE IF EXISTS secondary_id")
    cur.execute("TRUNCATE IF EXISTS edb_tmp")
    cur.execute("TRUNCATE IF EXISTS edb")
    cur.execute("TRUNCATE IF EXISTS mdb")
    conn.commit()
    cur.close()
