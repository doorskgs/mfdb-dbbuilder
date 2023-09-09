import psycopg2

from pipebro import SettingWrapper


def connect_db(cfg: SettingWrapper):
    assert cfg is not None
    conn = psycopg2.connect(**cfg['dbconn'])

    conn.autocommit = cfg.get('db.autocommit', cast=bool)
    #cur = conn.cursor()

    return conn


def disconnect_db(conn, cur=None):
    if cur:
        cur.close()
    conn.close()


def try_connect(dbcfg: SettingWrapper):
    from . import migrations

    try:
        conn = connect_db(dbcfg)
    except psycopg2.OperationalError as e:
        if 'does not exist' not in e.args[0]:
            raise e

        print(f"Database not found. Create new database with DSN: {dbcfg['dbconn']}? Y/n")
        if True or input().lower() == 'y':
            migrations.create_db(**dbcfg['dbconn'])

            conn = migrations.migrate_db(close=False, **dbcfg['dbconn'])
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
