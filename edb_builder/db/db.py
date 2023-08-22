import psycopg2

from pipebro import SettingWrapper


def connect_db(cfg: SettingWrapper):
    assert cfg is not None
    conn = psycopg2.connect(**cfg['dbconn'])

    conn.autocommit = cfg.get('db.autocommit', cast=bool)
    #cur = conn.cursor()

    return conn


def disconnect_db(conn, cur):
    cur.close()
    conn.close()
