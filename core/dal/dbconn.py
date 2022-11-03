import psycopg2
from eme.entities import SettingWrapper


def connect_db(cfg: SettingWrapper):
    conn = psycopg2.connect(**cfg['dbconn'])
    conn.autocommit = cfg.get('db.autocommit', cast=bool)
    cur = conn.cursor()

    return conn, cur


def disconnect_db(conn, cur):
    cur.close()
    conn.close()