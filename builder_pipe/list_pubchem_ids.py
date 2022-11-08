import os

from eme.entities import load_settings

from builder_pipe.db import connect_db, disconnect_db

_p = os.path.dirname(__file__)
def main(table_name='edb_tmp'):
    cfg = load_settings(os.path.dirname(__file__)+'/config/db_dump.ini')

    conn, cur = connect_db(cfg)
    cur.execute(f"""
        SELECT pubchem_id
        FROM public.{table_name}
        WHERE pubchem_id IS NOT NULL;
    """)
    pubchem_ids = [r[0] for r in cur.fetchall()]

    cur.execute(f"""
        SELECT json_array_elements(attr_mul->'pubchem_id')#>>'{{}}'
        FROM public.{table_name}
        WHERE attr_mul->'pubchem_id' IS NOT NULL
    """)
    pubchem_ids.extend(r[0] for r in cur.fetchall())
    disconnect_db(conn, cur)

    # save to pubchem download query file
    with open(_p+"/../db_dumps/pubchem_ids.txt", 'w') as fh:
        for pubchem_id in pubchem_ids:
            fh.write(f'{int(pubchem_id)}\n')


if __name__ == '__main__':
    print("Saving found Pubchem IDs")
    main("edb_tmp")
