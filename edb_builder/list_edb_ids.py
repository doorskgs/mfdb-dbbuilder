import os
import sys

from eme.entities import load_settings

from edb_builder.db import connect_db, disconnect_db

db_source = sys.argv[1]

_p = os.path.dirname(__file__)
secfile = f"{_p}/../db_dumps/{db_source}_ids.txt"

def main(table_name='edb_tmp'):
    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))

    cur = conn.cursor()
    cur.execute(f"""
        SELECT distinct {db_source}_id
        FROM public.{table_name}
        WHERE {db_source}_id IS NOT NULL;
    """)
    db_ids = [r[0] for r in cur.fetchall()]

    cur.execute(f"""
        SELECT distinct json_array_elements(attr_mul->'{db_source}_id')#>>'{{}}'
        FROM public.{table_name}
        WHERE attr_mul->'{db_source}_id' IS NOT NULL
    """)
    db_ids.extend(r[0] for r in cur.fetchall())
    disconnect_db(conn, cur)

    db_ids = set(db_ids)

    # save to download query file
    with open(secfile, 'w') as fh:
        for db_id in db_ids:
            fh.write(f'{db_id}\n')


if __name__ == '__main__':
    print(f"Saving found {db_source} IDs...")
    main("edb_tmp")

    if db_source == 'pubchem':
        print(f"Saved pubchem IDs to {secfile}.\n1) Visit this link to fetch bulk:\nhttps://pubchem.ncbi.nlm.nih.gov/#upload=true")
        print("2) Select Chemical Structure Records > SDF > GZip")
        print("3) And place file as db_dumps/PubChem_compound_cache_midb_records.sdf.gz")
    elif db_source == 'kegg':
        print(f"Saved KEGG IDs to {secfile}\n1) Now run bulk fetcher")
