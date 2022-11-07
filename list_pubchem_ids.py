from collections import Counter
from math import log, floor

from eme.entities import load_settings
import numpy as np
import matplotlib.pyplot as plt

from core import SUPPORTED_BULK, SUPPORTED_DB
from core.dal.dbconn import connect_db, disconnect_db


# ------------------------------------- #
#         Config
# ------------------------------------- #
table_name = 'edb_tmp'                  #
LONGTAIL = 1500                         #
# ------------------------------------- #


def estimate(number):
    units = ['', 'K', 'M', 'G', 'T', 'P']
    k = 1000.0
    magnitude = int(floor(log(number, k)))
    return '%.2f%s' % (number / k**magnitude, units[magnitude])

def pubchem_bulk_file_bucket(pubchem_id):
    """
    Returns # of file for pubchem_id
    :param pubchem_id: pubchem id to return file for
    :return: file id, bulk file name
    """
    return (int(pubchem_id)-1) // 500000

def get_pubchem_bulk_file(m, fileext="SDF"):
    """
    Returns pubchem bulk file name from file # id (pubchem_bulk_file_bucket) and file extension
    :param m: file # id
    :param fileext: file extension, default: SDF
    :return: pubchem bulk file url
    """
    r1,r2 = get_pubchem_range(m)
    return f'https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/{fileext.upper()}/Compound_{r1}_{r2}.sdf.gz'

def get_pubchem_range(m):
    """
    Gets from-to range IDs for bucket
    :param m: file id #
    :return: (from, to) IDs for pubchem bulk file
    """
    r1 = str(m * 500000 + 1).zfill(9)
    r2 = str((m+1) * 500000).zfill(9)

    return r1,r2

def plot_frequencies(pubchem_ids):
    # get bulk file bucket frequencies\
    tf = Counter(
        map(get_pubchem_range,#lambda m: estimate(m * 500000 + 1),
            map(pubchem_bulk_file_bucket, pubchem_ids)
        )
    )

    s = sum(count for tag, count in tf.most_common() if count < LONGTAIL)
    cnt = sum(1 for tag, count in tf.most_common() if count >= LONGTAIL)
    total = sum(count for tag, count in tf.most_common())

    print(f"Total pubchem bulk files containing under {LONGTAIL} compounds: {s} / {total}")
    print(f"Total number of bulk files over {LONGTAIL}: {cnt}")


    y = [count for tag, count in tf.most_common()]
    x = [str(tag[0]) for tag, count in tf.most_common()]

    plt.bar(x, y, color='blue')
    plt.title("Pubchem metabolite histogram per SDF bucket")
    plt.ylabel("Frequency")
    #plt.yscale('log')  # optionally set a log scale for the y-axis
    plt.xticks(rotation=90)
    for i, (tag, count) in enumerate(tf.most_common(20)):
        plt.text(i, count, f' {count} ', rotation=90,
                 ha='center', va='top' if i < 10 else 'bottom', color='white' if i < 10 else 'black')
    plt.xlim(-0.6, len(x) - 0.4)  # optionally set tighter x lims
    plt.tight_layout()  # change the whitespace such that all labels fit nicely
    plt.show()


def main():
    cfg = load_settings('builder_pipe/config/db_dump.ini')

    conn, cur = connect_db(cfg)
    cur.execute(f"""
        SELECT pubchem_id
        FROM public.{table_name}
        WHERE pubchem_id IS NOT NULL;
    """)
    pubchem_ids = [r[0] for r in cur.fetchall()]
    disconnect_db(conn, cur)

    # save to pubchem download query file
    with open("pubchem_ids.txt", 'w') as fh:
        for pubchem_id in pubchem_ids:
            fh.write(f'{int(pubchem_id)}\n')

    #plot_frequencies(pubchem_ids)

if __name__ == "__main__":
    main()
