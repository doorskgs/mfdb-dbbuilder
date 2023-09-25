import asyncio
import os
import sys
import argparse
import csv

import toml

from metcore.dal_psycopg import db
from mdb_builder.discovery.config import build_discovery
from mdb_builder.discovery import consistency as cons
from pipebro import SettingWrapper


async def discovery(edb_tuples, config=None, file=None, csv_kwargs=None, verbose=None):

    if config == 'test':
        config = os.path.dirname(__file__) + '/test_discovery.toml'
    elif not config:
        config = os.path.dirname(__file__) + '/discovery.toml'

    # load config

    if file and edb_tuples:
        raise ValueError("Can't provide both file and EDB ids!")

    # discovery by file - convert it to list
    elif file:
        # todo: @later: large file support?
        with open(file) as fh:
            csv_reader = csv.DictReader(fh, **(csv_kwargs or {}))
            edb_tuples = list(map(tuple, csv_reader))
    else:
        it = iter(edb_tuples)
        edb_tuples = list(zip(it, it))

    if len(edb_tuples) > 1:
        # todo: support dataframes, csv, and multiple cmd args
        raise NotImplementedError("Only single row of EDB entry is implemented.")

    disco = build_discovery(config, verbose=verbose)
    disco.log_level = 'DEBUG'
    disco.log_file = f'../../discover_{edb_tuples[0][0]}_{edb_tuples[0][1]}.log'

    await disco.mgr.initialize()
    meta = disco.add_scalar_input(*edb_tuples[0])

    await disco.run_discovery()

    # evaluate
    c_master_ids, c_edb_ids, c_mass = cons.get_consistency_class(meta)

    print(repr(meta))
    print("Master ID consistency: ", c_master_ids)
    print("EDB ID consistency: ", c_edb_ids)
    print("Mass Consistency: ", c_mass)

    print("Found 2nd IDs:", disco.secondary_ids)
    print("Discovered:", disco.discovered)
    print("Undiscovered:", disco.undiscovered)
    print("Ambiguous:", disco.ambiguous)


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', type=str)
    parser.add_argument('--file', type=str)
    parser.add_argument('--verbose', type=bool)
    parser.add_argument('edb_tuples', nargs='*', type=str)

    args = parser.parse_args(sys.argv[1:])

    # TODO: refactor DB to where?
    # todo: auto-migrate DB if needed
    dbcfg = SettingWrapper(toml.load(os.path.dirname(__file__) + '/../../db.toml'))
    conn = db.try_connect(dbcfg)
    db.disconnect_db(conn)

    await discovery(**vars(args))


if __name__ == "__main__":
    asyncio.run(main())
