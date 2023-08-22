from edb_builder.run_pipe import run_pipe


if __name__ == "__main__":
    run_pipe(
        'edb_handlers.edb_pubchem.dbb',
        clear_db=True, mute=False
    )
