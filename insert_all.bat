@echo off
python.exe builder_pipe/clear_edb_tmp_table.py
python.exe download_lipidmaps.py
python.exe download_chebi.py
python.exe download_pubchem.py
python.exe download_hmdb.py
python.exe builder_pipe/copy_database.py
pause