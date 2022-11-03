@echo off
cd builder_pipe
python.exe builder_pipe/init_database.py
python.exe download_lipidmaps.py
python.exe download_chebi.py
python.exe download_hmdb.py
python.exe builder_pipe/copy_database.py
pause