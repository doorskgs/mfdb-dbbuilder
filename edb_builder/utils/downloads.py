import os.path

import requests
import zipfile


def download_file(url, local_filename):

    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)



def uncompress_hierarchy(path):
    _, _ext = os.path.splitext(path)
    if _ext == '.zip':
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(path))

    _, _ext = os.path.splitext(path)
    if _ext == '.tar.gz':
        # todo: @later: handle tar.gz (no need for now)
        # with gzip.open(path, 'rb') as f:
        #
        #     print(1)
        #     file_content = f.read()
        pass
        # .gz files are not supported because we can yield from a later step (SDF parsing)
