import gzip
import os.path

import requests
from eme.pipe import Producer, DTYPES
from eme.pipe.elems.AbstractData import AbstractData
import zipfile



class BulkFileProducer(Producer):
    produces = str

    async def produce(self, data: str):
        dump_path = self.cfg.get('settings.dump_path')

        for edb_source, url in self.cfg['sources'].items():
            local_filename = url.split('/')[-1]
            check_end_filename = self.cfg.get(f'uncompressed_filenames.{edb_source}', cast=str)

            fn = self.cfg.get(f'dump_filenames.{edb_source}', default=local_filename, cast=str)
            #fn = fn(local_filename)
            #fn = fn.format(dict(edb_source=edb_source))
            #todo: @later: format filename?
            fn = os.path.join(dump_path, fn)
            check_end_filename = os.path.join(dump_path, check_end_filename)

            if not os.path.exists(check_end_filename):
                if self.app.verbose:
                    print(f"Downloading {edb_source} dump file: {fn}...")

                # download and unzip
                download_file(url, fn)

                uncompress_hierarchy(fn)

            yield fn, (str, f'{edb_source}_dump')
        self.mark_finished()


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

    os.unlink(local_filename)


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
