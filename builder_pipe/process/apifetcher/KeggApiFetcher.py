import asyncio
import math
import os.path
import time

import aiohttp
from eme.pipe import Producer, Process
from mfdb_parsinglib.apihandlers.api_parsers.keggparser import parse_kegg, parse_kegg_async




class KeggApiFetcher(Process):
    consumes = list, "kegg_ids"
    produces = dict, "kegg_raw"

    def __init__(self, n, ti, dbsplit, t1, **kwargs):
        super().__init__(n, **kwargs)
        self.task_id = ti
        self.t1 = t1
        self.dbsplit = dbsplit

    def initialize(self):
        self._mapping = self.cfg['attribute_mapping']
        self._important_attr = self.cfg.get('attributes.kegg_attr_etc', cast=set, default=set())

        self.api_split = 10
        self.fetched = 0

        self.fetched_in_last_sec = 0

        self.throttling = 100 / self.dbsplit - 1
        self.throttling_time = 10

        self.url = 'https://rest.kegg.jp/get/'
        self.ids_left = {}


    async def produce(self, all_kegg_ids):
        # bulk fetch from KEGG api
        self.ids_left = set(all_kegg_ids)

        t1_t = time.time()

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(all_kegg_ids), self.api_split):
                bulk_ids = all_kegg_ids[i:i + self.api_split]

                async with session.get(self.url + '+'.join(map(lambda x: 'cpd:' + str(x), bulk_ids))) as resp:

                    if resp.status != 200:
                        t2_t = time.time()
                        print(f'   !!!   Task #{self.task_id}: STOPPED AT:', self.fetched, 'time taken: ', t2_t - t1_t)
                        break
                        #continue

                    # parse api response
                    async for data in parse_kegg_async(resp.content):
                        yield data

                    # mark ids as done
                    self.ids_left -= set(bulk_ids)

                    self.fetched += 1
                    self.fetched_in_last_sec += 1

                    if self.fetched % 50 == 0:
                        t2 = time.time()
                        print(f"Task #{self.task_id}: {(self.fetched * self.api_split)} (dt: {round(t2 - self.t1)}s)...")

                    if self.fetched_in_last_sec >= self.throttling:
                        self.fetched_in_last_sec = 0

                        print(f'Task #{self.task_id}: throttling ({self.throttling_time}s)')
                        await asyncio.sleep(self.throttling_time)


        if self.app.verbose:
            print(f"Task #{self.task_id}: fetching finished. API requests: {self.fetched}, Total items: {self.fetched * self.api_split}")

    def dispose(self):
        pass
