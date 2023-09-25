import logging
import time
from importlib import import_module

from metcore.parsinglib import pad_id
from metcore.views import MetaboliteConsistent

from metcore.dal_asyncpg import get_repo, initialize_db
from metcore.dal_asyncpg.repositories.EDBRepository import EDBRepository
from metcore.dal_asyncpg.repositories.SecondaryIDRepository import SecondaryIDRepository

from edb_builder.dtypes import SecondaryID

from edb_handlers.core.ApiClientBase import ApiClientBase
from edb_handlers import EDB_SOURCES
from edb_handlers.edb_hmdb.dbb.parselib import replace_obvious_hmdb_id

logger = logging.getLogger('disco')


class EDBManager:

    def __init__(self, secondary_ids: set, opts):
        """
        Manages External DB's in-app cache and EDB's public API to fetch EDB records
        :param secondary_ids:
        """
        self.apis: dict[str, ApiClientBase] = {
            # 'chebi': ChebiClient(),
            # 'kegg': KeggClient(),
            # 'pubchem': PubchemClient(),
            # 'hmdb': HMDBClient(),
            # 'lipmaps': LipidmapsClient()
        }

        # dynamically import Api Client from each EDB Source
        for EDB_SOURCE in EDB_SOURCES:
            m = import_module(f'edb_handlers.edb_{EDB_SOURCE}.api')
            self.apis[EDB_SOURCE] = m.client()
            logger.debug(f"Adding API client for {EDB_SOURCE} ({m.client})")

        self.repo_edb: EDBRepository = get_repo(MetaboliteConsistent)
        self.repo_2nd: SecondaryIDRepository = get_repo(SecondaryID)

        self.secondary_ids = secondary_ids
        self.opts = opts
        self.t1 = time.time()

    async def initialize(self):
        """
        Initializes aio database and http libraries, connections
        """

        await initialize_db()

    async def get_metabolite(self, edb_tag: str, edb_id: str) -> list[MetaboliteConsistent]:
        """

        :param edb_tag:
        :param edb_id:
        :return:
        """
        edb_records: list[MetaboliteConsistent] | None = None

        edb_source = edb_tag.removesuffix('_id')
        opts = self.opts.get_opts(edb_source)

        logger.debug(f"GET: {edb_source}[{edb_id}]")

        if edb_source == 'hmdb':
            # pad hmdb id with 00, so that obvious secondary IDs are also found in DB
            #       (both formats are guaranteed for api fetch)
            logger.debug(f"Secondary ID {edb_source}[{edb_id}] -> (HMDB obvious 00 padding)")
            edb_id = replace_obvious_hmdb_id(edb_id)

        if opts.cache_enabled:
            # find by edb table
            edb_records = await self.repo_edb.get_by(edb_source, edb_id)

            if edb_records:
                logger.debug(f"Cache hit: {edb_source}[{edb_id}]")

        if not edb_records:
            # todo: make option to turn secondary ID resolve off
            #       -- this can optimize discoveries that dont rely on bulk filled cache
            # find primary ID from secondary id
            if edb_id_2nd := await self.resolve_secondary_id(edb_source, edb_id):
                logger.debug(f"Secondary ID {edb_source}[{edb_id}] -> {edb_source}[{edb_id_2nd}]")

                # query again
                edb_records = await self.repo_edb.get_by(edb_source, edb_id)

                if not edb_records:
                    logger.debug(f"Secondary ID {edb_source}[{edb_id}] -> {edb_source}[{edb_id_2nd}] was not cached in EDB table!")

        if not edb_records:
            if opts.api_enabled:
                if edb_record := await self.fetch_api(edb_source, edb_id, save_in_cache=opts.cache_upsert):
                    #logger.debug(f"API response for {edb_source}[{edb_id}]: {edb_record}")
                    edb_records = [edb_record]
            else:
                logger.warning(f"Record {edb_source}[{edb_id}] was not cached in bulk EDB database, "
                               f"and API fetching is disabled for {edb_source}."
                               f"You sould enable API fetching in options")

        if not edb_records:
            logger.debug(f"GET: {edb_source}[{edb_id}] failed")

        return edb_records

    async def fetch_api(self, edb_tag, edb_id, save_in_cache=False):
        """
        fetch edb record from their public API
        :param edb_tag:
        :param edb_id:
        :param save_in_cache:
        :return:
        """
        now = time.time()

        edb_id_padded = pad_id(edb_id, edb_tag)

        logger.info(f"Fetching API: {edb_tag}[{edb_id_padded}]")
        edb_record: MetaboliteConsistent = await self.apis[edb_tag].fetch_api(edb_id_padded)
        logger.debug(f"Fetching API: {edb_tag}[{edb_id_padded}] took {(time.time())-now}")

        self.t1 = now

        # map to edb_record
        #edb_record: ExternalDBEntity = map_to(edb_api, ExternalDBEntity)
        # Metabolite Consistent lacks edb id & source, so we manually add this after the mapping
        # edb_record.edb_id = edb_id
        # edb_record.edb_source = edb_tag.removesuffix('_id')

        if edb_record:
            if save_in_cache:
                logger.debug(f"Caching API result in: {edb_tag}[{edb_id}]")
                # cache api results to table

                # TODO: $ITT: implement repo create
                await self.repo_edb.create(edb_record)
        else:
            logger.info(f"API 404 for {edb_tag}[{edb_id}]")

        return edb_record

    async def resolve_secondary_id(self, edb_source, edb_id):
        """
        Gets record by querying EDB ID as a secondary ID instead of pkey
        :param edb_source:
        :param edb_id:
        :return:
        """
        primary_id = await self.repo_2nd.get_primary_id(edb_source, edb_id)

        if not primary_id:
            return None
        else:
            edb_tag = edb_source + '_id'
            self.secondary_ids.add((edb_tag, edb_id))

            return primary_id
