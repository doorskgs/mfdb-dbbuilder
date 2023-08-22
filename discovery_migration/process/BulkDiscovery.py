from eme.mapper import map_to
from eme.pipe import Process
from mfdb_parsinglib import MetaboliteDiscovery, discovery, EDBSource
from mfdb_parsinglib.consistency import ConsistencyClass, get_consistency_class, MetaboliteConsistent


class BulkDiscovery(Process):
    consumes = tuple[str, str], "edb_tag_id"
    produces = (dict, "mdb")

    def initialize(self):
        self.disco = discovery(self.cfg, verbose=self.app.debug and self.app.verbose)
        self.processed = 0

    async def produce(self, edb_ids: tuple[str, str]):
        for edb_id, edb_tag in edb_ids:

            meta: MetaboliteDiscovery = self.disco.add_scalar_input(edb_tag, edb_id)

            try:
                await self.disco.run_discovery()

                cpkeys, c2ndids, cm = get_consistency_class(meta)
                is_consistent = (cpkeys == ConsistencyClass.Consistent and c2ndids == ConsistencyClass.Consistent)

                _dict = meta.to_dict()
                _dict['result'] = {
                    'is_consistent': is_consistent,
                    'cons_edb_id': cpkeys.value,
                    'cons_attr_id': c2ndids.value,
                    'cons_mass': cm.value,
                }

                # if consistent, save to consistent class for CSV/DB saving
                yield _dict, self.produces

            except Exception as e:
                if self.app.debug:
                    raise e
                else:
                    print("    DISCO ERR:", e)
                    # error happened during Discovery, save to skipped id file
                    yield (edb_id, edb_tag, str(e)), self.produces[2]

            self.processed += 1
            if self.processed % 1000 == 0:
                self.app.print_progress(self.processed)

                # @TODO: @TEMPORAL CODE - - - - -
                # if self.processed % 2000 == 0:
                #     print('\n\n')
                #     for k,v in self.disco.mgr.repo_edb.iamspeed.items():
                #         print("   ", k, '--->',v)
                #
                #     break

            self.disco.clear()
