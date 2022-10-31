from eme.pipe import Process


class CSVParser(Process):
    """
    CSV & TSV parser
    """
    consumes = str, "filename"
    produces = dict, "csv_line"

    async def produce(self, data: str):
        items = defaultdict(dict)
        id_attr = mapping['__ID__']

        with open(filename, 'r', encoding=encoding) as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

            for row in csv_reader:
                m_id = row.pop(id_attr)

                for attr, val in row.items():
                    if attr not in items[m_id]:
                        items[m_id][attr] = []
                    cast = mapping.get(attr, str)
                    items[m_id][attr].append(cast(val) if val else None)

        items = dict(items)

        # post filter
        for bid, cfg in items.items():
            for _key, vals in cfg.copy().items():
                # remove [0,0,0,0...] and [null,null,null,...] values
                if not vals[0] and all(x == vals[0] for x in vals):
                    del cfg[_key]

