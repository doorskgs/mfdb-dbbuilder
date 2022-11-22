import asyncio

import aiohttp
from mfdb_parsinglib.apihandlers.api_parsers.keggparser import parse_kegg_async

e = ["C00001","C00002","C00003"]
url = 'https://rest.kegg.jp/get/'

async def main():

    async with aiohttp.ClientSession() as session:
        _url = url + '+'.join(map(lambda x: 'cpd:' + str(x), e))

        async with session.get(_url) as resp:
            async for data in parse_kegg_async(resp.content):
                print(data)

    print('anyad')

if __name__ == "__main__":
    asyncio.run(main())

    print("bye")
