import asyncio
from queue import SimpleQueue

q = {
    'prod': SimpleQueue(),
    'szar': SimpleQueue(),
    'fos': SimpleQueue(),
}

async def prod(start_event):
    print("START:", start_event)

    for i in range(100):
        yield "fos", i

        if i % 30 == 0:
            await asyncio.sleep(1)


async def fos(inp):
    strstr, i = inp
    yield i


async def szar(i):
    yield "szar", i

processes = [
    (prod, 'prod'),
    (fos, 'fos'),
    (szar, 'szar')
]


async def main():
    q['prod'].put("start_event")

    while True:
        for proc, proc_id in processes:
            print(proc_id)
            if not q[proc_id].empty():
                #print('  [Q]', proc_id, q[proc_id].qsize())
                resp = q[proc_id].get()

                async for eks in proc(resp):
                    print('  [PROD]', proc_id, eks)

                    # process linkage fake
                    if proc_id == 'prod':
                        cons_id = 'fos'
                    elif proc_id == 'fos':
                        cons_id = 'szar'
                    elif proc_id == 'szar':
                        print("CONSUMED: ", szar)
                        break
                    else:
                        raise Exception("Not found for " + proc_id)

                    print('  [ENQU]', cons_id, eks)
                    q[cons_id].put(resp)

    # # Create an Event object.
    # event = asyncio.Event()
    # waiter_task = asyncio.create_task(waiter(event))
    #
    # await asyncio.sleep(1)
    # event.set()
    #
    # await asyncio.sleep(3)
    # event.set()
    #
    # # Wait until the waiter task is finished.
    # await waiter_task

asyncio.run(main())
