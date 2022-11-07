import asyncio
import sys

from builder_pipe import build_pubchem
from builder_pipe.utils.ding import dingdingding

if __name__ == "__main__":
    app = build_pubchem()

    mute = len(sys.argv) > 1 and 'mute' in sys.argv[1:]
    app.debug = len(sys.argv) > 1 and 'debug' in sys.argv[1:]
    app.verbose = len(sys.argv) > 1 and 'verbose' in sys.argv[1:]

    #draw_pipes_network(pipe, filename='spike', show_queues=True)
    #debug_pipes(pipe)
    asyncio.run(app.run())

    if not app.debug and not mute:
        dingdingding()
