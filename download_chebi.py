import asyncio


from builder_pipe import build_chebi

if __name__ == "__main__":
    app = build_chebi()

    app.debug = False
    app.verbose = False

    #draw_pipes_network(pipe, filename='spike', show_queues=True)
    #debug_pipes(pipe)
    asyncio.run(app.run())
