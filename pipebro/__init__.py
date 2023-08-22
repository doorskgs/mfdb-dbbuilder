import os

import toml

from .utils import SettingWrapper
from .elems.data_types import DTYPES, DTYPE
from .Runners.SerialRunner import SerialRunner
from .elems.AbstractData import AbstractData
from .debugging import debug_pipes, draw_pipes_network
from .Process.Process import Process
from .Process.Concurrent import Concurrent
from .Process.Producer import Producer
from .Process.Consumer import Consumer
from .utils import AutoIncrement


class PipeAppBuilder:
    def __init__(self):
        self.cfg_path: str = None
        self.queue_id_fn = AutoIncrement()
        self.processes = []
        self.runner = None

    def add_processes(self, processes):
        self.processes.extend(processes)

        for process in processes:
            if hasattr(process, 'cfg'):
                cfg_path = process.CFG_PATH if hasattr(process, 'CFG_PATH') else self.cfg_path + '/' + process.__PROCESSID__ + '.toml'

                if cfg_path == 'local':
                    # config path is the same as the file source.
                    # useful for specific processes
                    cfg_path = os.path.dirname(process.__file__) + '/' + process.__PROCESSID__ + '.toml'

                process.cfg = SettingWrapper(toml.load(cfg_path))

    def set_runner(self, rid):
        if 'serial' == rid:
            self.runner = SerialRunner
        # elif 'async' == rid:
        #     self.runner = AsyncRunner()

    def build_app(self):
        return self.runner(self.processes)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def pipe_builder(**kwargs):
    return PipeAppBuilder()


__all__ = [
    'pipe_builder',
    'Process', 'Concurrent', 'Producer', 'Consumer',
    'DTYPE', 'DTYPES', 'debug_pipes', 'draw_pipes_network',
    'AbstractData'
]
