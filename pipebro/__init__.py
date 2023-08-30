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

    def add_processes(self, processes, cfg_path=None):
        self.processes.extend(processes)

        if not cfg_path:
            cfg_path = self.cfg_path or ''

        for process in processes:
            if hasattr(process, 'cfg'):
                cfg_file_path = os.path.join(cfg_path, process.__PROCESSID__ + '.toml')
                try:
                    process.cfg = SettingWrapper(toml.load(cfg_file_path))
                except toml.decoder.TomlDecodeError as e:
                    raise Exception(f"Error parsing Process config: {cfg_file_path} - {e}")

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
