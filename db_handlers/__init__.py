import os


EDB_LIST = list(filter(lambda x: x.startswith('edp_'), os.listdir('.')))


__all__ = ['EDB_LIST']
