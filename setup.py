import re
import os
import sys
from collections import defaultdict

from setuptools import setup, find_packages


def find_package_data(where: str):
    p = re.compile('('+
        '|'.join([
            r'.*[\\/]content[\\/].*',
            r'.*[\\/]*\.(json|yaml|tpl|toml)',
        ])
    +')')

    incl = defaultdict(list)

    for root, dirs, files in os.walk(where):
        if re.match(p, root):
            for file in files:
                incl[groups.group(1)].append(os.path.join(root, file))

    return dict(incl)


setup(name='mfdb-parsinglib',
      version='1.0.0',
      description='Multi-purpose web framework',
      url='https://github.com/doorskgs/mfdb-parsinglib',
      author='oboforty',
      author_email='rajmund.csombordi@hotmail.com',
      license='MIT',
      zip_safe=False,
      # packages=find_packages(where='mfdb_parsinglib'),
      # package_data=find_package_data(where='mfdb_parsinglib'),
      package_dir={"": "."},
      entry_points={
        #   'console_scripts': [
        #       'midb = mfdb-parsinglib._tools.cli:main',
        #   ],
      },
      install_requires=[
      ])
