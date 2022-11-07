import time
from io import BytesIO
from xml.etree import cElementTree as ET
from eme.pipe.utils import print_progress

t1 = time.time()
xfn = '../db_dumps/hmdb_metabolites.xml'

xmlns = "http://www.hmdb.ca"
fh = open(xfn, 'rb')
i = 0

context = ET.iterparse(fh, events=("end",), tag=f'{{{xmlns}}}metabolite')

for event, elem in context:
    i += 1
    if i % 1000 == 0:
        print_progress("{iter} [{dt}s]", i, tstart=t1)
    elem.clear()
print_progress("{iter} [{dt}s]", i, tstart=t1)

# Clean up
del context
fh.close()
t2 = time.time()


print("\n\nTime taken:", round(t2-t1), 's')
