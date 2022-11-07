import time

from eme.pipe.utils import print_progress
from lxml import etree


# def process_element(elem):
#     if elem.ns.text == '0':
#         print(elem.title.text)

xfn = '../db_dumps/hmdb_metabolites.xml'

xmlns = "http://www.hmdb.ca"

t1 = time.time()

fh = open(xfn, 'rb')

context=etree.iterparse(fh, events=('end',), tag=f'{{{xmlns}}}metabolite')
i = 0

for event, elem in context:
    i += 1

    for c in elem:
        print(c)

    if i % 1000 == 0:
        print_progress("{iter} [{dt}s]", i, tstart=t1)
    elem.clear(keep_tail=True)
    break
print_progress("{iter} [{dt}s]", i, tstart=t1)

# clean up
del context
fh.close()

t2 = time.time()


print("\n\nTime taken:", round(t2-t1), 's')
