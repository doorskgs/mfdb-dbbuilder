import json
import os
from mfdb_parsinglib.edb_formatting import get_id_from_url


class MockResponse:
    def __init__(self, text, status_code, ctype=None):
        self.text = text
        self.status_code = status_code
        self.headers = {
            'content-type': ctype
        }

    def json(self):
        return json.loads(self.content)

    @property
    def content(self):
        return self.text.encode('utf8')

    def iter_lines(self, decode_unicode=False):
        return iter(self.text.split('\n'))

    @property
    def is_redirect(self):
        return False


def mocked_requests_get(*args, **kwargs):

    uri = args[0] if len(args) >= 1 else kwargs['url']
    db_id, db_tag = get_id_from_url(uri)

    if 'hmdb' == db_tag:
        fn = f'data/apis/{db_id}.xml'
        content_type = 'application/xml'
    elif 'chebi' == db_tag:
        fn = 'data/apis/getCompleteEntity.xml'
        content_type = 'application/xml'
    elif 'kegg' == db_tag:
        if isinstance(db_id, list):
            db_id = '+'.join(db_id)
        fn = f'data/apis/cpd_{db_id}.txt'
        content_type = 'plain/text'
    else:
        raise NotImplementedError("test: " + db_tag)
        #return MockResponse(None, 404)

    if not os.path.exists(fn):
        return MockResponse(None, 404)
    with open(fn, 'r') as fh:
        data = fh.read()
    return MockResponse(data, 200, content_type)

