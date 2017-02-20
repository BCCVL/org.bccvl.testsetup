import unittest
import json
from pkg_resources import resource_listdir
from pkg_resources import resource_filename
import os
import os.path


class Test_JsonData(unittest.TestCase):

    def iter_content_resource(self, rroot='data/datasets'):
        for folder in resource_listdir('org.bccvl.testsetup', rroot):
            folder = resource_filename('org.bccvl.testsetup',
                                       os.path.join(rroot, folder))
            for root, dirs, files in os.walk(folder):
                for name in files:
                    yield os.path.join(root, name)

    def test_json_parseable(self):
        for item in self.iter_content_resource():
            if not (item.endswith(".json") or item.endswith('.txt')):
                continue
            e = None
            try:
                content = json.load(open(item))
            except Exception as e:
                content = None
            self.assertIsNotNone(content, "can't parse %s: %s" % (item, e))
