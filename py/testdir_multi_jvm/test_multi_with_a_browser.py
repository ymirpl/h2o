# this lets me be lazy..starts the cloud up like I want from my json, and gives me a browser
# copies the jars for me, etc. Just hangs at the end for 10 minutes while I play with the browser
import unittest
import time,sys
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        h2o.beta_features = True
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=1, use_hdfs=True, base_port=54321)
        else:
            h2o_hosts.build_cloud_with_hosts(base_port=54321)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_multi_with_a_browser(self):
        h2b.browseTheCloud()

        if not h2o.browse_disable:
            time.sleep(500000)

if __name__ == '__main__':

    h2o.unit_main()
