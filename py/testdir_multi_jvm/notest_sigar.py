import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts

class SigarApi(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,sigar=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_netstat(self):
        # Ask each node for network statistics
        for n in h2o.nodes:
            a = n.netstat()
            print a

if __name__ == '__main__':
    unittest.main()
