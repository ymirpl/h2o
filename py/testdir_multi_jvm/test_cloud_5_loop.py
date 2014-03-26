import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def tearDownClass(cls):
        # this is for safety after error, plus gets us the grep of stdout/stderr for errors
        h2o.tear_down_cloud()

    def test_Cloud(self):
        # FIX! weird timeout H2O exceptions with >8? maybe shouldn't
        # don't know if we care
        base_port = 54321 + random.randint(0,256)
        ports_per_node = 2
        tryNodes = 5
        for trial in range(10):
            h2o.verboseprint("Trying cloud of", tryNodes)
            sys.stdout.write('.')
            sys.stdout.flush()

            start = time.time()
            h2o.build_cloud(tryNodes, base_port=base_port, 
                retryDelaySecs=2, timeoutSecs=max(30,10*tryNodes), java_heap_GB=1)
            print "trial #%d: Build cloud of %d in %d secs" % (trial, tryNodes, (time.time() - start))

            h2o.verify_cloud_size()
            time.sleep(5)
            h2o.tear_down_cloud()
            # base_port += ports_per_node * tryNodes
if __name__ == '__main__':
    h2o.unit_main()
