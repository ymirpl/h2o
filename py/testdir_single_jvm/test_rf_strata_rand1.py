import unittest, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i

# make a dict of lists, with some legal choices for each. None means no value.
# assume poker1000 datset

# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 0-8 in the last col of poker1000

paramDict = {
    'sampling_strategy': ['RANDOM', 'STRATIFIED_LOCAL' ],
    'strata_samples': [
        "0=10",
        "1=5",
        "0=7,2=3", 
        "0=1,1=1,2=1,3=1,4=1,5=1,6=1,7=1,8=1", 
        # all 100 causes an error in the checking (div by 0) ?
        "0=90,1=90,2=90,3=90,4=90,5=90,6=90,7=90,8=90", 
        "0=10,1=20,2=30,3=40,4=50,5=40,6=30,7=20,8=10", 
        "0=90,1=70,2=50,3=30,4=10,5=30,6=50,7=70,8=90", 
        "0=0,1=0,2=0,3=0,4=0,5=0,6=0,7=0,8=0",
        ]
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_loop_random_param_poker1000(self):
        csvPathname = 'poker/poker1000'
        for trial in range(20):
            # params is mutable. This is default.
            params = {'ntree': 19}
            colX = h2o_rf.pickRandRfParams(paramDict, params)
            kwargs = params.copy()
            # adjust timeoutSecs with the number of trees
            timeoutSecs = 30 + kwargs['ntree'] * 10
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', 
                timeoutSecs=timeoutSecs)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
