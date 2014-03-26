import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i, h2o_jobs

paramDict = {
    'response': [None,'C54'],
    'max_depth': [None, 1,10,20,100],
    'nbins': [None,5,10,100,1000],
    'ignored_cols_by_name': [None,'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    'mtries': [None,1,3,5,7,9,11,13,17,19,23,37,51],
    }

print "Will RF train on one dataset, test on another (multiple params)"
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_change_data_key_fvec(self):
        h2o.beta_features = True
        importFolderPath = 'standard'

        csvFilenameTrain = 'covtype.data'
        csvPathname = importFolderPath + "/" + csvFilenameTrain
        parseResultTrain = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=500)
        inspect = h2o_cmd.runInspect(key=parseResultTrain['destination_key'])
        dataKeyTrain = parseResultTrain['destination_key']
        print "Parse end", dataKeyTrain

        # we could train on covtype, and then use covtype20x for test? or vice versa
        # parseResult = parseResult
        # dataKeyTest = dataKeyTrain
        csvFilenameTest = 'covtype20x.data'
        csvPathname = importFolderPath + "/" + csvFilenameTest
        parseResultTest = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=500)
        print "Parse result['destination_key']:", parseResultTest['destination_key']
        inspect = h2o_cmd.runInspect(key=parseResultTest['destination_key'])
        dataKeyTest = parseResultTest['destination_key']

        print "Parse end", dataKeyTest

        # train
        # this does RFView to understand when RF completes, so the time reported for RFView here, should be 
        # considered the "first RFView" times..subsequent have some caching?. 
        # unless the no_confusion_matrix works

        # params is mutable. This is default.
        params = {
            'ntrees': 6, 
            'destination_key': 'RF_model'
        }

        colX = h2o_rf.pickRandRfParams(paramDict, params)
        kwargs = params.copy()
        # adjust timeoutSecs with the number of trees
        # seems ec2 can be really slow
        timeoutSecs = 30 + kwargs['ntrees'] * 60 

        start = time.time()
        rfv = h2o_cmd.runRF(parseResult=parseResultTrain,
            timeoutSecs=timeoutSecs, retryDelaySecs=1, noPoll=True, **kwargs)
        print "rf job dispatch end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'
        ### print "rf response:", h2o.dump_json(rfv)


        start = time.time()
        h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=180, pollTimeoutSecs=120, retryDelaySecs=5)
        print "rf job end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        print "\nRFView start after job completion"
        model_key = kwargs['destination_key']
        ntrees = kwargs['ntrees']
        start = time.time()
        h2o_cmd.runRFView(None, dataKeyTrain, model_key, ntrees, timeoutSecs)
        print "First rfview end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        for trial in range(3):
            # scoring
            start = time.time()
            rfView = h2o_cmd.runRFView(None, dataKeyTest, 
                model_key, ntrees, timeoutSecs, out_of_bag_error_estimate=1, retryDelaySecs=1)
            print "rfview", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntrees)
            # FIX! should update this expected classification error
            # self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)
            print "predict", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
