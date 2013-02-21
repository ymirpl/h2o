import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_pp, h2o_rf

# RF train parameters
paramsTrainRF = { 
            'ntree'      : 100, 
            'depth'      : 300,
            'parallel'   : 1, 
            'bin_limit'  : 20000,
            'ignore'     : 'ArrDelay,DepDelay',
            'gini'       : 0,
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit'    : 0,
            'timeoutSecs': 14800,
            }

# RF test parameters
paramsScoreRF = {
            'timeoutSecs': 14800,
        }

trainDS = {
        's3bucket'    : 'h2o_airlines_unpacked',
        'filename'    : 'year1987.csv',
        'timeoutSecs' : 14800,
        'header'      : True
        }

scoreDS = {
        's3bucket'    : 'h2o_airlines_unpacked',
        'filename'    : 'year1988.csv',
        'timeoutSecs' : 14800,
        'header'      : True
        }

PARSE_TIMEOUT=14800

class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def parseS3File(self, s3bucket, filename, **kwargs):
        start      = time.time()
        parseKey   = h2o_cmd.parseS3File(bucket=s3bucket, filename=filename, **kwargs)
        parse_time = time.time() - start 
        h2o.verboseprint("S3 parse took {0} sec".format(parse_time))
        parseKey['python_call_timer'] = parse_time
        return parseKey

    def loadTrainData(self):
        trainKey = self.parseS3File(s3bucket=trainDS['s3bucket'], filename=trainDS['filename'], trainDS.copy())
        return trainKey
    
    def loadScoreData(self):
        scoreKey = self.parseS3File(s3bucket=scoreDS['s3bucket'], filename=scoreDS['filename'], scoreDS.copy())
        return trainKey

    def test_RF(self):
        trainKey = self.loadTrainData()
        kwargs   = paramsTrainRF.copy()
        trainResult = h2o_rf.trainRF(trainKey, **kwargs)

        scoreKey = self.loadScoreData()
        kwargs   = paramsScoreRF.copy()
        scoreResult = h2o_rf.scoreRF(scoreKey, trainResult, **kwargs)

        print "\nTrain\n=========={0}".format(h2o_pp.pp_rf_result(trainResult))
        print "\nScoring\n========={0}".format(h2o_pp.pp_rf_result(scoreResult))

if __name__ == '__main__':
    h2o.unit_main()
