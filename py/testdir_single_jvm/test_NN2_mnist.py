import unittest, time, sys, random, string

sys.path.extend(['.','..','py'])
import h2o, h2o_gbm, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=2, base_port=54323)
        else:
            h2o_hosts.build_cloud_with_hosts(base_port=54323)

    @classmethod
    def tearDownClass(cls):
        ###h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_NN_mnist(self):
        #h2b.browseTheCloud()
        h2o.beta_features = True
        csvPathname_train = 'mnist/train.csv.gz'
        csvPathname_test  = 'mnist/test.csv.gz'
        hex_key = 'mnist_train.hex'
        validation_key = 'mnist_test.hex'
        timeoutSecs = 30
        parseResult  = h2i.import_parse(bucket='smalldata', path=csvPathname_train, schema='put', hex_key=hex_key, timeoutSecs=timeoutSecs)
        parseResultV = h2i.import_parse(bucket='smalldata', path=csvPathname_test, schema='put', hex_key=validation_key, timeoutSecs=timeoutSecs)

        inspect = h2o_cmd.runInspect(None, hex_key)
        print "\n" + csvPathname_train, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])
        response = inspect['numCols'] - 1

        #Making random id
        identifier = ''.join(random.sample(string.ascii_lowercase + string.digits, 10))
        model_key = 'nn_' + identifier + '.hex'

        kwargs = {
            'ignored_cols'                 : None,
            'response'                     : response,
            'classification'               : 1,
            'activation'                   : 'RectifierWithDropout',
            'input_dropout_ratio'          : 0.2,
            'hidden'                       : '117,131,129',
            'adaptive_rate'                : 0,
            'rate'                         : 0.005,
            'rate_annealing'               : 1e-6,
            'momentum_start'               : 0.5,
            'momentum_ramp'                : 100000,
            'momentum_stable'              : 0.9,
            'l1'                           : 0.00001,
            'l2'                           : 0.0000001,
            'seed'                         : 98037452452,
            'loss'                         : 'CrossEntropy',
            'max_w2'                       : 15,
            'initial_weight_distribution'  : 'UniformAdaptive',
            #'initial_weight_scale'         : 0.01,
            'epochs'                       : 2.0,
            'destination_key'              : model_key,
            'validation'                   : validation_key,
            'score_interval'               : 10000
            }
        expectedErr = 0.057 ## expected validation error for the above model
        relTol = 0.20 ## 20% rel. error tolerance due to Hogwild!

        timeoutSecs = 600
        start = time.time()
        nn = h2o_cmd.runDeepLearning(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "neural net end on ", csvPathname_train, " and ", csvPathname_test, 'took', time.time() - start, 'seconds'

        predict_key = 'score_' + identifier + '.hex'

        kwargs = {
            'data_key': validation_key,
            'destination_key': predict_key,
            'model_key': model_key
            }

        h2o.beta_features = True
        predictResult = h2o_cmd.runPredict(timeoutSecs=timeoutSecs, **kwargs)

        h2o_cmd.runInspect(key=predict_key, verbose=True)

        kwargs = {
        }

        predictCMResult = h2o.nodes[0].predict_confusion_matrix(
            actual=validation_key,
            vactual=response,
            predict=predict_key,
            vpredict='predict',
            timeoutSecs=timeoutSecs, **kwargs)

        cm = predictCMResult['cm']

        print h2o_gbm.pp_cm(cm)
        actualErr = h2o_gbm.pp_cm_summary(cm)/100.;

        print "actual   classification error:" + format(actualErr)
        print "expected classification error:" + format(expectedErr)
        if actualErr != expectedErr and abs((expectedErr - actualErr)/expectedErr) > relTol:
            raise Exception("Scored classification error of %s is not within %s %% relative error of %s" %
                            (actualErr, float(relTol)*100, expectedErr))

        h2o.beta_features = False

if __name__ == '__main__':
    h2o.unit_main()
