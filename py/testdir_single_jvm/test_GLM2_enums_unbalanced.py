import unittest, random, sys, time, re, math
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_jobs as h2j, h2o_gbm

# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

ONE_RATIO = 100
ENUM_RANGE = 20
def create_enum_list(randChars="abcd", maxEnumSize=8, listSize=ENUM_RANGE):
    # okay to have duplicates?
    enumList = [random_enum(randChars, random.randint(2,maxEnumSize)) for i in range(listSize)]
    return enumList


def write_syn_dataset(csvPathname, enumList, rowCount, colCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n"):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        rowData = []
        for col in range(colCount):
            ri = random.choice(enumList)
            rowData.append(ri)

        # output column
        # ri = r1.randint(0,1)
        # skew the binomial 0,1 distribution. (by rounding to 0 or 1
        # ri = round(r1.triangular(0,1,0.3), 0)
        # just put a 1 in every 100th row
        if (row % ONE_RATIO)==0:
            ri = 1
        else:
            ri = 0
        rowData.append(ri)

        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        dsf.write(rowDataCsv)
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_enums_unbalanced(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 2000
        tryList = [
            (n, 1, 'cD', 300), 
            (n, 2, 'cE', 300), 
            (n, 4, 'cF', 300), 
            (n, 8, 'cG', 300), 
            (n, 16, 'cH', 300), 
            (n, 32, 'cI', 300), 
            ]

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            # using the comma is nice to ensure no craziness
            colSepHexString = '2c' # comma
            colSepChar = colSepHexString.decode('hex')
            colSepInt = int(colSepHexString, base=16)
            print "colSepChar:", colSepChar

            rowSepHexString = '0a' # newline
            rowSepChar = rowSepHexString.decode('hex')
            print "rowSepChar:", rowSepChar

            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_enums_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            csvScoreFilename = 'syn_enums_score_' + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvScorePathname = SYNDATASETS_DIR + '/' + csvScoreFilename

            enumList = create_enum_list(listSize=10)
            # use half of the enums for creating the scoring dataset
            enumListForScore = random.sample(enumList,5)

            print "Creating random", csvPathname, "for glm2 model building"
            write_syn_dataset(csvPathname, enumList, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            print "Creating another random", csvScorePathname, "for glm2 scoring with prior model (using enum subset)"
            write_syn_dataset(csvScorePathname, enumListForScore, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, 
                timeoutSecs=30, separator=colSepInt)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)

            testDataKey = "score_" + hex_key
            parseResult = h2i.import_parse(path=csvScorePathname, schema='put', hex_key=testDataKey,
                timeoutSecs=30, separator=colSepInt)

            y = colCount
            modelKey = 'glm_model'
            kwargs = {
                'standardize': 0,
                'destination_key': modelKey,
                'response': 'C' + str(y+1), 
                'max_iter': 200, 
                'family': 'binomial',
                'n_folds': 0, 
                'alpha': 0, 
                'lambda': 0, 
                }

            start = time.time()

            updateList= [ 
                {'alpha': 0.5, 'lambda': 1e-4},
                {'alpha': 0.25, 'lambda': 1e-6},
                {'alpha': 0.0, 'lambda': 1e-12},
                {'alpha': 0.5, 'lambda': 1e-12},
                {'alpha': 0.0, 'lambda': 1e-12},
                {'alpha': 0.0, 'lambda': 0},
            ]

            # Try each one
            h2o.beta_features = True
            for updateDict in updateList:
                print "\n#################################################################"
                print updateDict
                kwargs.update(updateDict)
                print "If we poll, we get a message saying it was cancelled by user??"
                glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, noPoll=True, **kwargs)
                h2j.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5, errorIfCancelled=True)
                glm = h2o.nodes[0].glm_view(_modelKey=modelKey)
                print "glm2 end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'

                glm_model = glm['glm_model']
                _names = glm_model['_names']
                modelKey = glm_model['_key']
                coefficients_names = glm_model['coefficients_names']
                submodels = glm_model['submodels'][0]

                beta = submodels['beta']
                norm_beta = submodels['norm_beta']
                iteration = submodels['iteration']

                validation = submodels['validation']

                if not validation or 'avg_err' not in validation:
                    raise Exception("glm: %s" % h2o.dump_json(glm) + \
                        "\nNo avg_err in validation." + \
                        "\nLikely if you look back, the job was cancelled, so there's no cross validation.")
        
                avg_err = validation['avg_err']
                auc = validation['auc']
                aic = validation['aic']
                null_deviance = validation['null_deviance']
                residual_deviance = validation['residual_deviance']

                print '_names', _names
                print 'coefficients_names', coefficients_names
                # did beta get shortened? the simple check confirms names/beta/norm_beta are same length
                print 'beta', beta
                print 'iteration', iteration
                print 'avg_err', avg_err
                print 'auc', auc

                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
                if iteration > 20:
                    raise Exception("Why take so many iterations:  %s in this glm2 training?" % iterations)

               # Score **********************************************
                print "Problems with test data having different enums than train? just use train for now"
                testDataKey = hex_key
                predictKey = 'Predict.hex'
                start = time.time()

                predictResult = h2o_cmd.runPredict(
                    data_key=testDataKey,
                    model_key=modelKey,
                    destination_key=predictKey,
                    timeoutSecs=timeoutSecs)

                predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                    actual=testDataKey,
                    vactual='C' + str(y),
                    predict=predictKey,
                    vpredict='predict',
                    )

                cm = predictCMResult['cm']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                self.assertLess(pctWrong, 8,"Should see less than 7 pct error (class = 4): %s" % pctWrong)

                print "\nTest\n==========\n"
                print h2o_gbm.pp_cm(cm)


                if 1==0:
                    # stuff from GLM1

                    classErr = glmScore['validation']['classErr']
                    auc = glmScore['validation']['auc']
                    err = glmScore['validation']['err']
                    nullDev = glmScore['validation']['nullDev']
                    resDev = glmScore['validation']['resDev']
                    h2o_glm.simpleCheckGLMScore(self, glmScore, **kwargs)

                    print "score classErr:", classErr
                    print "score err:", err
                    print "score auc:", auc
                    print "score resDev:", resDev
                    print "score nullDev:", nullDev

                    if math.isnan(resDev):
                        emsg = "Why is this resDev = 'nan'?? %6s %s" % ("resDev:\t", validation['resDev'])
                        raise Exception(emsg)

                    # what is reasonable?
                    # self.assertAlmostEqual(err, 0.3, delta=0.15, msg="actual err: %s not close enough to 0.3" % err)
                    self.assertAlmostEqual(auc, 0.5, delta=0.15, msg="actual auc: %s not close enough to 0.5" % auc)

                    if math.isnan(err):
                        emsg = "Why is this err = 'nan'?? %6s %s" % ("err:\t", err)
                        raise Exception(emsg)

                    if math.isnan(resDev):
                        emsg = "Why is this resDev = 'nan'?? %6s %s" % ("resDev:\t", resDev)
                        raise Exception(emsg)

                    if math.isnan(nullDev):
                        emsg = "Why is this nullDev = 'nan'?? %6s %s" % ("nullDev:\t", nullDev)

if __name__ == '__main__':
    h2o.unit_main()
