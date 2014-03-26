import unittest, random, sys, time, re, math
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util

# use randChars for the random chars to use
def random_enum(randChars, maxEnumSize):
    choiceStr = randChars
    r = ''.join(random.choice(choiceStr) for x in range(maxEnumSize))
    return r

def create_enum_list(randChars="abcd", maxEnumSize=8, listSize=10):
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
        ri = round(r1.triangular(0,1,0.3), 0)
        rowData.append(ri)
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar

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

    def test_GLM_enums_score_superset(self):
        print "FIX!: this should cause an error. We should detect that it's not causing an error/warning?"
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 200
        tryList = [
            (n, 1, 'cD', 300), 
            (n, 2, 'cE', 300), 
            (n, 3, 'cF', 300), 
            (n, 4, 'cG', 300), 
            (n, 5, 'cH', 300), 
            (n, 6, 'cI', 300), 
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

            # add a extra enum for scoring that's not in the model enumList
            enumListForScore.append("xyzzy")

            print "Creating random", csvPathname, "for glm model building"
            write_syn_dataset(csvPathname, enumList, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            print "Creating random", csvScorePathname, "for glm scoring with prior model (using enum subset)"
            write_syn_dataset(csvScorePathname, enumListForScore, rowCount, colCount, SEEDPERFILE, 
                colSepChar=colSepChar, rowSepChar=rowSepChar)

            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key,
                timeoutSecs=30, separator=colSepInt)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)

            y = colCount
            kwargs = {'y': y, 'max_iter': 1, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5, 'family': 'binomial',
                'case_mode': '=', 'case': 0}
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutSecs=180, **kwargs)
            print "glm end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            GLMModel = glm['GLMModel']
            modelKey = GLMModel['model_key']

            parseResult = h2i.import_parse(path=csvScorePathname, schema='put', hex_key="score_" + hex_key, 
                timeoutSecs=30, separator=colSepInt)

            start = time.time()
            # score with same dataset (will change to recreated dataset with one less enum
            glmScore = h2o_cmd.runGLMScore(key=parseResult['destination_key'],
                model_key=modelKey, thresholds="0.5", timeoutSecs=timeoutSecs)
            print "glmScore end on ", parseResult['destination_key'], 'took', time.time() - start, 'seconds'
            ### print h2o.dump_json(glmScore)
            classErr = glmScore['validation']['classErr']
            auc = glmScore['validation']['auc']
            err = glmScore['validation']['err']
            resDev = glmScore['validation']['resDev']
            nullDev = glmScore['validation']['nullDev']
            print "classErr:", classErr
            print "err:", err
            print "auc:", auc
            print "resDev:", resDev
            print "nullDev:", nullDev
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
