import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_glm, h2o_common
import h2o_print

DO_GLM = True
LOG_MACHINE_STATS = False

print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):
    def sub_c2_fvec_long(self):
        # a kludge
        h2o.setup_benchmark_log()

        avgMichalSize = 116561140 
        bucket = 'home-0xdiag-datasets'
        ### importFolderPath = 'more1_1200_link'
        importFolderPath = 'manyfiles-nflx-gz'
        print "Using .gz'ed files in", importFolderPath
        if len(h2o.nodes)==1:
            csvFilenameList= [
                ("*[1][0][0-9].dat.gz", "file_10_A.dat.gz", 10 * avgMichalSize, 600),
            ]
        else:
            csvFilenameList= [
                ("*[1][0-4][0-9].dat.gz", "file_50_A.dat.gz", 50 * avgMichalSize, 1800),
                # ("*[1][0-9][0-9].dat.gz", "file_100_A.dat.gz", 100 * avgMichalSize, 3600),
            ]

        if LOG_MACHINE_STATS:
            benchmarkLogging = ['cpu', 'disk', 'network']
        else:
            benchmarkLogging = []

        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for trial, (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
                csvPathname = importFolderPath + "/" + csvFilepattern

                (importResult, importPattern) = h2i.import_only(bucket=bucket, path=csvPathname, schema='local')
                importFullList = importResult['files']
                importFailList = importResult['fails']
                print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)

                # this accumulates performance stats into a benchmark log over multiple runs 
                # good for tracking whether we're getting slower or faster
                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")

                start = time.time()
                parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                    hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    benchmarkLogging=benchmarkLogging)
                elapsed = time.time() - start
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                print "Parse result['destination_key']:", parseResult['destination_key']
                h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

                if totalBytes is not None:
                    fileMBS = (totalBytes/1e6)/elapsed
                    msg = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, fileMBS, elapsed)
                    print msg
                    h2o.cloudPerfH2O.message(msg)

                if DO_GLM:
                    # these are all the columns that are enums in the dataset...too many for GLM!
                    x = range(542) # don't include the output column
                    # remove the output too! (378)
                    ignore_x = []
                    for i in [3,4,5,6,7,8,9,10,11,14,16,17,18,19,20,424,425,426,540,541]:
                        x.remove(i)
                        ignore_x.append(i)

                    # plus 1 because we are no longer 0 offset
                    x = ",".join(map(lambda x: "C" + str(x+1), x))
                    ignore_x = ",".join(map(lambda x: "C" + str(x+1), ignore_x))

                    GLMkwargs = {
                        'ignored_cols': ignore_x, 
                        'family': 'binomial',
                        'response': 'C379', 
                        'max_iter': 4, 
                        'n_folds': 1, 
                        'family': 'binomial',
                        'alpha': 0.2, 
                        'lambda': 1e-5
                    }

                    # convert to binomial
                    execExpr="A.hex=%s" % parseResult['destination_key']
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=60)
                    execExpr="A.hex[,%s]=(A.hex[,%s]>%s)" % ('C379', 'C379', 15)
                    h2e.exec_expr(execExpr=execExpr, timeoutSecs=60)
                    aHack = {'destination_key': "A.hex"}

                    start = time.time()
                    glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **GLMkwargs)
                    elapsed = time.time() - start
                    h2o.check_sandbox_for_errors()

                    h2o_glm.simpleCheckGLM(self, glm, None, **GLMkwargs)
                    msg = '{:d} jvms, {:d}GB heap, {:s} {:s} GLM: {:6.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, csvFilepattern, csvFilename, elapsed)
                    print msg
                    h2o.cloudPerfH2O.message(msg)

                h2o_cmd.checkKeyDistribution()

    #***********************************************************************
    # these will be tracked individual by jenkins, which is nice
    #***********************************************************************

    def test_A_c2_fvec_short(self):
        h2o.beta_features = True
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_B_c2_fvec_long(self):
        h2o.beta_features = True
        self.sub_c2_fvec_long()


if __name__ == '__main__':
    h2o.unit_main()
