import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_summary_manyfiles_s3n(self):
        # these will be used as directory imports/parse
        csvDirlist = [
            ("manyfiles-nflx-gz",   600),
        ]
        trial = 0
        for (csvDirname, timeoutSecs) in csvDirlist:

            csvPathname = csvDirname + "/file_[2][0-9][0-9].dat.gz"
            (importHDFSResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, schema='s3n', timeoutSecs=timeoutSecs)
            s3nFullList = importHDFSResult['succeeded']
            self.assertGreater(len(s3nFullList),1,"Should see more than 1 files in s3n?")

            print "\nTrying StoreView after the import hdfs"
            h2o_cmd.runStoreView(timeoutSecs=120)

            trialStart = time.time()
            # PARSE****************************************
            hex_key = csvDirname + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120)
            elapsed = time.time() - start
            print "parse end on ", parseResult['destination_key'], 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # INSPECT******************************************
            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            # gives us some reporting on missing values, constant values, to see if we have x specified well
            # figures out everything from parseResult['destination_key']
            # needs y to avoid output column (which can be index or name)
            # assume all the configs have the same y..just check with the firs tone
            goodX = h2o_glm.goodXFromColumnInfo(y=54, key=parseResult['destination_key'], timeoutSecs=300)

            # SUMMARY****************************************
            summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=360)
            h2o_cmd.infoFromSummary(summaryResult)

            # STOREVIEW***************************************
            print "\nTrying StoreView after the parse"
            h2o_cmd.runStoreView(timeoutSecs=120)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
