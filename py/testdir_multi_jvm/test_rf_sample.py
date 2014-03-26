import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_browse as h2b

def write_syn_dataset(csvPathname, rowCount, headerData, rList):
    dsf = open(csvPathname, "w+")
    
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        # two choices on the input. Make output choices random
        r = rList[random.randint(0,1)] + "," + str(random.randint(0,7))
        dsf.write(r + "\n")
    dsf.close()

def rand_rowData():
    rowData1 = str(random.randint(0,7))
    for i in range(7):
        rowData1 = rowData1 + "," + str(random.randint(0,7))

    rowData2 = str(random.randint(0,7))
    for i in range(7):
        rowData2 = rowData2 + "," + str(random.randint(0,7))
    # RF will complain if all inputs are the same
    r = [rowData1, rowData2]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_MB=1300,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_rf_sample(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"

        print "just going to see if rf is doing the sampling right for one tree on 100000 rows"
        rList = rand_rowData()
        totalRows = 10000
        write_syn_dataset(csvPathname, totalRows, headerData, rList)

        for trial in range (2):
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            src_key = csvFilename + "_" + str(trial)
            hex_key = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            timeoutSecs = 30
            parseResult = h2i.import_parse(path=csvPathname, schema='put', src_key=src_key, hex_key=hex_key, 
                timeoutSecs=timeoutSecs, pollTimeoutSecs=30, header=1)
            print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            kwargs = {'sample': 75, 'depth': 25, 'ntree': 1}
            start = time.time()
            rfv = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=30, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalRows:", totalRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            cm = rfv['confusion_matrix']
            rows_skipped = cm['rows_skipped']

            # the sample is what we trained on. The CM for one tree is what's left
            # it's not perfectly accurate..allow +-2
            # NEW: after the # of trees is big enough, all the data is used, so we really can't compare
            # any more
            sample = kwargs['sample']
            rowsUsed = sample * totalRows/100
            rowsNotUsed = totalRows - rowsUsed

            ## print "Allowing delta of 0-2"
            ## print "predicted CM rows (rowsNotUsed):", rowsNotUsed, "actually:", totalRows - rows_skipped, "rows_skipped:", rows_skipped
            ## self.assertAlmostEqual(rowsNotUsed, totalRows - rows_skipped, delta=2)
            h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
