import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,
                use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')
        else:
            h2o_hosts.build_cloud_with_hosts(
                use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_hdfs_files(self):
        # larger set in my local dir
        # fails because classes aren't integers
        #    "allstate_claim_prediction_train_set.zip",
        csvFilenameAll = [
            "3G_poker_shuffle",
            "TEST-poker1000.csv",
            # corrupt zip file?
            # "allstate_claim_prediction_train_set.zip",
            "and-testing.data",
            "arcene2_train.both",
            "arcene_train.both",
            "bestbuy_test.csv",
            "bestbuy_train.csv",
            "billion_rows.csv.gz",
            "covtype.13x.data",
            "covtype.13x.shuffle.data",
            "covtype.169x.data",
            "covtype.4x.shuffle.data",
            "covtype.data",
            "covtype4x.shuffle.data",
            "hhp.unbalanced.012.1x11.data.gz",
            "hhp.unbalanced.012.data.gz",
            "hhp.unbalanced.data.gz",
            "hhp2.os.noisy.0_1.data",
            "hhp2.os.noisy.9_4.data",
            "hhp_9_14_12.data",
            "leads.csv",
            "prostate_long_1G.csv",
        ]

        # pick 8 randomly!
        if (1==0):
            csvFilenameList = random.sample(csvFilenameAll,8)
        # Alternatively: do the list in order! Note the order is easy to hard
        else:
            csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        h2b.browseTheCloud()

        timeoutSecs = 1000
        # save the first, for all comparisions, to avoid slow drift with each iteration
        firstglm = {}

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in hdfs dir 
            start = time.time()
            print 'Parsing', csvFilename
            csvPathname = "datasets/" + csvFilename
            parseResult = h2i.import_parse(path=csvPathname, schema='hdfs', 
                timeoutSecs=timeoutSecs, retryDelaySecs=1.0)
            print csvFilename, '\nparse time (python)', time.time() - start, 'seconds'
            print csvFilename, '\nparse time (h2o):', parseResult['response']['time']
            ### print h2o.dump_json(parseResult['response'])

            print "parse result:", parseResult['destination_key']
            # I use this if i want the larger set in my localdir
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

            ### print h2o.dump_json(inspect)
            cols = inspect['cols']

            # look for nonzero num_missing_values count in each col
            for i, colDict in enumerate(cols):
                num_missing_values = colDict['num_missing_values']
                if num_missing_values != 0:
                    ### print "%s: col: %d, num_missing_values: %d" % (csvFilename, i, num_missing_values)
                    pass

            ### print h2o.dump_json(cols[0])

            num_cols = inspect['num_cols']
            num_rows = inspect['num_rows']
            row_size = inspect['row_size']
            ptype = inspect['type']
            value_size_bytes = inspect['value_size_bytes']
            response = inspect['response']
            ptime = response['time']

            print "num_cols: %s, num_rows: %s, row_size: %s, ptype: %s, \
                   value_size_bytes: %s, response: %s, time: %s" % \
                   (num_cols, num_rows, row_size, ptype, value_size_bytes, response, ptime)

            # h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            print "\n" + csvFilename

if __name__ == '__main__':
    h2o.unit_main()
