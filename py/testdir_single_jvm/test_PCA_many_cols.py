import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_pca, h2o_jobs as h2j


DO_PCA_SCORE = True
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1)
            # put every column in twice, to create dependent cols!
            # the extra cols will have 0 for pca component values
            # add a little noise to them, to see if PCA sees it's mostly the same
            rowData.append(ri)
            # duplicate, but flip it 1 out of 10
            rj = r1.randint(0,10)
            if rj==0:
                # flip it (0/1)
                ri = 0 if ri else 1
            rowData.append(ri)

        # PCA doesn't use/need an output
        # ri = r1.randint(0,1)
        # rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

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
            h2o.build_cloud(1,java_heap_GB=10, enable_benchmark_log=True)
        else:
            h2o_hosts.build_cloud_with_hosts(enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_PCA_many_cols(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        if localhost:
            tryList = [
                (10000, 10, 'cA', 300), 
                (10000, 50, 'cB', 300), 
                (10000, 100, 'cC', 300), 
                # (10000, 500, 'cH', 300), 
                # (10000, 1000, 'cI', 300), 
                ]
        else:
            tryList = [
                # (10000, 10, 'cB', 300), 
                # (10000, 50, 'cC', 300), 
                (10000, 100, 'cD', 300), 
                (10000, 200, 'cE', 300), 
                (10000, 300, 'cF', 300), 
                (10000, 400, 'cG', 300), 
                (10000, 500, 'cH', 300), 
                (10000, 1000, 'cI', 300), 
                ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            print (rowCount, colCount, hex_key, timeoutSecs)
            SEEDPERFILE = random.randint(0, sys.maxint)
            # csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

            # PARSE ****************************************
            h2o.beta_features = False #turn off beta_features
            start = time.time()

            #h2o.beta_features = False
            modelKey = 'PCAModelKey'
            scoreKey = 'PCAScoreKey'

            # Parse ****************************************
            if h2o.beta_features:
                print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            parseResult = h2i.import_parse(bucket=None, path=csvPathname, schema='put',
                hex_key=hex_key, timeoutSecs=timeoutSecs, noPoll=h2o.beta_features, doSummary=False)
            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                print "Filling in the parseResult['destination_key'] for h2o"
                parseResult['destination_key'] = trainKey

            elapsed = time.time() - start
            print "parse end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # Logging to a benchmark file
            algo = "Parse"
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

            # if you set beta_features here, the fvec translate will happen with the Inspect not the PCA
            # h2o.beta_features = True
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']

            # PCA(tolerance iterate)****************************************
            h2o.beta_features = True
            for tolerance in [i/10.0 for i in range(11)]:
                params = {
                    'destination_key': modelKey,
                    'tolerance': tolerance,
                    'standardize': 1,
                }
                kwargs = params.copy()
                h2o.beta_features = True
                PCAResult = {'python_elapsed': 0, 'python_%timeout': 0}
                start = time.time()
                h2o_cmd.runPCA(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
                h2j.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=120, retryDelaySecs=2)
                elapsed = time.time() - start
                PCAResult['python_elapsed']  = elapsed
                PCAResult['python_%timeout'] = 1.0*elapsed / timeoutSecs
                print "PCA completed in",     PCAResult['python_elapsed'], "seconds.", \
                      "%f pct. of timeout" % (PCAResult['python_%timeout'])
                
                pcaView = h2o_cmd.runPCAView(modelKey=modelKey)
                h2o_pca.simpleCheckPCA(self,pcaView)
                h2o_pca.resultsCheckPCA(self,pcaView)

                # Logging to a benchmark file
                algo = "PCA " + " tolerance=" + str(tolerance)
                l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, PCAResult['python_elapsed'])
                print l
                h2o.cloudPerfH2O.message(l)

                #h2o.beta_features = True
                pcaInspect = pcaView
                # errrs from end of list? is that the last tree?
                sdevs = pcaInspect["pca_model"]["sdev"] 
                print "PCA: standard deviations are :", sdevs
                print
                print
                propVars = pcaInspect["pca_model"]["propVar"]
                print "PCA: Proportions of variance by eigenvector are :", propVars
                print
                print
                #h2o.beta_features=False
                print
                print
                print 
                num_pc = pcaInspect['pca_model']['num_pc']
                print "The number of standard deviations obtained: ", num_pc
                print 
                print
                print


                if DO_PCA_SCORE:
                    # just score with same data
                    score_params = {
                        'destination_key': scoreKey,
                        'model': modelKey,
                        'num_pc': num_pc,
                        'source':  hex_key,
                    }
                    kwargs = score_params.copy()
                    pcaScoreResult = h2o.nodes[0].pca_score(timeoutSecs=timeoutSecs, noPoll=True, **kwargs)
                    h2j.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=120, retryDelaySecs=2)
                    print "PCAScore completed in", pcaScoreResult['python_elapsed'], "seconds. On dataset: ", csvPathname
                    print "Elapsed time was ", pcaScoreResult['python_%timeout'], "% of the timeout"

                    # Logging to a benchmark file
                    algo = "PCAScore " + " num_pc=" + str(score_params['num_pc'])
                    l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                        len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, pcaScoreResult['python_elapsed'])
                    print l
                    h2o.cloudPerfH2O.message(l)

if __name__ == '__main__':
    h2o.unit_main()
