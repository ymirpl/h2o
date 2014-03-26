import unittest, random, sys, time, string
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_pca, h2o_jobs as h2j


def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # do we need more than one random generator?
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            if j%2==0:
                ri1 = int(r1.triangular(1,5,2.5))
            else:
                # odd lines get enums
                # hack to get lots of enumsrandom length 16 in odd cols
                ri1 = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(16))
            rowData.append(ri1)

        # don't need an output col
        rowDataStr = map(str,rowData)
        rowDataCsv = ",".join(rowDataStr)
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

    def test_PCA_many_cols_enum_fvec(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']

        tryList = [
            # (10000, 10, 'cB', 300), 
            # (10000, 50, 'cC', 300), 
            (100, 100, 'cD', 300), 
            (100, 200, 'cE', 300), 
            (100, 300, 'cF', 300), 
            (100, 400, 'cG', 300), 
            (100, 500, 'cH', 300), 
            (100, 1000, 'cI', 300), 
            ]

        ### h2b.browseTheCloud()
        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            print "Creating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, translateList)

            # PARSE ****************************************
            start = time.time()
            modelKey = 'PCAModelKey'

            # Parse ****************************************
            parseResult = h2i.import_parse(bucket=None, path=csvPathname, schema='put',
                hex_key=hex_key, timeoutSecs=timeoutSecs, doSummary=False)

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

            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            # PCA(tolerance iterate)****************************************
            for tolerance in [i/10.0 for i in range(11)]:
                params = {
                    'ignored_cols': 'C1',
                    'destination_key': modelKey,
                    'tolerance': tolerance,
                    'standardize': 1,
                }
                print "Using these parameters for PCA: ", params
                kwargs = params.copy()
                PCAResult = {'python_elapsed': 0, 'python_%timeout': 0}
                start = time.time()
                pcaResult = h2o_cmd.runPCA(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                elapsed = time.time() - start
                PCAResult['python_elapsed']  = elapsed
                PCAResult['python_%timeout'] = 1.0*elapsed / timeoutSecs
                print "PCA completed in",     PCAResult['python_elapsed'], "seconds.", \
                      "%f pct. of timeout" % (PCAResult['python_%timeout'])            
    
                print "Checking PCA results: "
                pcaView = h2o_cmd.runPCAView(modelKey = modelKey) 
                h2o_pca.simpleCheckPCA(self,pcaView)
                h2o_pca.resultsCheckPCA(self,pcaView)

                # Logging to a benchmark file
                algo = "PCA " + " tolerance=" + str(tolerance)
                l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, PCAResult['python_elapsed'])
                print l
                h2o.cloudPerfH2O.message(l)
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

if __name__ == '__main__':
    h2o.unit_main()
