import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

zeroList = [
        'Result0 = 0',
]

exprList = [
        'Result<n>.hex = log(<keyX>[<col1>])',
        'Result<n>.hex = randomBitVector(19,0,123) + Result<n-1>.hex',
        'Result<n>.hex = randomFilter(<keyX>,<col1>,<row>)',
        'Result<n>.hex = factor(<keyX>[<col1>])',
        'Result<n>.hex = slice(<keyX>[<col1>],<row>)',
        'Result<n>.hex = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
        'Result<n>.hex = <keyX>[<col1>]',
        'Result<n>.hex = min(<keyX>[<col1>])',
        'Result<n>.hex = max(<keyX>[<col1>]) + Result<n-1>.hex',
        'Result<n>.hex = mean(<keyX>[<col1>]) + Result<n-1>.hex',
        'Result<n>.hex = sum(<keyX>[<col1>]) + Result.hex',
    ]

def exec_list(exprList, lenNodes, csvFilename, hex_key):
        h2e.exec_zero_list(zeroList)
        # start with trial = 1 because trial-1 is used to point to Result0 which must be initted
        trial = 1
        while (trial < 100):
            for exprTemplate in exprList:
                # do each expression at a random node, to facilate key movement
                nodeX = random.randint(0,lenNodes-1)
                colX = random.randint(1,54)
                # FIX! should tune this for covtype20x vs 200x vs covtype.data..but for now
                row = str(random.randint(1,400000))

                execExpr = h2e.fill_in_expr_template(exprTemplate, colX, trial, row, hex_key)
                execResultInspect = h2e.exec_expr(h2o.nodes[nodeX], execExpr, 
                    resultKey="Result"+str(trial)+".hex", timeoutSecs=60)

                eri0 = execResultInspect[0]
                eri1 = execResultInspect[1]
                columns = eri0.pop('cols')
                columnsDict = columns[0]
                print "\nexecResult columns[0]:", h2o.dump_json(columnsDict)
                print "\nexecResult [0]:", h2o.dump_json(eri0)
                print "\nexecResult [1] :", h2o.dump_json(eri1)
                
                min = columnsDict["min"]
                h2o.verboseprint("min: ", min, "trial:", trial)
                ### self.assertEqual(float(min), float(trial),"what can we check here")

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                # slows things down to check every iteration, but good for isolation
                h2o.check_sandbox_for_errors()
                print "Trial #", trial, "completed\n"
                trial += 1

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_import_hosts_bigfiles(self):
        # just do the import folder once
        timeoutSecs = 4000

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        # Update: need unique key names apparently. can't overwrite prior parse output key?
        # replicating lines means they'll get reparsed. good! (but give new key names)

        csvFilenameList = [
            ("covtype.data", "c"),
            ("covtype20x.data", "c20"),
            ("covtype200x.data", "c200"),
            ("billion_rows.csv.gz", "b"),
            ]

        # h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)
        importFolderPath = "standard"
        for (csvFilename, hex_key) in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, 
                timeoutSecs=2000)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename
            exec_list(exprList, lenNodes, csvFilename, hex_key)


if __name__ == '__main__':
    h2o.unit_main()
