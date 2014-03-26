import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i
import h2o_browse as h2b
import h2o_exec as h2e

print "colswap doesn't seem like it can update a col with NAs correctly."
print "smalldata/tnc3.csv was modified all missing entries replaced with <dquote>-1<dquote>." 
print "then all starting quotes are replaced with <dquote><space>"
print "so quoted numbers get treated as strings by parser"
print "But that leaves NAs in the number cols?"
numExprList = [
        # 'tnc3.hex = colSwap(tnc3.hex,<col1>,tnc3.hex[<col1>] + 0)',
        # 'tnc3.hex = colSwap(tnc3.hex,<col1>,(tnc3.hex[<col1>]==0 ? 9999 : tnc3.hex[<col1>]))',
        # 'tnc3.hex = colSwap( tnc3.hex, <col1>, (tnc3.hex[<col1>] + 1) )',
        'tnc3.hex = colSwap(tnc3.hex,<col1>,(tnc3.hex[<col1>]==-1 ? 1 : tnc3.hex[<col1>]))',
    ]

charExprList = [
        'tnc3.hex = colSwap(tnc3.hex,<col1>,(tnc3.hex[<col1>]==-1 ? "na" : tnc3.hex[<col1>]))',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_tnc3_10(self):
        csvFilename = 'tnc3_10.csv'
        print "\n" + csvFilename
        hex_key = "tnc3.hex"
        h2b.browseTheCloud()

        parseResult = h2i.import_parse(bucket='smalldata', path=csvFilename, schema='put', hex_key=hex_key, timeoutSecs=10)
        print "Parse result['Key']:", parseResult['destination_key']
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### time.sleep(10)

        if (1==0):
            lenNodes = len(h2o.nodes)
            colResultList = h2e.exec_expr_list_across_cols(lenNodes, numExprList, hex_key, maxCol=10,
                incrementingResult=False, timeoutSecs=10)
            print "\ncolResultList after num swap", colResultList

        if (1==1):
            start = time.time()
            kwargs = {'y': 13, 'n_folds': 6}
            # hmm. maybe we should update to use key as input
            # in case exec is used to change the parseResult
            # in any case, the destination_key in parseResult was what was updated
            # so if we Exec, it's correct.
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=300, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end on ", csvFilename, 'took', time.time() - start, 'seconds'


        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### time.sleep(3600)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        #******************
        if (1==0):
            colResultList = h2e.exec_expr_list_across_cols(lenNodes, charExprList, hex_key, maxCol=10,
                incrementingResult=False, timeoutSecs=10)
            print "\ncolResultList after char swap", colResultList

        if (1==1):
            start = time.time()
            kwargs = {'y': 13, 'n_folds': 6}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=300, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end on ", csvFilename, 'took', time.time() - start, 'seconds'

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        ### time.sleep(3600)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        if not h2o.browse_disable:
            ### print "\n <ctrl-C> to quit sleeping here"
            ### time.sleep(1500)
            pass

if __name__ == '__main__':
    h2o.unit_main()
