import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_browse as h2b

print "h2o should automatically deduce the header for this guy"
print "should probably check the names too ..right now failing on rows"
# 'comment, is okay
# "this comment, is okay too
# 'this' comment, is okay too
datalines = """' FirstName '' Middle Initials '' LastName '' Date of Birth  '
'0''0.5''1''0'
'3''0''4''1'
'0.6''0.7''0.8''1'
'+0.6''+0.7''+0.8''0'
'-0.6''-0.7''-0.8''1'
'.6''.7''.8''0'
'+.6''+.7''+.8''1'
'-.6''-.7''-.8''0'
'+0.6e0''+0.7e0''+0.8e0''1'
'-0.6e0''-0.7e0''-0.8e0''0'
'.6e0''.7e0''.8e0''1'
'+.6e0''+.7e0''+.8e0''0'
'-.6e0''-.7e0''-.8e0''1'
'+0.6e00''+0.7e00''+0.8e00''0'
'-0.6e00''-0.7e00''-0.8e00''1'
'.6e00''.7e00''.8e00''0'
'+.6e00''+.7e00''+.8e00''1'
'-.6e00''-.7e00''-.8e00''0'
'+0.6e-01''+0.7e-01''+0.8e-01''1'
'-0.6e-01''-0.7e-01''-0.8e-01''0'
'.6e-01''.7e-01''.8e-01''1'
'+.6e-01''+.7e-01''+.8e-01''0'
'-.6e-01''-.7e-01''-.8e-01''1'
'+0.6e+01''+0.7e+01''+0.8e+01''0'
'-0.6e+01''-0.7e+01''-0.8e+01''1'
'.6e+01''.7e+01''.8e+01''0'
'+.6e+01''+.7e+01''+.8e+01''1'
'-.6e+01''-.7e+01''-.8e+01''0'
'6''''8''0'
"""


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

    def test_parse_bad_30rows(self):
        # h2b.browseTheCloud()
        h2o.beta_features = False
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvPathname = SYNDATASETS_DIR + '/bad.data'
        dsf = open(csvPathname, "w+")
        dsf.write(datalines)
        dsf.close()

        for i in range(20):
            # every other one
            single_quotes = 1

            parseResult = h2i.import_parse(path=csvPathname, schema='put', single_quotes=single_quotes,
                hex_key="trial" + str(i) + ".hex")
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']
            self.assertEqual(num_cols, 4, "Parsed wrong number of cols: %s" % num_cols)
            self.assertNotEqual(num_rows, 30, "Parsed wrong number of rows. Should be 29.\
                 Didn't deduce header?: %s" % num_rows)
            self.assertEqual(num_rows, 29, "Parsed wrong number of rows: %s" % num_rows)


if __name__ == '__main__':
    h2o.unit_main()
                        
