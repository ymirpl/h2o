import unittest, time, sys, time, itertools
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

def file_to_put():
    # kbn fails 10/15/12
    # return 'smalldata/poker/poker-hand-testing.data'
    a = h2i.find_folder_and_filename('smalldata', 'poker/poker1000', schema='put', returnFullPath=True)
    print "\nfind_folder_and_filename:", a
    return a

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.verboseprint("Tearing down cloud")
        h2o.tear_down_cloud()

    # Try to put a file to each node in the cloud and checked reported size of the saved file 
    def test_A_putfile_to_all_nodes(self):
        csvfile  = file_to_put()
        print "csvfile:", csvfile
        origSize = h2o.get_file_size(csvfile)

        # Putfile to each node and check the returned size
        for node in h2o.nodes:
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.verboseprint("put_file:", csvfile, "node:", node, "origSize:", origSize)
            key        = node.put_file(csvfile)
            resultSize = node.inspect(key)['value_size_bytes']
            self.assertEqual(origSize,resultSize)

    # Try to put a file, get file and diff original file and returned file.
    def test_B_putfile_and_getfile_to_all_nodes(self):
        csvfile = file_to_put()
        nodeTry = 0
        for node in h2o.nodes:
            sys.stdout.write('.')
            sys.stdout.flush()
            h2o.verboseprint("put_file", csvfile, "to", node)
            key = node.put_file(csvfile)
            h2o.verboseprint("put_file ok for node", nodeTry)
            print "starting get_key..this is the same as the original source?"
            r      = node.get_key(key)
            f      = open(csvfile)
            self.diff(r, f)
            h2o.verboseprint("put_file filesize ok")
            f.close()
            nodeTry += 1

    def diff(self,r, f):
        h2o.verboseprint("checking r and f:", r, f)
        for (r_chunk,f_chunk) in itertools.izip(r.iter_content(1024), h2o.iter_chunked_file(f, 1024)):
            # print "\nr_chunk:", r_chunk, 
            # print "\nf_chunk:", f_chunk
            self.assertEqual(r_chunk,f_chunk)


if __name__ == '__main__':
    h2o.unit_main()

