#TODO: Debug & Examples
#TODO: Add test name to the jvm_output_file name

from h2oPerf.Table import *
from h2oPerf import PerfUtils
from h2oPerf.Runner import *

import sys
import os
import argparse
import signal
import time

def parse_args():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--wipe", action = 'store_true', help = "Wipes the output dir before starting.")
        parser.add_argument("--baseport", help = "The first port at which H2O starts searching for free ports.")
        parser.add_argument("--test", help = "If you only want to run one test, specify it like this.")
        parser.add_argument("--testlist", help = "A file containing a list of tests to run (for example the 'failed.txt' file from the output directory).")
        parser.add_argument("--testgroup", help = "Test a group of tests by function: pca, glm, kmeans, gbm, rf, algos, golden, munging")
        parser.add_argument("--usecloud", help = "ip:port of cloud to send tests to instead of starting clouds.")
        parser.add_argument("--testname", help = "Test a single test name.")
        args = ""
        args = vars(parser.parse_args())
    except:
        parser.print_help()
        sys.exit(1)
    return args

def main(argv):
    script_name = os.path.basename(argv[0])
    test_root_output_dir = os.path.dirname(os.path.realpath(__file__))
    
    output_dir = os.path.join(test_root_output_dir, "results")
    
    test_root_dir = os.path.join(
                        os.path.dirname(test_root_output_dir), "tests")

    h2o_jar = os.path.abspath(
                  os.path.join(test_root_dir, "..", "..", "perf-target", "h2o.jar"))

    if not os.path.exists(h2o_jar):
        print("")
        print("ERROR: H2O jar not found: " + h2o_jar)
        print("")
        sys.exit(1)
  
    #parse cmdline arguments
    args = parse_args()
    if args['wipe']:
        PerfUtils.wipe_output_dir(output_dir)

    #if True:
    #    try:
    #        cmd = ["R",
    #               "--quiet",
    #               "-f",
    #               os.path.join("../../../R/tests", "Utils/runnerSetupPackage.R"),
    #               "--args",
    #               "127.0.0.1:" + str(port)]
    #        child = subprocess.Popen(args=cmd)
    #        rv = child.wait()
    #    except: pass

    #new perfdb connection
    perfdb = PerfDB()
    test_to_run = ""

    if args['testname']:
        test_to_run = args['testname']

    perf_runner = PerfRunner(test_root_dir, output_dir, h2o_jar, perfdb)
    perf_runner.build_test_list(test_to_run)
    
    signal.signal(signal.SIGINT, PerfUtils.signal_handler)
    signal.signal(signal.SIGTERM, PerfUtils.signal_handler)

    #run tests
    perf_runner.run_tests()
    #report summary

if __name__ == "__main__":
    main(sys.argv)
