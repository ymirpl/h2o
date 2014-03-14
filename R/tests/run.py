#!/usr/bin/python

import sys
import os
import shutil
import signal
import time
import random
import getpass
import re
import subprocess


class H2OUseCloudNode:
    """
    A class representing one node in an H2O cloud which was specified by the user.
    Don't try to build or tear down this kind of node.

    use_ip: The given ip of the cloud.
    use_port: The given port of the cloud.
    """

    def __init__(self, use_ip, use_port):
        self.use_ip = use_ip
        self.use_port = use_port

    def start(self):
        pass

    def stop(self):
        pass

    def terminate(self):
        pass

    def get_ip(self):
        return self.use_ip

    def get_port(self):
        return self.use_port


class H2OUseCloud:
    """
    A class representing an H2O clouds which was specified by the user.
    Don't try to build or tear down this kind of cloud.
    """

    def __init__(self, cloud_num, use_ip, use_port):
        self.cloud_num = cloud_num
        self.use_ip = use_ip
        self.use_port = use_port

        self.nodes = []
        node = H2OUseCloudNode(self.use_ip, self.use_port)
        self.nodes.append(node)

    def start(self):
        pass

    def wait_for_cloud_to_be_up(self):
        pass

    def stop(self):
        pass

    def terminate(self):
        pass

    def get_ip(self):
        node = self.nodes[0]
        return node.get_ip()

    def get_port(self):
        node = self.nodes[0]
        return node.get_port()


class H2OCloudNode:
    """
    A class representing one node in an H2O cloud.
    Note that the base_port is only a request for H2O.
    H2O may choose to ignore our request and pick any port it likes.
    So we have to scrape the real port number from stdout as part of cloud startup.

    port: The actual port chosen at run time.
    pid: The process id of the node.
    output_file_name: Where stdout and stderr go.  They are merged.
    child: subprocess.Popen object.
    terminated: Only from a signal.  Not normal shutdown.
    """

    def __init__(self, cloud_num, nodes_per_cloud, node_num, cloud_name, h2o_jar, ip, base_port, xmx, output_dir):
        """
        Create a node in a cloud.

        @param cloud_num: Dense 0-based cloud index number.
        @param nodes_per_cloud: How many H2O java instances are in a cloud.  Clouds are symmetric.
        @param node_num: This node's dense 0-based node index number.
        @param cloud_name: The H2O -name command-line argument.
        @param h2o_jar: Path to H2O jar file.
        @param base_port: The starting port number we are trying to get our nodes to listen on.
        @param xmx: Java memory parameter.
        @param output_dir: The directory where we can create an output file for this process.
        @return: The node object.
        """
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.node_num = node_num
        self.cloud_name = cloud_name
        self.h2o_jar = h2o_jar
        self.ip = ip
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        self.port = -1
        self.pid = -1
        self.output_file_name = ""
        self.child = None
        self.terminated = False

        # Choose my base port number here.  All math is done here.  Every node has the same
        # base_port and calculates it's own my_base_port.
        ports_per_node = 2
        self.my_base_port = \
            self.base_port + \
            (self.cloud_num * self.nodes_per_cloud * ports_per_node) + \
            (self.node_num * ports_per_node)

    def start(self):
        """
        Start one node of H2O.
        (Stash away the self.child and self.pid internally here.)

        @return: none
        """

        # there is no hdfs currently in ec2, except s3n/hdfs
        # the core-site.xml provides s3n info
        # it's possible that we can just always hardware the hdfs version
        # to match the cdh3 cluster we're hardwiring tests to
        # i.e. it won't make s3n/s3 break on ec2

        cmd = ["java",
               "-Xmx" + self.xmx,
               "-ea",
               "-jar", self.h2o_jar,
               "-name", self.cloud_name,
               "-baseport", str(self.my_base_port),
               "-hdfs_version", "cdh3"]

        # Add S3N credentials to cmd if they exist.
        ec2_hdfs_config_file_name = os.path.expanduser("~/.ec2/core-site.xml")
        if (os.path.exists(ec2_hdfs_config_file_name)):
            cmd.append("-hdfs_config")
            cmd.append(ec2_hdfs_config_file_name)

        self.output_file_name = \
            os.path.join(self.output_dir, "java_" + str(self.cloud_num) + "_" + str(self.node_num) + ".out.txt")
        f = open(self.output_file_name, "w")
        self.child = subprocess.Popen(args=cmd,
                                      stdout=f,
                                      stderr=subprocess.STDOUT,
                                      cwd=self.output_dir)
        self.pid = self.child.pid
        print("+ CMD: " + ' '.join(cmd))

    def scrape_port_from_stdout(self):
        """
        Look at the stdout log and figure out which port the JVM chose.
        Write this to self.port.
        This call is blocking.
        Exit if this fails.

        @return: none
        """
        retries = 30
        while (retries > 0):
            if (self.terminated):
                return
            f = open(self.output_file_name, "r")
            s = f.readline()
            while (len(s) > 0):
                if (self.terminated):
                    return
                match_groups = re.search(r"Listening for HTTP and REST traffic on  http://(\S+):(\d+)", s)
                if (match_groups is not None):
                    port = match_groups.group(2)
                    if (port is not None):
                        self.port = port
                        f.close()
                        print("H2O Cloud {} Node {} started with output file {}".format(self.cloud_num,
                                                                                        self.node_num,
                                                                                        self.output_file_name))
                        return

                s = f.readline()

            f.close()
            retries -= 1
            if (self.terminated):
                return
            time.sleep(1)

        print("")
        print("ERROR: Too many retries starting cloud.")
        print("")
        sys.exit(1)

    def stop(self):
        """
        Normal node shutdown.
        Ignore failures for now.

        @return: none
        """
        if (self.pid > 0):
            print("Killing JVM with PID {}".format(self.pid))
            try:
                self.child.terminate()
            except OSError:
                pass
            self.pid = -1

    def terminate(self):
        """
        Terminate a running node.  (Due to a signal.)

        @return: none
        """
        self.terminated = True
        self.stop()

    def get_ip(self):
        """ Return the ip address this node is really listening on. """
        return self.ip

    def get_port(self):
        """ Return the port this node is really listening on. """
        return self.port

    def __str__(self):
        s = ""
        s += "    node {}\n".format(self.node_num)
        s += "        xmx:          {}\n".format(self.xmx)
        s += "        my_base_port: {}\n".format(self.my_base_port)
        s += "        port:         {}\n".format(self.port)
        s += "        pid:          {}\n".format(self.pid)
        return s


class H2OCloud:
    """
    A class representing one of the H2O clouds.
    """

    def __init__(self, cloud_num, nodes_per_cloud, h2o_jar, base_port, xmx, output_dir):
        """
        Create a cloud.
        See node definition above for argument descriptions.

        @return: The cloud object.
        """
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        # Randomly choose a five digit cloud number.
        n = random.randint(10000, 99999)
        user = getpass.getuser()
        user = ''.join(user.split())

        self.cloud_name = "H2O_runit_{}_{}".format(user, n)
        self.nodes = []
        self.jobs_run = 0

        for node_num in range(self.nodes_per_cloud):
            node = H2OCloudNode(self.cloud_num, self.nodes_per_cloud, node_num,
                                self.cloud_name,
                                self.h2o_jar,
                                "127.0.0.1", self.base_port,
                                self.xmx, self.output_dir)
            self.nodes.append(node)

    def start(self):
        """
        Start H2O cloud.
        The cloud is not up until wait_for_cloud_to_be_up() is called and returns.

        @return: none
        """
        if (self.nodes_per_cloud > 1):
            print("")
            print("ERROR: Unimplemented: wait for cloud size > 1.")
            print("")
            sys.exit(1)

        for node in self.nodes:
            node.start()

    def wait_for_cloud_to_be_up(self):
        """
        Blocking call ensuring the cloud is available.

        @return: none
        """
        self._scrape_port_from_stdout()

    def stop(self):
        """
        Normal cloud shutdown.

        @return: none
        """
        for node in self.nodes:
            node.stop()

    def terminate(self):
        """
        Terminate a running cloud.  (Due to a signal.)

        @return: none
        """
        for node in self.nodes:
            node.terminate()

    def get_ip(self):
        """ Return an ip to use to talk to this cloud. """
        node = self.nodes[0]
        return node.get_ip()

    def get_port(self):
        """ Return a port to use to talk to this cloud. """
        node = self.nodes[0]
        return node.get_port()

    def _scrape_port_from_stdout(self):
        for node in self.nodes:
            node.scrape_port_from_stdout()

    def __str__(self):
        s = ""
        s += "cloud {}\n".format(self.cloud_num)
        s += "    name:     {}\n".format(self.cloud_name)
        s += "    jobs_run: {}\n".format(self.jobs_run)
        for node in self.nodes:
            s += str(node)
        return s


class Test:
    """
    A class representing one Test.

    cancelled: Don't start this test.
    terminated: Test killed due to signal.
    returncode: Exit code of child.
    pid: Process id of the test.
    ip: IP of cloud to run test.
    port: Port of cloud to run test.
    child: subprocess.Popen object.
    """

    @staticmethod
    def test_did_not_complete():
        """
        returncode marker to know if the test ran or not.
        """
        return -9999999

    def __init__(self, test_dir, test_short_dir, test_name, output_dir):
        """
        Create a Test.

        @param test_dir: Full absolute path to the test directory.
        @param test_short_dir: Path from h2o/R/tests to the test directory.
        @param test_name: Test filename with the directory removed.
        @param output_dir: The directory where we can create an output file for this process.
        @return: The test object.
        """
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.test_name = test_name
        self.output_dir = output_dir
        self.output_file_name = ""

        self.cancelled = False
        self.terminated = False
        self.returncode = Test.test_did_not_complete()
        self.start_seconds = -1
        self.pid = -1
        self.ip = None
        self.port = -1
        self.child = None

    def start(self, ip, port):
        """
        Start the test in a non-blocking fashion.

        @param ip: IP address of cloud to run on.
        @param port: Port of cloud to run on.
        @return: none
        """
        if (self.cancelled or self.terminated):
            return

        self.start_seconds = time.time()
        self.ip = ip
        self.port = port

        cmd = ["R",
               "-f",
               self.test_name,
               "--args",
               self.ip + ":" + str(self.port)]
        test_short_dir_with_no_slashes = re.sub(r'[\\/]', "_", self.test_short_dir)
        self.output_file_name = \
            os.path.join(self.output_dir, test_short_dir_with_no_slashes + "_" + self.test_name + ".out.txt")
        f = open(self.output_file_name, "w")
        self.child = subprocess.Popen(args=cmd,
                                      stdout=f,
                                      stderr=subprocess.STDOUT,
                                      cwd=self.test_dir)
        self.pid = self.child.pid
        # print("+ CMD: " + ' '.join(cmd))

    def is_completed(self):
        """
        Check if test has completed.

        This has side effects and MUST be called for the normal test queueing to work.
        Specifically, child.poll().

        @return: True if the test completed, False otherwise.
        """
        child = self.child
        if (child is None):
            return False
        child.poll()
        if (child.returncode is None):
            return False
        self.pid = -1
        self.returncode = child.returncode
        return True

    def cancel(self):
        """
        Mark this test as cancelled so it never tries to start.

        @return: none
        """
        if (self.pid <= 0):
            self.cancelled = True

    def terminate(self):
        """
        Terminate a running test.  (Due to a signal.)

        @return: none
        """
        self.terminated = True
        if (self.pid > 0):
            print("Killing Test with PID {}".format(self.pid))
            try:
                self.child.terminate()
            except OSError:
                pass
        self.pid = -1

    def get_test_dir_file_name(self):
        """
        @return: The full absolute path of this test.
        """
        return os.path.join(self.test_dir, self.test_name)

    def get_test_name(self):
        """
        @return: The file name (no directory) of this test.
        """
        return self.test_name

    def get_seed_used(self):
        """
        @return: The seed used by this test.
        """
        return self._scrape_output_for_seed()

    def get_ip(self):
        """
        @return: IP of the cloud where this test ran.
        """
        return self.ip

    def get_port(self):
        """
        @return: Integer port number of the cloud where this test ran.
        """
        return int(self.port)

    def get_passed(self):
        """
        @return: True if the test passed, False otherwise.
        """
        return (self.returncode == 0)

    def get_nopass(self):
        """
        Some tests are known not to fail and even if they don't pass we don't want
        to fail the overall regression PASS/FAIL status.

        @return: True if the test has been marked as NOPASS, False otherwise.
        """
        a = re.compile("NOPASS")
        return a.search(self.test_name)

    def get_completed(self):
        """
        @return: True if the test completed (pass or fail), False otherwise.
        """
        return (self.returncode > Test.test_did_not_complete())

    def get_output_dir_file_name(self):
        """
        @return: Full path to the output file which you can paste to a terminal window.
        """
        return (os.path.join(self.output_dir, self.output_file_name))

    def _scrape_output_for_seed(self):
        """
        @return: The seed scraped from the outpul file.
        """
        res = ""
        with open(self.get_output_dir_file_name(), "r") as f:
            for line in f:
                if "SEED used" in line:
                    line = line.strip().split(' ')
                    res = line[-1]
                    break
        return res

    def __str__(self):
        s = ""
        s += "Test: {}/{}\n".format(self.test_dir, self.test_name)
        return s


class RUnitRunner:
    """
    A class for running the RUnit tests.

    The tests list contains an object for every test.
    The tests_not_started list acts as a job queue.
    The tests_running list is polled for jobs that have finished.
    """

    def __init__(self,
                 test_root_dir,
                 use_cloud, use_ip, use_port,
                 num_clouds, nodes_per_cloud, h2o_jar, base_port, xmx, output_dir, failed_output_dir):
        """
        Create a runner.

        @param test_root_dir: h2o/R/tests directory.
        @param use_cloud: Use this one user-specified cloud.  Overrides num_clouds.
        @param use_ip: (if use_cloud) IP of one cloud to use.
        @param use_port: (if use_cloud) Port of one cloud to use.
        @param num_clouds: Number of H2O clouds to start.
        @param nodes_per_cloud: Number of H2O nodes to start per cloud.
        @param h2o_jar: Path to H2O jar file to run.
        @param base_port: Base H2O port (e.g. 54321) to start choosing from.
        @param xmx: Java -Xmx parameter.
        @param output_dir: Directory for output files.
        @return: The runner object.
        """
        self.test_root_dir = test_root_dir

        self.use_cloud = use_cloud

        # Valid if use_cloud is True
        self.use_ip = use_ip
        self.use_port = use_port

        # Valid if use_cloud is False
        self.num_clouds = num_clouds
        self.nodes_per_cloud = nodes_per_cloud
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.output_dir = output_dir
        self.failed_output_dir = failed_output_dir

        self.start_seconds = time.time()
        self.terminated = False
        self.clouds = []
        self.tests = []
        self.tests_not_started = []
        self.tests_running = []
        self.regression_passed = False
        self._create_output_dir()
        self._create_failed_output_dir()

        if (use_cloud):
            node_num = 0
            cloud = H2OUseCloud(node_num, use_ip, use_port)
            self.clouds.append(cloud)
        else:
            for i in range(self.num_clouds):
                cloud = H2OCloud(i, self.nodes_per_cloud, h2o_jar, self.base_port, xmx, self.output_dir)
                self.clouds.append(cloud)

    @staticmethod
    def find_test(test_to_run):
        """
        Be nice and try to help find the test if possible.
        If the test is actually found without looking, then just use it.
        Otherwise, search from the script's down directory down.
        """
        if (os.path.exists(test_to_run)):
            abspath_test = os.path.abspath(test_to_run)
            return abspath_test

        for d, subdirs, files in os.walk(os.path.dirname(os.path.realpath(__file__))):
            for f in files:
                if (f == test_to_run):
                    return os.path.join(d, f)

        # Not found, return the file, which will result in an error downstream when it can't be found.
        print("")
        print("ERROR: Test does not exist: " + test_to_run)
        print("")
        sys.exit(1)

    def read_test_list_file(self, test_list_file):
        """
        Read in a test list file line by line.  Each line in the file is a test
        to add to the test run.

        @param test_list_file: Filesystem path to a file with a list of tests to run.
        @return: none
        """
        try:
            f = open(test_list_file, "r")
            s = f.readline()
            while (len(s) != 0):
                stripped = s.strip()
                if (len(stripped) == 0):
                    s = f.readline()
                    continue
                if (stripped.startswith("#")):
                    s = f.readline()
                    continue
                found_stripped = RUnitRunner.find_test(stripped)
                self.add_test(found_stripped)
                s = f.readline()
            f.close()
        except IOError as e:
            print("")
            print("ERROR: Failure reading test list: " + test_list_file)
            print("       (errno {0}): {1}".format(e.errno, e.strerror))
            print("")
            sys.exit(1)

    def build_test_list(self, test_group, run_small, run_medium, run_large):
        """
        Recursively find the list of tests to run and store them in the object.
        Fills in self.tests and self.tests_not_started.

        @param test_group: Name of the test group of tests to run.
        @return:  none
        """
        if (self.terminated):
            return

        for root, dirs, files in os.walk(self.test_root_dir):
            if (root.endswith("Util")):
                continue

            for f in files:
                if (not re.match(".*runit.*\.[rR]$", f)):
                    continue

                is_small = False
                is_medium = False
                is_large = False

                if (re.match(".*large.*", f)):
                    is_large = True
                elif (re.match(".*medium.*", f)):
                    is_large = True
                else:
                    is_small = True

                if (is_small and not run_small):
                    continue
                if (is_medium and not run_medium):
                    continue
                if (is_large and not run_large):
                    continue

                if (test_group is not None):
                    test_short_dir = self._calc_test_short_dir(os.path.join(root, f))
                    if test_group.lower() not in test_short_dir:
                        continue

                self.add_test(os.path.join(root, f))

    def add_test(self, test_path):
        """
        Add one test to the list of tests to run.

        @param test_path: File system path to the test.
        @return: none
        """
        abs_test_path = os.path.abspath(test_path)
        abs_test_dir = os.path.dirname(abs_test_path)
        test_file = os.path.basename(abs_test_path)

        if (not os.path.exists(abs_test_path)):
            print("")
            print("ERROR: Test does not exist: " + abs_test_path)
            print("")
            sys.exit(1)

        test_short_dir = self._calc_test_short_dir(test_path)

        test = Test(abs_test_dir, test_short_dir, test_file, self.output_dir)
        self.tests.append(test)
        self.tests_not_started.append(test)

    def start_clouds(self):
        """
        Start all H2O clouds.

        @return: none
        """
        if (self.terminated):
            return

        if (self.use_cloud):
            return

        print("")
        print("Starting clouds...")
        print("")

        for cloud in self.clouds:
            if (self.terminated):
                return
            cloud.start()

        print("")
        print("Waiting for H2O nodes to come up...")
        print("")

        for cloud in self.clouds:
            if (self.terminated):
                return
            cloud.wait_for_cloud_to_be_up()

    def run_tests(self):
        """
        Run all tests.

        @return: none
        """
        if (self.terminated):
            return

        self._log("")
        self._log("Setting up R H2O package...")
        if (True):
            out_file_name = os.path.join(self.output_dir, "runnerSetupPackage.out.txt")
            out = open(out_file_name, "w")
            cloud = self.clouds[0]
            port = cloud.get_port()
            cmd = ["R",
                   "--quiet",
                   "-f",
                   os.path.join(self.test_root_dir, "Utils/runnerSetupPackage.R"),
                   "--args",
                   "127.0.0.1:" + str(port)]
            child = subprocess.Popen(args=cmd,
                                     stdout=out,
                                     stderr=subprocess.STDOUT)
            rv = child.wait()
            if (self.terminated):
                return
            if (rv != 0):
                print("")
                print("ERROR: Utils/runnerSetupPackage.R failed.")
                print("       (See " + out_file_name + ")")
                print("")
                sys.exit(1)
            out.close()

        num_tests = len(self.tests)
        num_nodes = len(self.clouds * self.nodes_per_cloud)
        self._log("")
        if (self.use_cloud):
            self._log("Starting {} tests...".format(num_tests))
        else:
            self._log("Starting {} tests on {} total H2O nodes...".format(num_tests, num_nodes))
        self._log("")

        # Start the first n tests, where n is the lesser of the total number of tests and the total number of clouds.
        start_count = min(len(self.tests_not_started), len(self.clouds))
        for i in range(start_count):
            cloud = self.clouds[i]
            ip = cloud.get_ip()
            port = cloud.get_port()
            self._start_next_test_on_ip_port(ip, port)

        # As each test finishes, send a new one to the cloud that just freed up.
        while (len(self.tests_not_started) > 0):
            if (self.terminated):
                return
            completed_test = self._wait_for_one_test_to_complete()
            if (self.terminated):
                return
            self._report_test_result(completed_test)
            ip_of_completed_test = completed_test.get_ip()
            port_of_completed_test = completed_test.get_port()
            self._start_next_test_on_ip_port(ip_of_completed_test, port_of_completed_test)

        # Wait for remaining running tests to complete.
        while (len(self.tests_running) > 0):
            if (self.terminated):
                return
            completed_test = self._wait_for_one_test_to_complete()
            if (self.terminated):
                return
            self._report_test_result(completed_test)

    def stop_clouds(self):
        """
        Stop all H2O clouds.

        @return: none
        """
        if (self.terminated):
            return

        if (self.use_cloud):
            print("")
            print("All tests completed...")
            print("")
            return

        print("")
        print("All tests completed; tearing down clouds...")
        print("")
        for cloud in self.clouds:
            cloud.stop()

    def report_summary(self):
        """
        Report some summary information when the tests have finished running.

        @return: none
        """
        passed = 0
        nopass_but_tolerate = 0
        failed = 0
        notrun = 0
        total = 0
        true_fail_list = []
        for test in self.tests:
            if (test.get_passed()):
                passed += 1
            else:
                if (test.get_nopass()):
                    nopass_but_tolerate += 1

                if (test.get_completed()):
                    failed += 1
                    if (not test.get_nopass()):
                        true_fail_list.append(test.test_name)
                else:
                    notrun += 1
            total += 1

        if ((passed + nopass_but_tolerate) == total):
            self.regression_passed = True
        else:
            self.regression_passed = False

        end_seconds = time.time()
        delta_seconds = end_seconds - self.start_seconds
        run = total - notrun
        self._log("")
        self._log("----------------------------------------------------------------------")
        self._log("")
        self._log("SUMMARY OF RESULTS")
        self._log("")
        self._log("----------------------------------------------------------------------")
        self._log("")
        self._log("Total tests:          " + str(total))
        self._log("Passed:               " + str(passed))
        self._log("Did not pass:         " + str(failed))
        self._log("Did not complete:     " + str(notrun))
        self._log("Tolerated NOPASS:     " + str(nopass_but_tolerate))
        self._log("")
        self._log("Total time:           %.2f sec" % delta_seconds)
        if (run > 0):
            self._log("Time/completed test:  %.2f sec" % (delta_seconds / run))
        else:
            self._log("Time/completed test:  N/A")
        self._log("")
        self._log("True fail list:       " + ", ".join(true_fail_list))
        self._log("")

    def terminate(self):
        """
        Terminate all running clouds.  (Due to a signal.)

        @return: none
        """
        self.terminated = True

        for test in self.tests:
            test.cancel()

        for test in self.tests:
            test.terminate()

        for cloud in self.clouds:
            cloud.terminate()

    def get_regression_passed(self):
        """
        Return whether the overall regression passed or not.

        @return: true if the exit value should be 0, false otherwise.
        """
        return self.regression_passed

    #--------------------------------------------------------------------
    # Private methods below this line.
    #--------------------------------------------------------------------

    def _calc_test_short_dir(self, test_path):
        """
        Calculate directory of test relative to test_root_dir.

        @param test_path: Path to test file.
        @return: test_short_dir, relative directory containing test (relative to test_root_dir).
        """
        abs_test_root_dir = os.path.abspath(self.test_root_dir)
        abs_test_path = os.path.abspath(test_path)
        abs_test_dir = os.path.dirname(abs_test_path)

        test_short_dir = abs_test_dir
        prefix = os.path.join(abs_test_root_dir, "")
        if (test_short_dir.startswith(prefix)):
            test_short_dir = test_short_dir.replace(prefix, "", 1)

        return test_short_dir

    def _create_failed_output_dir(self):
        try:
            os.makedirs(self.failed_output_dir)
        except OSError as e:
            print("")
            print("mkdir failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + self.failed_output_dir)
            print("")
            print("(try adding --wipe)")
            print("")
            sys.exit(1)

    def _create_output_dir(self):
        try:
            os.makedirs(self.output_dir)
        except OSError as e:
            print("")
            print("mkdir failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + self.output_dir)
            print("")
            print("(try adding --wipe)")
            print("")
            sys.exit(1)

    def _start_next_test_on_ip_port(self, ip, port):
        test = self.tests_not_started.pop(0)
        self.tests_running.append(test)
        test.start(ip, port)

    def _wait_for_one_test_to_complete(self):
        while (True):
            for test in self.tests_running:
                if (self.terminated):
                    return None
                if (test.is_completed()):
                    self.tests_running.remove(test)
                    return test
            if (self.terminated):
                return
            time.sleep(1)

    def _report_test_result(self, test):
        port = test.get_port()
        now = time.time()
        duration = now - test.start_seconds
        if (test.get_passed()):
            s = "PASS      %d %4ds %-60s" % (port, duration, test.get_test_name())
            self._log(s)
        else:
            s = "     FAIL %d %4ds %-60s %s  %s" % (port, duration, test.get_test_name(), test.get_output_dir_file_name(), test.get_seed_used())
            self._log(s)
            f = self._get_failed_filehandle_for_appending()
            f.write(test.get_test_dir_file_name() + "\n")
            f.close()
            # Copy failed test output into directory failed
            if not test.get_nopass():
                shutil.copy(test.get_output_dir_file_name(), self.failed_output_dir)

    def _log(self, s):
        f = self._get_summary_filehandle_for_appending()
        print(s)
        f.write(s + "\n")
        f.close()

    def _get_summary_filehandle_for_appending(self):
        summary_file_name = os.path.join(self.output_dir, "summary.txt")
        f = open(summary_file_name, "a")
        return f

    def _get_failed_filehandle_for_appending(self):
        summary_file_name = os.path.join(self.output_dir, "failed.txt")
        f = open(summary_file_name, "a")
        return f

    def __str__(self):
        s = "\n"
        s += "test_root_dir:    {}\n".format(self.test_root_dir)
        s += "output_dir:       {}\n".format(self.output_dir)
        s += "h2o_jar:          {}\n".format(self.h2o_jar)
        s += "num_clouds:       {}\n".format(self.num_clouds)
        s += "nodes_per_cloud:  {}\n".format(self.nodes_per_cloud)
        s += "base_port:        {}\n".format(self.base_port)
        s += "\n"
        for c in self.clouds:
            s += str(c)
        s += "\n"
        # for t in self.tests:
        #     s += str(t)
        return s


#--------------------------------------------------------------------
# Main program
#--------------------------------------------------------------------

# Global variables that can be set by the user.
g_script_name = ""
g_base_port = 40000
g_num_clouds = 5
g_wipe_test_state = False
g_wipe_output_dir = False
g_test_to_run = None
g_test_list_file = None
g_test_group = None
g_run_small = True
g_run_medium = True
g_run_large = True
g_use_cloud = False
g_use_ip = None
g_use_port = None
g_no_run = False
g_jvm_xmx = "1g"

# Global variables that are set internally.
g_output_dir = None
g_runner = None
g_handling_signal = False


def use(x):
    """ Hack to remove compiler warning. """
    if False:
        print(x)


def signal_handler(signum, stackframe):
    global g_runner
    global g_handling_signal

    use(stackframe)

    if (g_handling_signal):
        # Don't do this recursively.
        return
    g_handling_signal = True

    print("")
    print("----------------------------------------------------------------------")
    print("")
    print("SIGNAL CAUGHT (" + str(signum) + ").  TEARING DOWN CLOUDS.")
    print("")
    print("----------------------------------------------------------------------")
    g_runner.terminate()


def usage():
    print("")
    print("Usage:  " + g_script_name +
          " [--wipeall]"
          " [--wipe]"
          " [--baseport port]"
          " [--numclouds n]"
          " [--test path/to/test.R]"
          " [--testlist path/to/list/file]"
          " [--testgroup group]"
          " [--testsize (s|m|l)]"
          " [--usecloud ip:port]"
          " [--norun]")
    print("")
    print("    (Output dir is: " + g_output_dir + ")")
    print("    (Default number of clouds is: " + str(g_num_clouds) + ")")
    print("")
    print("    --wipeall     Remove all prior test state before starting, particularly")
    print("                  random seeds.")
    print("                  (Removes master_seed file and all Rsandbox directories.")
    print("                  Also wipes the output dir before starting.)")
    print("")
    print("    --wipe        Wipes the output dir before starting.  Keeps old random seeds.")
    print("")
    print("    --baseport    The first port at which H2O starts searching for free ports.")
    print("")
    print("    --numclouds   The number of clouds to start.")
    print("                  Each test is randomly assigned to a cloud.")
    print("")
    print("    --test        If you only want to run one test, specify it like this.")
    print("")
    print("    --testlist    A file containing a list of tests to run (for example the")
    print("                  'failed.txt' file from the output directory).")
    print("")
    print("    --testgroup   Test a group of tests by function:")
    print("                  pca, glm, kmeans, gbm, rf, algos, golden, munging")
    print("")
    print("    --testsize    Sizes (and by extension length) of tests to run:")
    print("                  s=small (seconds), m=medium (a minute or two), l=large (longer)")
    print("                  (Default is to run all tests.)")
    print("")
    print("    --usecloud    ip:port of cloud to send tests to instead of starting clouds.")
    print("                  (When this is specified, numclouds is ignored.)")
    print("")
    print("    --norun       Perform side effects like wipe, but don't actually run tests.")
    print("")
    print("    --jvm.xmx     Configure size of launched JVM running H2O. E.g. '--jvm.xmx 3g'")
    print("")
    print("    If neither --test nor --testlist is specified, then the list of tests is")
    print("    discovered automatically as files matching '*runit*.R'.")
    print("")
    print("")
    print("Examples:")
    print("")
    print("    Just accept the defaults and go (note: output dir must not exist):")
    print("        "+g_script_name)
    print("")
    print("    Remove all random seeds (i.e. make new ones) but don't run any tests:")
    print("        "+g_script_name+" --wipeall --norun")
    print("")
    print("    For a powerful laptop with 8 cores (keep default numclouds):")
    print("        "+g_script_name+" --wipeall")
    print("")
    print("    For a big server with 32 cores:")
    print("        "+g_script_name+" --wipeall --numclouds 16")
    print("")
    print("    Just run the tests that finish quickly")
    print("        "+g_script_name+" --wipeall --testsize s")
    print("")
    print("    Run one specific test, keeping old random seeds:")
    print("        "+g_script_name+" --wipe --test path/to/test.R")
    print("")
    print("    Rerunning failures from a previous run, keeping old random seeds:")
    print("        # Copy failures.txt, otherwise --wipe removes the directory with the list!")
    print("        cp " + os.path.join(g_output_dir, "failures.txt") + " .")
    print("        "+g_script_name+" --wipe --numclouds 16 --testlist failed.txt")
    print("")
    print("    Run tests on a pre-existing cloud (e.g. in a debugger), keeping old random seeds:")
    print("        "+g_script_name+" --wipe --usecloud ip:port")
    sys.exit(1)


def unknown_arg(s):
    print("")
    print("ERROR: Unknown argument: " + s)
    print("")
    usage()


def bad_arg(s):
    print("")
    print("ERROR: Illegal use of (otherwise valid) argument: " + s)
    print("")
    usage()


def parse_args(argv):
    global g_base_port
    global g_num_clouds
    global g_wipe_test_state
    global g_wipe_output_dir
    global g_test_to_run
    global g_test_list_file
    global g_test_group
    global g_run_small
    global g_run_medium
    global g_run_large
    global g_use_cloud
    global g_use_ip
    global g_use_port
    global g_no_run
    global g_jvm_xmx

    i = 1
    while (i < len(argv)):
        s = argv[i]

        if (s == "--baseport"):
            i += 1
            if (i > len(argv)):
                usage()
            g_base_port = int(argv[i])
        elif (s == "--numclouds"):
            i += 1
            if (i > len(argv)):
                usage()
            g_num_clouds = int(argv[i])
        elif (s == "--wipeall"):
            g_wipe_test_state = True
            g_wipe_output_dir = True
        elif (s == "--wipe"):
            g_wipe_output_dir = True
        elif (s == "--test"):
            i += 1
            if (i > len(argv)):
                usage()
            g_test_to_run = RUnitRunner.find_test(argv[i])
        elif (s == "--testlist"):
            i += 1
            if (i > len(argv)):
                usage()
            g_test_list_file = argv[i]
        elif (s == "--testgroup"):
            i += 1
            if (i > len(argv)):
                usage()
            g_test_group = argv[i]
        elif (s == "--testsize"):
            i += 1
            if (i > len(argv)):
                usage()
            v = argv[i]
            if (re.match(r'(s)?(m)?(l)?', v)):
                if (not 's' in v):
                    g_run_small = False
                if (not 'm' in v):
                    g_run_medium = False
                if (not 'l' in v):
                    g_run_large = False
            else:
                bad_arg(s)
        elif (s == "--usecloud"):
            i += 1
            if (i > len(argv)):
                usage()
            s = argv[i]
            m = re.match(r'(\S+):([1-9][0-9]*)', s)
            if (m is None):
                unknown_arg(s)
            g_use_cloud = True
            g_use_ip = m.group(1)
            port_string = m.group(2)
            g_use_port = int(port_string)
        elif (s == "--jvm.xmx"):
            i += 1
            if (i > len(argv)):
                usage()
            g_jvm_xmx = argv[i]
        elif (s == "--norun"):
            g_no_run = True
        elif (s == "-h" or s == "--h" or s == "-help" or s == "--help"):
            usage()
        else:
            unknown_arg(s)

        i += 1


def wipe_output_dir():
    print("")
    print("Wiping output directory...")
    try:
        if (os.path.exists(g_output_dir)):
            shutil.rmtree(g_output_dir)
    except OSError as e:
        print("")
        print("ERROR: Removing output directory failed: " + g_output_dir)
        print("       (errno {0}): {1}".format(e.errno, e.strerror))
        print("")
        sys.exit(1)


def wipe_test_state(test_root_dir):
    print("")
    print("Wiping test state (including random seeds)...")
    if (True):
        possible_seed_file = os.path.join(test_root_dir, str("master_seed"))
        if (os.path.exists(possible_seed_file)):
            try:
                os.remove(possible_seed_file)
            except OSError as e:
                print("")
                print("ERROR: Removing seed file failed: " + possible_seed_file)
                print("       (errno {0}): {1}".format(e.errno, e.strerror))
                print("")
                sys.exit(1)
    for d, subdirs, files in os.walk(test_root_dir):
        for s in subdirs:
            if ("Rsandbox" in s):
                rsandbox_dir = os.path.join(d, s)
                try:
                    shutil.rmtree(rsandbox_dir)
                except OSError as e:
                    print("")
                    print("ERROR: Removing RSandbox directory failed: " + rsandbox_dir)
                    print("       (errno {0}): {1}".format(e.errno, e.strerror))
                    print("")
                    sys.exit(1)


def main(argv):
    """
    Main program.

    @return: none
    """
    global g_script_name
    global g_num_clouds
    global g_output_dir
    global g_failed_output_dir
    global g_test_to_run
    global g_test_list_file
    global g_test_group
    global g_runner

    g_script_name = os.path.basename(argv[0])
    test_root_dir = os.path.dirname(os.path.realpath(__file__))

    # Calculate global variables.
    g_output_dir = os.path.join(test_root_dir, str("results"))
    g_failed_output_dir = os.path.join(g_output_dir, str("failed"))

    # Calculate and set other variables.
    nodes_per_cloud = 1
    h2o_jar = os.path.abspath(
        os.path.join(os.path.join(os.path.join(os.path.join(
            test_root_dir, ".."), ".."), "target"), "h2o.jar"))

    # Override any defaults with the user's choices.
    parse_args(argv)

    # Wipe output directory if requested.
    if (g_wipe_output_dir):
        wipe_output_dir()

    # Wipe persistent test state if requested.
    if (g_wipe_test_state):
        wipe_test_state(test_root_dir)

    # Create runner object.
    # Just create one cloud if we're only running one test, even if the user specified more.
    if (g_test_to_run is not None):
        g_num_clouds = 1

    g_runner = RUnitRunner(test_root_dir,
                           g_use_cloud, g_use_ip, g_use_port,
                           g_num_clouds, nodes_per_cloud, h2o_jar, g_base_port, g_jvm_xmx, g_output_dir, g_failed_output_dir)

    # Build test list.
    if (g_test_to_run is not None):
        g_runner.add_test(g_test_to_run)
    elif (g_test_list_file is not None):
        g_runner.read_test_list_file(g_test_list_file)
    else:
        # Test group can be None or not.
        g_runner.build_test_list(g_test_group, g_run_small, g_run_medium, g_run_large)

    # If no run is specified, then do an early exit here.
    if (g_no_run):
        sys.exit(0)

    # Handle killing the runner.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Sanity check existence of H2O jar file before starting the cloud.
    if (not os.path.exists(h2o_jar)):
        print("")
        print("ERROR: H2O jar not found: " + h2o_jar)
        print("")
        sys.exit(1)

    # Run.
    try:
        g_runner.start_clouds()
        g_runner.run_tests()
    finally:
        g_runner.stop_clouds()
        g_runner.report_summary()

    # If the overall regression did not pass then exit with a failure status code.
    if (not g_runner.get_regression_passed()):
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)
