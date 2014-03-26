package water.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.lang.management.ManagementPermission;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Driver class to start a Hadoop mapreduce job which wraps an H2O cluster launch.
 *
 * All mapreduce I/O is typed as <Text, Text>.
 * The first Text is the Key (Mapper Id).
 * The second Text is the Value (a log output).
 *
 * Adapted from
 * https://svn.apache.org/repos/asf/hadoop/common/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/SleepJob.java
 */
@SuppressWarnings("deprecation")
public class h2odriver extends Configured implements Tool {
  final static int DEFAULT_CLOUD_FORMATION_TIMEOUT_SECONDS = 120;
  final static int CLOUD_FORMATION_SETTLE_DOWN_SECONDS = 2;
  final static int DEFAULT_EXTRA_MEM_PERCENT = 10;

  // Options that are parsed by the main thread before other threads are created.
  static String jobtrackerName = null;
  static int numNodes = -1;
  static String outputPath = null;
  static String mapperXmx = null;
  static int extraMemPercent = -1;            // Between 0 and 10, typically.  Cannot be negative.
  static String driverCallbackIp = null;
  static int driverCallbackPort = 0;          // By default, let the system pick the port.
  static String network = null;
  static boolean disown = false;
  static String clusterReadyFileName = null;
  static int cloudFormationTimeoutSeconds = DEFAULT_CLOUD_FORMATION_TIMEOUT_SECONDS;
  static int nthreads = -1;
  static int basePort = -1;
  static boolean beta = false;
  static boolean enableExceptions = false;

  // Runtime state that might be touched by different threads.
  volatile ServerSocket driverCallbackSocket = null;
  volatile Job job = null;
  volatile CtrlCHandler ctrlc = null;
  volatile boolean clusterIsUp = false;
  volatile boolean clusterFailedToComeUp = false;

  public static class H2ORecordReader extends RecordReader<Text, Text> {
    H2ORecordReader() {
    }

    public void initialize(InputSplit split, TaskAttemptContext context) {
    }

    public boolean nextKeyValue() throws IOException {
      return false;
    }

    public Text getCurrentKey() { return null; }
    public Text getCurrentValue() { return null; }
    public void close() throws IOException { }
    public float getProgress() throws IOException { return 0; }
  }

  public static class EmptySplit extends InputSplit implements Writable {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static class H2OInputFormat extends InputFormat<Text, Text> {
    H2OInputFormat() {
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      List<InputSplit> ret = new ArrayList<InputSplit>();
      int numSplits = numNodes;
      for (int i = 0; i < numSplits; ++i) {
        ret.add(new EmptySplit());
      }
      return ret;
    }

    public RecordReader<Text, Text> createRecordReader(
            InputSplit ignored, TaskAttemptContext taskContext)
            throws IOException {
      H2ORecordReader trr = new H2ORecordReader();
      return trr;
    }
  }

  /**
   * Handle Ctrl-C and other catchable shutdown events.
   * If we successfully catch one, then try to kill the hadoop job if
   * we have not already been told it completed.
   *
   * (Of course kill -9 cannot be handled.)
   */
  class CtrlCHandler extends Thread {
    volatile boolean _complete = false;

    public void setComplete() {
      _complete = true;
    }

    @Override
    public void run() {
      if (_complete) {
        return;
      }

      boolean killed = false;

      try {
        System.out.println("Attempting to clean up hadoop job...");
        job.killJob();
        for (int i = 0; i < 5; i++) {
          if (job.isComplete()) {
            System.out.println("Killed.");
            killed = true;
            break;
          }

          Thread.sleep(1000);
        }
      }
      catch (Exception _) {
      }
      finally {
        if (! killed) {
          System.out.println("Kill attempt failed, please clean up job manually.");
        }
      }
    }
  }

  /**
   * Read and handle one Mapper->Driver Callback message.
   */
  class CallbackHandlerThread extends Thread {
    private Socket _s;
    private CallbackManager _cm;

    private void createClusterReadyFile(String ip, int port) throws Exception {
      String fileName = clusterReadyFileName + ".tmp";
      String text = ip + ":" + port + "\n";
      try {
        File file = new File(fileName);
        BufferedWriter output = new BufferedWriter(new FileWriter(file));
        output.write(text);
        output.flush();
        output.close();

        File file2 = new File(clusterReadyFileName);
        boolean success = file.renameTo(file2);
        if (! success) {
          throw new Exception ("Failed to create file " + clusterReadyFileName);
        }
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }

    public void setSocket (Socket value) {
      _s = value;
    }

    public void setCallbackManager (CallbackManager value) {
      _cm = value;
    }

    @Override
    public void run() {
      MapperToDriverMessage msg = new MapperToDriverMessage();
      try {
        msg.read(_s);
        char type = msg.getType();
        if (type == MapperToDriverMessage.TYPE_EOF_NO_MESSAGE) {
          // Ignore it.
          _s.close();
          return;
        }

        // System.out.println("Read message with type " + (int)type);
        if (type == MapperToDriverMessage.TYPE_EMBEDDED_WEB_SERVER_IP_PORT) {
          // System.out.println("H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() + " started");
          _s.close();
        }
        else if (type == MapperToDriverMessage.TYPE_FETCH_FLATFILE) {
          // DO NOT close _s here!
          // Callback manager accumulates sockets to H2O nodes so it can
          // a synthesized flatfile once everyone has arrived.

          System.out.println("H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() + " requested flatfile");
          _cm.registerNode(msg.getEmbeddedWebServerIp(), msg.getEmbeddedWebServerPort(), _s);
        }
        else if (type == MapperToDriverMessage.TYPE_CLOUD_SIZE) {
          _s.close();
          System.out.println("H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() + " reports H2O cluster size " + msg.getCloudSize());
          if (msg.getCloudSize() == numNodes) {
            // Do this under a synchronized block to avoid getting multiple cluster ready notification files.
            synchronized (h2odriver.class) {
              if (! clusterIsUp) {
                if (clusterReadyFileName != null) {
                  createClusterReadyFile(msg.getEmbeddedWebServerIp(), msg.getEmbeddedWebServerPort());
                  System.out.println("Cluster notification file (" + clusterReadyFileName + ") created.");
                }
                clusterIsUp = true;
              }
            }
          }
        }
        else if (type == MapperToDriverMessage.TYPE_EXIT) {
          System.out.println(
                  "H2O node " + msg.getEmbeddedWebServerIp() + ":" + msg.getEmbeddedWebServerPort() +
                  " on host " + _s.getInetAddress().getHostAddress() +
                  " exited with status " + msg.getExitStatus()
          );
          _s.close();
          if (! clusterIsUp) {
            clusterFailedToComeUp = true;
          }
        }
        else {
          _s.close();
          System.err.println("MapperToDriverMessage: Read invalid type (" + type + ") from socket, ignoring...");
        }
      }
      catch (Exception e) {
        System.out.println("Exception occurred in CallbackHandlerThread");
        System.out.println(e.toString());
        if (e.getMessage() != null) {
          System.out.println(e.getMessage());
        }
        e.printStackTrace();
      }
    }
  }

  /**
   * Start a long-running thread ready to handle Mapper->Driver messages.
   */
  class CallbackManager extends Thread {
    private boolean _registered = false;

    private ServerSocket _ss;

    // Nodes and socks
    private HashSet<String> _dupChecker;
    private ArrayList<String> _nodes;
    private ArrayList<Socket> _socks;

    public void setServerSocket (ServerSocket value) {
      _ss = value;
    }

    public void registerNode (String ip, int port, Socket s) {
      synchronized (_dupChecker) {
        String entry = ip + ":" + port;

        if (_dupChecker.contains(entry)) {
          // This is bad.
          System.out.println("ERROR: Duplicate node registered (" + entry + "), exiting");
          System.exit(1);
        }

        _nodes.add(entry);
        _socks.add(s);
        if (_nodes.size() != numNodes) {
          return;
        }

        _registered = true;   // Definitely don't want to do this more than once.

        System.out.println("Sending flatfiles to nodes...");

        assert (_nodes.size() == numNodes);
        assert (_nodes.size() == _socks.size());

        // Build the flatfile and send it to all nodes.
        String flatfile = "";
        for (int i = 0; i < _nodes.size(); i++) {
          String val = _nodes.get(i);
          flatfile += val;
          flatfile += "\n";
        }

        for (int i = 0; i < _socks.size(); i++) {
          Socket nodeSock = _socks.get(i);
          DriverToMapperMessage msg = new DriverToMapperMessage();
          msg.setMessageFetchFlatfileResponse(flatfile);
          try {
            System.out.println("    [Sending flatfile to node " + _nodes.get(i) + "]");
            msg.write(nodeSock);
            nodeSock.close();
          }
          catch (Exception e) {
            System.out.println("ERROR: Failed to write to H2O node " + _nodes.get(i));
            System.out.println(e.toString());
            if (e.getMessage() != null) {
              System.out.println(e.getMessage());
            }
            e.printStackTrace();
            System.exit(1);
          }
        }
      }
    }

    @Override
    public void run() {
      _dupChecker = new HashSet<String>();
      _nodes = new ArrayList<String>();
      _socks = new ArrayList<Socket>();

      while (true) {
        try {
          Socket s = _ss.accept();
          CallbackHandlerThread t = new CallbackHandlerThread();
          t.setSocket(s);
          t.setCallbackManager(this);
          t.start();
        }
        catch (Exception e) {
          System.out.println("Exception occurred in CallbackManager");
          System.out.println(e.toString());
          if (e.getMessage() != null) {
            System.out.println(e.getMessage());
          }
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Print usage and exit 1.
   */
  static void usage() {
    System.err.printf(
            "\n" +
                    "Usage: h2odriver\n" +
                    "          -libjars <.../h2o.jar>\n" +
                    "          [other generic Hadoop ToolRunner options]\n" +
                    "          [-h | -help]\n" +
                    "          [-jobname <name of job in jobtracker (defaults to: 'H2O_nnnnn')>]\n" +
                    "              (Note nnnnn is chosen randomly to produce a unique name)\n" +
                    "          [-driverif <ip address of mapper->driver callback interface>]\n" +
                    "          [-driverport <port of mapper->driver callback interface>]\n" +
                    "          [-network <IPv4network1Specification>[,<IPv4network2Specification> ...]\n" +
                    "          [-timeout <seconds>]\n" +
                    "          [-disown]\n" +
                    "          [-notify <notification file name>]\n" +
                    "          -mapperXmx <per mapper Java Xmx heap size>\n" +
                    "          [-extramempercent <0 to 20>]\n" +
                    "          -n | -nodes <number of H2O nodes (i.e. mappers) to create>\n" +
                    "          [-nthreads <maximum typical worker threads, i.e. cpus to use>]\n" +
                    "          [-baseport <starting HTTP port for H2O nodes; default is 54321>]\n" +
                    "          [-ea]\n" +
                    "          -o | -output <hdfs output dir>\n" +
                    "\n" +
                    "Notes:\n" +
                    "          o  Each H2O node runs as a mapper.\n" +
                    "          o  Only one mapper may be run per host.\n" +
                    "          o  There are no combiners or reducers.\n" +
                    "          o  Each H2O cluster should have a unique jobname.\n" +
                    "          o  -mapperXmx, -nodes and -output are required.\n" +
                    "\n" +
                    "          o  -mapperXmx is set to both Xms and Xmx of the mapper to reserve\n" +
                    "             memory up front.\n" +
                    "          o  -extramempercent is a percentage of mapperXmx.  (Default: " + DEFAULT_EXTRA_MEM_PERCENT + ")\n" +
                    "             Extra memory for internal JVM use outside of Java heap.\n" +
                    "                 mapreduce.map.memory.mb = mapperXmx * (1 + extramempercent/100)\n" +
                    "          o  -libjars with an h2o.jar is required.\n" +
                    "          o  -driverif and -driverport let the user optionally specify the\n" +
                    "             network interface and port (on the driver host) for callback\n" +
                    "             messages from the mapper to the driver.\n" +
                    "          o  -network allows the user to specify a list of networks that the\n" +
                    "             H2O nodes can bind to.  Use this if you have multiple network\n" +
                    "             interfaces on the hosts in your Hadoop cluster and you want to\n" +
                    "             force H2O to use a specific one.\n" +
                    "             (Example network specification: '10.1.2.0/24' allows 256 legal\n" +
                    "             possibilities.)\n" +
                    "          o  -timeout specifies how many seconds to wait for the H2O cluster\n" +
                    "             to come up before giving up.  (Default: " + DEFAULT_CLOUD_FORMATION_TIMEOUT_SECONDS + " seconds\n" +
                    "          o  -disown causes the driver to exit as soon as the cloud forms.\n" +
                    "             Otherwise, Ctrl-C of the driver kills the Hadoop Job.\n" +
                    "          o  -notify specifies a file to write when the cluster is up.\n" +
                    "             The file contains one line with the IP and port of the embedded\n" +
                    "             web server for one of the H2O nodes in the cluster.  e.g.\n" +
                    "                 192.168.1.100:54321\n" +
                    "          o  All mappers must start before the H2O cloud is considered up.\n" +
                    "\n" +
                    "Examples:\n" +
                    "          hadoop jar h2odriver_HHH.jar water.hadoop.h2odriver -jt <yourjobtracker>:<yourport> -libjars h2o.jar -mapperXmx 1g -nodes 1 -output hdfsOutputDir\n" +
                    "          hadoop jar h2odriver_HHH.jar water.hadoop.h2odriver -jt <yourjobtracker>:<yourport> -libjars h2o.jar -mapperXmx 1g -nodes 1 -notify notify.txt -disown -output hdfsOutputDir\n" +
                    "          (Choose the proper h2odriver (_HHH) for your version of hadoop.\n" +
                    "\n" +
                    "Exit value:\n" +
                    "          0 means the cluster exited successfully with an orderly Shutdown.\n" +
                    "              (From the Web UI or the REST API.)\n" +
                    "\n" +
                    "          non-zero means the cluster exited with a failure.\n" +
                    "              (Note that Ctrl-C is treated as a failure.)\n" +
                    "\n"
    );

    System.exit(1);
  }

  /**
   * Print an error message, print usage, and exit 1.
   * @param s Error message
   */
  static void error(String s) {
    System.err.printf("\nERROR: " + "%s\n\n", s);
    usage();
  }

  /**
   * Parse remaining arguments after the ToolRunner args have already been removed.
   * @param args Argument list
   */
  void parseArgs(String[] args) {
    int i = 0;
    while (true) {
      if (i >= args.length) {
        break;
      }

      String s = args[i];
      if (s.equals("-h") ||
              s.equals("help") ||
              s.equals("-help") ||
              s.equals("--help")) {
        usage();
      }
      else if (s.equals("-n") ||
              s.equals("-nodes")) {
        i++; if (i >= args.length) { usage(); }
        numNodes = Integer.parseInt(args[i]);
      }
      else if (s.equals("-o") ||
              s.equals("-output")) {
        i++; if (i >= args.length) { usage(); }
        outputPath = args[i];
      }
      else if (s.equals("-jobname")) {
        i++; if (i >= args.length) { usage(); }
        jobtrackerName = args[i];
      }
      else if (s.equals("-mapperXmx")) {
        i++; if (i >= args.length) { usage(); }
        mapperXmx = args[i];
      }
      else if (s.equals("-extramempercent")) {
        i++; if (i >= args.length) { usage(); }
        extraMemPercent = Integer.parseInt(args[i]);
      }
      else if (s.equals("-driverif")) {
        i++; if (i >= args.length) { usage(); }
        driverCallbackIp = args[i];
      }
      else if (s.equals("-driverport")) {
        i++; if (i >= args.length) { usage(); }
        driverCallbackPort = Integer.parseInt(args[i]);
      }
      else if (s.equals("-network")) {
        i++; if (i >= args.length) { usage(); }
        network = args[i];
      }
      else if (s.equals("-timeout")) {
        i++; if (i >= args.length) { usage(); }
        cloudFormationTimeoutSeconds = Integer.parseInt(args[i]);
      }
      else if (s.equals("-disown")) {
        disown = true;
      }
      else if (s.equals("-notify")) {
        i++; if (i >= args.length) { usage(); }
        clusterReadyFileName = args[i];
      }
      else if (s.equals("-nthreads")) {
        i++; if (i >= args.length) { usage(); }
        nthreads = Integer.parseInt(args[i]);
      }
      else if (s.equals("-baseport")) {
        i++; if (i >= args.length) { usage(); }
        basePort = Integer.parseInt(args[i]);
	if ((basePort < 0) || (basePort > 65535)) {
	    error("Base port must be between 1 and 65535");
	}
      }
      else if (s.equals("-beta")) {
        beta = true;
      }
      else if (s.equals("-ea")) {
        enableExceptions = true;
      }
      else {
        error("Unrecognized option " + s);
      }

      i++;
    }

    // Check for mandatory arguments.
    if (numNodes < 1) {
      error("Number of H2O nodes must be greater than 0 (must specify -n)");
    }
    if (outputPath == null) {
      error("Missing required option -output");
    }
    if (mapperXmx == null) {
      error("Missing required option -mapperXmx");
    }

    // Check for sane arguments.
    if (! mapperXmx.matches("[1-9][0-9]*[mgMG]")) {
      error("-mapperXmx invalid (try something like -mapperXmx 4g)");
    }

    if (extraMemPercent < 0) {
      extraMemPercent = DEFAULT_EXTRA_MEM_PERCENT;
    }

    if (jobtrackerName == null) {
      Random rng = new Random();
      int num = rng.nextInt(99999);
      jobtrackerName = "H2O_" + num;
    }

    if (network == null) {
      network = "";
    }
    else {
      String[] networks;
      if (network.contains(",")) {
        networks = network.split(",");
      }
      else {
        networks = new String[1];
        networks[0] = network;
      }

      for (int j = 0; j < networks.length; j++) {
        String n = networks[j];
        Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)/(\\d+)");
        Matcher m = p.matcher(n);
        boolean b = m.matches();
        if (! b) {
          error("network invalid: " + n);
        }

        for (int k = 1; k <=4; k++) {
          int o = Integer.parseInt(m.group(k));
          if ((o < 0) || (o > 255)) {
            error("network invalid: " + n);
          }

          int bits = Integer.parseInt(m.group(5));
          if ((bits < 0) || (bits > 32)) {
            error("network invalid: " + n);
          }
        }
      }
    }

    if (network == null) {
      error("Internal error, network should not be null at this point");
    }

    if ((nthreads >= 0) && (nthreads < 4)) {
      error("nthreads invalid (must be >= 4): " + nthreads);
    }
  }

  static String calcMyIp() throws Exception {
    Enumeration nis = NetworkInterface.getNetworkInterfaces();

    System.out.println("Determining driver host interface for mapper->driver callback...");
    while (nis.hasMoreElements()) {
      NetworkInterface ni = (NetworkInterface) nis.nextElement();
      Enumeration ias = ni.getInetAddresses();
      while (ias.hasMoreElements()) {
        InetAddress ia = (InetAddress) ias.nextElement();
        String s = ia.getHostAddress();
        System.out.println("    [Possible callback IP address: " + s + "]");
      }
    }

    InetAddress ia = InetAddress.getLocalHost();
    String s = ia.getHostAddress();

    return s;
  }

  private int waitForClusterToComeUp() throws Exception {
    long startMillis = System.currentTimeMillis();
    while (true) {
      if (clusterFailedToComeUp) {
        System.out.println("ERROR: At least one node failed to come up during cluster formation");
        job.killJob();
        return 4;
      }

      if (job.isComplete()) {
        break;
      }

      if (clusterIsUp) {
        break;
      }

      long nowMillis = System.currentTimeMillis();
      long deltaMillis = nowMillis - startMillis;
      if (cloudFormationTimeoutSeconds > 0) {
        if (deltaMillis > (cloudFormationTimeoutSeconds * 1000)) {
          System.out.println("ERROR: Timed out waiting for H2O cluster to come up (" + cloudFormationTimeoutSeconds + " seconds)");
          System.out.println("ERROR: (Try specifying the -timeout option to increase the waiting time limit)");
          job.killJob();
          return 3;
        }
      }

      final int ONE_SECOND_MILLIS = 1000;
      Thread.sleep (ONE_SECOND_MILLIS);
    }

    return 0;
  }

  private void waitForClusterToShutdown() throws Exception {
    while (true) {
      if (job.isComplete()) {
        break;
      }

      final int ONE_SECOND_MILLIS = 1000;
      Thread.sleep (ONE_SECOND_MILLIS);
    }
  }

  private int run2(String[] args) throws Exception {
    // Parse arguments.
    // ----------------
    parseArgs (args);

    // Set up callback address and port.
    // ---------------------------------
    if (driverCallbackIp == null) {
      driverCallbackIp = calcMyIp();
    }
    driverCallbackSocket = new ServerSocket();
    driverCallbackSocket.setReuseAddress(true);
    InetSocketAddress sa = new InetSocketAddress(driverCallbackIp, driverCallbackPort);
    driverCallbackSocket.bind(sa, driverCallbackPort);
    int actualDriverCallbackPort = driverCallbackSocket.getLocalPort();
    CallbackManager cm = new CallbackManager();
    cm.setServerSocket(driverCallbackSocket);
    cm.start();
    System.out.println("Using mapper->driver callback IP address and port: " + driverCallbackIp + ":" + actualDriverCallbackPort);
    System.out.println("(You can override these with -driverif and -driverport.)");

    // Set up configuration.
    // ---------------------
    Configuration conf = getConf();

    if (h2odriver_config.usingYarn()) {
      System.out.println("Driver program compiled with MapReduce V2 (Yarn)");
    }
    else {
      System.out.println("Driver program compiled with MapReduce V1 (Classic)");
    }

    // Set memory parameters.
    {
      Pattern p = Pattern.compile("([1-9][0-9]*)([mgMG])");
      Matcher m = p.matcher(mapperXmx);
      boolean b = m.matches();
      if (b == false) {
        System.out.println("(Could not parse mapperXmx.");
        System.out.println("INTERNAL FAILURE.  PLEASE CONTACT TECHNICAL SUPPORT.");
        System.exit(1);
      }
      assert (m.groupCount() == 2);
      String number = m.group(1);
      String units = m.group(2);
      long megabytes = Long.parseLong(number);
      if (units.equals("g") || units.equals("G")) {
        megabytes = megabytes * 1024;
      }

      // YARN container must be sized greater than Xmx.
      // YARN will kill the application if the RSS of the process is larger than
      // mapreduce.map.memory.mb.
      long jvmInternalMemoryMegabytes = (long) ((double)megabytes * ((double)extraMemPercent)/100.0);
      long processTotalPhysicalMemoryMegabytes = megabytes + jvmInternalMemoryMegabytes;
      conf.set("mapreduce.job.ubertask.enable", "false");
      String mapreduceMapMemoryMb = Long.toString(processTotalPhysicalMemoryMegabytes);
      conf.set("mapreduce.map.memory.mb", mapreduceMapMemoryMb);

      // MRv1 standard options, but also required for YARN.
      String mapChildJavaOpts = "-Xms" + mapperXmx + " -Xmx" + mapperXmx + (enableExceptions ? " -ea" : "");
      conf.set("mapred.child.java.opts", mapChildJavaOpts);
      conf.set("mapred.map.child.java.opts", mapChildJavaOpts);       // MapR 2.x requires this.

      System.out.println("Memory Settings:");
      System.out.println("    mapred.child.java.opts:      " + mapChildJavaOpts);
      System.out.println("    mapred.map.child.java.opts:  " + mapChildJavaOpts);
      System.out.println("    Extra memory percent:        " + extraMemPercent);
      System.out.println("    mapreduce.map.memory.mb:     " + mapreduceMapMemoryMb);
    }

    // Sometimes for debugging purposes, it helps to jam stuff in to the Java command
    // of the mapper child.
    //
    //        conf.set("mapred.child.java.opts", "-Dh2o.FINDME=ignored");
    //        conf.set("mapred.map.child.java.opts", "-Dh2o.FINDME2=ignored");
    //        conf.set("mapred.map.child.java.opts", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8999");

    // This is really silly, but without this line, the following ugly warning
    // gets emitted as the very first line of output, which is confusing for
    // the user.
    // Generic options parser is used automatically by ToolRunner, but somehow
    // that framework is not smart enough to disable the warning.
    //
    // Eliminates this runtime warning!
    // "WARN mapred.JobClient: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same."
    conf.set("mapred.used.genericoptionsparser", "true");

    // We don't want hadoop launching extra nodes just to shoot them down.
    // Not good for in-memory H2O processing!
    conf.set("mapreduce.map.speculative", "false");
    conf.set("mapred.map.tasks.speculative.execution", "false");

    conf.set("mapred.map.max.attempts", "1");
    conf.set("mapred.job.reuse.jvm.num.tasks", "1");
    conf.set(h2omapper.H2O_JOBTRACKERNAME_KEY, jobtrackerName);

    conf.set(h2omapper.H2O_DRIVER_IP_KEY, driverCallbackIp);
    conf.set(h2omapper.H2O_DRIVER_PORT_KEY, Integer.toString(actualDriverCallbackPort));
    conf.set(h2omapper.H2O_NETWORK_KEY, network);
    if (nthreads >= 0) {
        conf.set(h2omapper.H2O_NTHREADS_KEY, Integer.toString(nthreads));
    }
    if (basePort >= 0) {
        conf.set(h2omapper.H2O_BASE_PORT_KEY, Integer.toString(basePort));
    }
    if (beta) {
        conf.set(h2omapper.H2O_BETA_KEY, "-beta");
    }

    // Set up job stuff.
    // -----------------
    job = new Job(conf, jobtrackerName);
    job.setJarByClass(getClass());
    job.setInputFormatClass(H2OInputFormat.class);
    job.setMapperClass(h2omapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path("ignored"));
    if (outputPath != null) {
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    // Run job.  We are running a zero combiner and zero reducer configuration.
    // ------------------------------------------------------------------------
    job.submit();
    System.out.println("Job name '" + jobtrackerName + "' submitted");
    System.out.println("JobTracker job ID is '" + job.getJobID() + "'");

    // Register ctrl-c handler to try to clean up job when possible.
    ctrlc = new CtrlCHandler();
    Runtime.getRuntime().addShutdownHook(ctrlc);

    System.out.printf("Waiting for H2O cluster to come up...\n", numNodes);
    int rv = waitForClusterToComeUp();
    if (rv != 0) {
      System.out.println("ERROR: H2O cluster failed to come up");
      return rv;
    }

    if (job.isComplete()) {
      System.out.println("ERROR: H2O cluster failed to come up");
      ctrlc.setComplete();
      return 2;
    }

    System.out.printf("H2O cluster (%d nodes) is up\n", numNodes);
    if (disown) {
      // Do a short sleep here just to make sure all of the cloud
      // status stuff in H2O has settled down.
      Thread.sleep(CLOUD_FORMATION_SETTLE_DOWN_SECONDS);

      System.out.println("Disowning cluster and exiting.");
      Runtime.getRuntime().removeShutdownHook(ctrlc);
      return 0;
    }

    System.out.println("(Note: Use the -disown option to exit the driver after cluster formation)");
    System.out.println("(Press Ctrl-C to kill the cluster)");
    System.out.println("Blocking until the H2O cluster shuts down...");
    waitForClusterToShutdown();
    ctrlc.setComplete();
    boolean success = job.isSuccessful();
    int exitStatus;
    exitStatus = success ? 0 : 1;
    System.out.println((success ? "" : "ERROR: ") + "Job was" + (success ? " " : " not ") + "successful");
    if (success) {
      System.out.println("Exiting with status 0");
    }
    else {
      System.out.println("Exiting with nonzero exit status");
    }
    return exitStatus;
  }

  /**
   * The run method called by ToolRunner.
   * @param args Arguments after ToolRunner arguments have been removed.
   * @return Exit value of program.
   * @throws Exception
   */
  @Override
  public int run(String[] args) {
    int rv = -1;

    try {
      rv = run2(args);
    }
    catch (org.apache.hadoop.mapred.FileAlreadyExistsException e) {
      if (ctrlc != null) { ctrlc.setComplete(); }
      System.out.println("ERROR: " + (e.getMessage() != null ? e.getMessage() : "(null)"));
      System.exit(1);
    }
    catch (Exception e) {
      System.out.println("ERROR: " + (e.getMessage() != null ? e.getMessage() : "(null)"));
      e.printStackTrace();
      System.exit(1);
    }

    return rv;
  }

  /**
   * Main entry point
   * @param args Full program args, including those that go to ToolRunner.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new h2odriver(), args);
    System.exit(exitCode);
  }
}
