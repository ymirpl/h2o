package water.util;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Linux /proc file reader.
 *
 * Read tick information for the system and the current process in order to provide
 * stats on the cloud page about CPU utilization.
 *
 * Tick counts are monotonically increasing since boot.
 *
 * Find definitions of /proc file info here.
 * http://man7.org/linux/man-pages/man5/proc.5.html
 */
public class LinuxProcFileReader {
  private String _systemData;
  private String _processData;

  private long _systemIdleTicks = -1;
  private long _systemTotalTicks = -1;
  private long _processTotalTicks = -1;

  private int _processNumOpenFds = -1;

  /**
   * Constructor.
   */
  public LinuxProcFileReader() {
  }

  /**
   * @return ticks the system was idle.  in general:  idle + busy == 100%
   */
  public long getSystemIdleTicks()   { assert _systemIdleTicks > 0;    return _systemIdleTicks; }

  /**
   * @return ticks the system was up.
   */
  public long getSystemTotalTicks()  { assert _systemTotalTicks > 0;   return _systemTotalTicks; }

  /**
   * @return ticks this process was running.
   */
  public long getProcessTotalTicks() { assert _processTotalTicks > 0;  return _processTotalTicks; }

  /**
   * @return number of currently open fds of this process.
   */
  public int getProcessNumOpenFds() { assert _processNumOpenFds > 0;  return _processNumOpenFds; }

  /**
   * Read and parse data from /proc/stat and /proc/<pid>/stat.
   * If this doesn't work for some reason, the values will be -1.
   */
  public void read() {
    File f = new File ("/proc/stat");
    if (! f.exists()) {
      return;
    }

    String pid;
    try {
      pid = getProcessId();

      readSystemProcFile();
      readProcessProcFile(pid);
      readProcessNumOpenFds(pid);
      parseSystemProcFile(_systemData);
      parseProcessProcFile(_processData);
    }
    catch (Exception _) {}
  }

  /**
   * @return true if all the values are ok to use; false otherwise.
   */
  public boolean valid() {
    return ((_systemIdleTicks >= 0) && (_systemTotalTicks >= 0) && (_processTotalTicks >= 0) &&
            (_processNumOpenFds >= 0));
  }

  private static String getProcessId() throws Exception {
    // Note: may fail in some JVM implementations
    // therefore fallback has to be provided

    // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
    final String jvmName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    final int index = jvmName.indexOf('@');

    if (index < 1) {
      // part before '@' empty (index = 0) / '@' not found (index = -1)
      throw new Exception ("Can't get process Id");
    }

    return Long.toString(Long.parseLong(jvmName.substring(0, index)));
  }

  private String readFile(File f) throws Exception {
    char[] buffer = new char[16 * 1024];
    FileReader fr = new FileReader(f);
    int bytesRead = 0;
    while (true) {
      int n = fr.read(buffer, bytesRead, buffer.length - bytesRead);
      if (n < 0) {
        fr.close();
        return new String (buffer, 0, bytesRead);
      }
      else if (n == 0) {
        // This is weird.
        fr.close();
        throw new Exception("LinuxProcFileReader readFile read 0 bytes");
      }

      bytesRead += n;

      if (bytesRead >= buffer.length) {
        fr.close();
        throw new Exception("LinuxProcFileReader readFile unexpected buffer full");
      }
    }
  }

  private void readSystemProcFile() {
    try {
      _systemData = readFile(new File("/proc/stat"));
    }
    catch (Exception _) {}
  }

  /**
   * @param s String containing contents of proc file.
   */
  private void parseSystemProcFile(String s) {
    if (s == null) return;

    try {
      BufferedReader reader = new BufferedReader(new StringReader(s));
      String line = reader.readLine();

      Pattern p = Pattern.compile("cpu\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+).*");
      Matcher m = p.matcher(line);
      boolean b = m.matches();
      if (! b) {
        return;
      }

      long systemUserTicks   = Long.parseLong(m.group(1));
      long systemNiceTicks   = Long.parseLong(m.group(2));
      long systemSystemTicks = Long.parseLong(m.group(3));
      _systemIdleTicks   = Long.parseLong(m.group(4));
      _systemTotalTicks = systemUserTicks + systemNiceTicks + systemSystemTicks + _systemIdleTicks;
    }
    catch (Exception _) {}
  }

  private void readProcessProcFile(String pid) {
    try {
      String s = "/proc/" + pid + "/stat";
      _processData = readFile(new File(s));
    }
    catch (Exception _) {}
  }

  private void parseProcessProcFile(String s) {
    if (s == null) return;

    try {
      BufferedReader reader = new BufferedReader(new StringReader(s));
      String line = reader.readLine();

      Pattern p = Pattern.compile("(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+).*");
      Matcher m = p.matcher(line);
      boolean b = m.matches();
      if (! b) {
        return;
      }

      long processUserTicks   = Long.parseLong(m.group(14));
      long processSystemTicks   = Long.parseLong(m.group(15));
      _processTotalTicks = processUserTicks + processSystemTicks;
    }
    catch (Exception _) {}
  }

  private void readProcessNumOpenFds(String pid) {
    try {
      String s = "/proc/" + pid + "/fd";
      File f = new File(s);
      String[] arr = f.list();
      if (arr != null) {
        _processNumOpenFds = arr.length;
      }
    }
    catch (Exception _) {}
  }

  /**
   * Main is purely for command-line testing.
   */
  public static void main(String[] args) {
    final String sysTestData =
            "cpu  43559117 24094 1632164 1033740407 245624 29 200080 0 0 0\n"+
                    "cpu0 1630761 1762 62861 31960072 40486 15 10614 0 0 0\n"+
                    "cpu1 1531923 86 62987 32118372 13190 0 6806 0 0 0\n"+
                    "cpu2 1436788 332 66513 32210723 10867 0 6772 0 0 0\n"+
                    "cpu3 1428700 1001 64574 32223156 8751 0 6811 0 0 0\n"+
                    "cpu4 1424410 152 62649 32232602 6552 0 6836 0 0 0\n"+
                    "cpu5 1427172 1478 58744 32233938 5471 0 6708 0 0 0\n"+
                    "cpu6 1418433 348 60957 32241807 5301 0 6639 0 0 0\n"+
                    "cpu7 1404882 182 60640 32258150 3847 0 6632 0 0 0\n"+
                    "cpu8 1485698 3593 67154 32101739 38387 0 9016 0 0 0\n"+
                    "cpu9 1422404 1601 66489 32193865 15133 0 8800 0 0 0\n"+
                    "cpu10 1383939 3386 69151 32233567 11219 0 8719 0 0 0\n"+
                    "cpu11 1376904 3051 65256 32246197 8307 0 8519 0 0 0\n"+
                    "cpu12 1381437 1496 68003 32237894 6966 0 8676 0 0 0\n"+
                    "cpu13 1376250 1527 66598 32247951 7020 0 8554 0 0 0\n"+
                    "cpu14 1364352 1573 65520 32262764 5093 0 8531 0 0 0\n"+
                    "cpu15 1359076 1176 64380 32269336 5219 0 8593 0 0 0\n"+
                    "cpu16 1363844 6 29612 32344252 4830 2 4366 0 0 0\n"+
                    "cpu17 1477797 1019 70211 32190189 6278 0 3731 0 0 0\n"+
                    "cpu18 1285849 30 29219 32428612 3549 0 3557 0 0 0\n"+
                    "cpu19 1272308 0 27306 32445340 2089 0 3541 0 0 0\n"+
                    "cpu20 1326369 5 29152 32386824 2458 0 4416 0 0 0\n"+
                    "cpu21 1320883 28 31886 32384709 2327 1 4869 0 0 0\n"+
                    "cpu22 1259498 1 26954 32458931 2247 0 3511 0 0 0\n"+
                    "cpu23 1279464 0 26694 32439550 1914 0 3571 0 0 0\n"+
                    "cpu24 1229977 19 32308 32471217 4191 0 4732 0 0 0\n"+
                    "cpu25 1329079 92 79253 32324092 5267 0 4821 0 0 0\n"+
                    "cpu26 1225922 30 34837 32475220 4000 0 4711 0 0 0\n"+
                    "cpu27 1261848 56 43928 32397341 3552 0 5625 0 0 0\n"+
                    "cpu28 1226707 20 36281 32463498 3935 4 5943 0 0 0\n"+
                    "cpu29 1379751 19 35593 32317723 2872 4 5913 0 0 0\n"+
                    "cpu30 1247661 0 32636 32455845 2033 0 4775 0 0 0\n"+
                    "cpu31 1219016 10 33804 32484916 2254 0 4756 0 0 0\n"+
                    "intr 840450413 1194 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 55 0 0 0 0 0 0 45 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 593665 88058 57766 41441 62426 61320 39848 39787 522984 116724 99144 95021 113975 99093 78676 78144 0 168858 168858 168858 162 2986764 4720950 3610168 5059579 3251008 2765017 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 00 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"+
                    "ctxt 1506565570\n"+
                    "btime 1385196580\n"+
                    "processes 1226464\n"+
                    "procs_running 21\n"+
                    "procs_blocked 0\n"+
                    "softirq 793917930 0 156954983 77578 492842649 1992553 0 7758971 51856558 228040 82206598\n";

    final String procTestData = "16790 (java) S 1 16789 16789 0 -1 4202496 6714145 0 0 0 4773058 5391 0 0 20 0 110 0 33573283 64362651648 6467228 18446744073709551615 1073741824 1073778376 140734614041280 140734614032416 140242897981768 0 0 3 16800972 18446744073709551615 0 0 17 27 0 0 0 0 0\n";

    LinuxProcFileReader lpfr = new LinuxProcFileReader();
    lpfr.parseSystemProcFile(sysTestData);
    lpfr.parseProcessProcFile(procTestData);
    System.out.println("System idle ticks: " + lpfr.getSystemIdleTicks());
    System.out.println("System total ticks: " + lpfr.getSystemTotalTicks());
    System.out.println("Process total ticks: " + lpfr.getProcessTotalTicks());
  }
}
