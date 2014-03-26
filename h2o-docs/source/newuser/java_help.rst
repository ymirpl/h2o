H\ :sub:`2`\ O Command-line Options
""""""""""""""""""""""""""""""""""""
When an instance of H\ :sub:`2`\ O is started from the command line, users
generally call a java command similar to "java -Xmx1g -jar
h2o.jar " Users can customize their running
instance of H\ :sub:`2`\ O by changing options in this java command. Each of the
options has a default. 

**Help in the terminal:**

  Users can access help while working in the terminal by calling 
  **java -jar h2o.jar -help** in the working directory where their H\ :sub:`2`\ O
  jar is located. 

**Memory Allocation** 

  In **java [-Xmx<size>] -jar h2o.jar [options]** users specify the
  amount of memory allocated to a particular node by specifying a
  number in place of the indicator *<size>*. If your data set is
  large, give H\ :sub:`2`\ O more memory (for example, -Xmx4g gives H\ :sub:`2`\ O four
  gigabytes of memory).  For best performance, Xmx should be 4x the
  size of your data, but never more than the total amount of memory on
  the machine where H\ :sub:`2`\ O is running.

**Where to Place Options**

  There are two places in the java command where options can be specified. 
  In the call options should be inserted as follows:
  **java -Xmx<size> [ADDITIONAL JVM OPTIONS GO HERE] -jar h2o.jar [H\ :sub:`2`\ O OPTIONS GO HERE]**


JVM Options
-----------

The common options are listed below; however, H\ :sub:`2`\ O is designed to not require many flags. 
Advanced and curious users may find http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html
helpful for defining and describing other common java command options. 

**-version**
    
  Print java version info and exit.

**-Xmx[totalheapsize]**

  Sets the total heap size for the node of H\ :sub:`2`\ O.


H\ :sub:`2`\ O Options
----------------------- 

**-flatfile <FlatFileName>**
    
  Configuration flat file explicitly listing H\ :sub:`2`\ O cloud node members. 
  
**-h | -help**
          
  Print this help.

**-ice_root <fileSystemPath>**
    
  The directory where H\ :sub:`2`\ O spills temporary data to disk.
  (The default is '/tmp/h2o-User'.)
  
**-ip <ipAddressOfNode>**
    
  IP address of this node.

**-name <h2oCloudName>**

  Cloud name used for discovery of other nodes.
  Nodes with the same cloud name will form an H\ :sub:`2`\ O cloud
  (also known as an H\ :sub:`2`\ O cluster).

**-network <IPv4network1Specification>[,<IPv4network2Specification> …]**
    
  The IP address discovery code will bind to the first interface
  that matches one of the networks in the comma-separated list.
  Use instead of -ip when a broad range of addresses is legal.
  (Example network specification: '10.1.2.0/24' allows 256 legal
  possibilities.)

**-nthreads <number>**

  Maximum number of typical worker threads.  Think of this as the 
  maximum number of CPUs H\ :sub:`2`\ O will use.  This value is
  per java instance.  Specifying too small a value may result in
  deadlocks.  Never specify fewer than 4.  The default value is 99.

**-port <port>**

  Port number for this node (note: port+1 is also used).
  (The default port is 54321.)


Cloud formation behavior
------------------------

New H\ :sub:`2`\ O nodes join together to form a cloud at startup time.
Once a cloud is given work to perform, it locks out new members
from joining. H\ :sub:`2`\ O works best on distributed multinode clusters
when the clusters are similar in configuration, and allocated
equal amounts of memory. 

**Examples:**

  Start an H\ :sub:`2`\ O node with 4GB of memory and a *default cloud name:*
      $ java -Xmx4g -jar h2o.jar

  Start an H\ :sub:`2`\ O node with 6GB of memory and a *specified cloud name:*
      $ java -Xmx6g -jar h2o.jar -name TomsCloud

  Start an H\ :sub:`2`\ O cloud with three 2GB nodes and a *specified cloud name:*
      $ java -Xmx2g -jar h2o.jar -name TomsCloud
      $ java -Xmx2g -jar h2o.jar -name TomsCloud
      $ java -Xmx2g -jar h2o.jar -name TomsCloud
