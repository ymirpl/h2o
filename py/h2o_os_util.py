
# since we hang if hosts has bad IP addresses, thought it'd be nice
# to have simple obvious feedback to user if he's running with -v 
# and machines are down or his hosts definition has bad IPs.
# FIX! currently not used
def ping_host_if_verbose(host):
    # if (h2o.verbose) 
    ping = subprocess.Popen( ["ping", "-c", "4", host])
    ping.communicate()


def check_port_group(base_port):
    # disabled
    if (1==0):
        import getpass
        username = getpass.getuser()
        if username=='jenkins' or username=='kevin' or username=='michal':
            # assumes you want to know about 3 ports starting at base_port
            command1Split = ['netstat', '-anp']
            command2Split = ['egrep']
            # colon so only match ports. space at end? so no submatches
            command2Split.append("(%s | %s)" % (base_port, base_port+1) )
            command3Split = ['wc','-l']

            print "Checking 2 ports starting at ", base_port
            print ' '.join(command2Split)

            # use netstat thru subprocess
            p1 = Popen(command1Split, stdout=PIPE)
            p2 = Popen(command2Split, stdin=p1.stdout, stdout=PIPE)
            output = p2.communicate()[0]
            print output

# I suppose we should use psutil here. since everyone has it installed?
# and it should work on windows?
def show_h2o_processes():
    import psutil
    # print "get_users:", psutil.get_users()
    print "total physical dram: %0.2f GB", (psutil.TOTAL_PHYMEM+0)/(1024*1024)
    print "max cpu threads:", psutil.NUM_CPUS

    print "\nReporting on h2o"
    users = set()
    h2oUsers = set()
    h2oFound = False
    for p in psutil.process_iter():
        h2oProcess = False
        if 'java' in p.name:
            users.add(p.username)
            # now iterate through the cmdline, to see if it's got 'h2o
            for c in p.cmdline:
                if 'h2o' in c: 
                    h2oProcess = True
                    h2oUsers.add(p.username)
                    break

        if h2oProcess:
            h2oFound = True
            print "\n#**********************************************"
            print p
            # process could disappear while we're looking? (fast h2o version java process?)
            try:
                print "pid:", p.pid
                print "cmdline:", p.cmdline
                # AccessDenied problem?
                # print p.getcwd()
                print "status:", p.status
                print "username:", p.username
                print "cpu_percent:", p.get_cpu_percent(interval=1.0)
                print "memory_percent:", p.get_memory_percent()
                print p.get_memory_info()
                # AccessDenied problem
                # print p.get_io_counters()
                # AccessDenied problem
                # p.get_open_files()
                # AccessDenied problem
                # print p.get_connections()
            except:
                pass
                

    if h2oFound:
        print "\n#**********************************************"
    else:
        print "No h2o processes found."
    print "users running java:", list(users)
    print "users running h2o java:", list(h2oUsers)

