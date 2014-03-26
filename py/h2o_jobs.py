import time, sys
import h2o, h2o_browse as h2b


def pollStatsWhileBusy(timeoutSecs=300, pollTimeoutSecs=15, retryDelaySecs=5):
    busy = True
    trials = 0

    start = time.time()
    polls = 0
    statSum = {}
    # just init for worst case 32 nodes?
    lastUsedMemBytes = [1 for i in range(32)]
    while busy:
        polls += 1
        # get utilization and print it
        # any busy jobs
        a = h2o.nodes[0].jobs_admin(timeoutSecs=10)
        busy = False
        for j in a['jobs']:
            if j['end_time']=='' and not (j['cancelled'] or (j['result'].get('val', None)=='CANCELLED')):
                busy = True
                h2o.verboseprint("Still busy")
                break

        cloudStatus = h2o.nodes[0].get_cloud(timeoutSecs=timeoutSecs)
        nodes = cloudStatus['nodes']
        for i,n in enumerate(nodes):

            # check for drop in tot_mem_bytes, and report as "probably post GC"
            totMemBytes = n['tot_mem_bytes']
            maxMemBytes = n['max_mem_bytes']
            freeMemBytes = n['free_mem_bytes']

            usedMemBytes = totMemBytes - freeMemBytes
            availMemBytes = maxMemBytes - usedMemBytes
            print 'Node %s:' % i, \
                'num_cpus:', n['num_cpus'],\
                'my_cpu_%:', n['my_cpu_%'],\
                'sys_cpu_%:', n['sys_cpu_%'],\
                'system_load:', n['system_load'],\
                'tot_mem_bytes: {:,}'.format(totMemBytes),\
                'max_mem_bytes: {:,}'.format(maxMemBytes),\
                'free_mem_bytes: {:,}'.format(freeMemBytes),\
                'usedMemBytes: {:,}'.format(usedMemBytes)

            decrease = round((0.0 + lastUsedMemBytes[i] - usedMemBytes) / lastUsedMemBytes[i], 3)
            if decrease > .05:
                print
                print "\nProbably GC at Node {:}: usedMemBytes decreased by {:f} pct.. {:,} {:,}".format(i, 100 * decrease, lastUsedMemBytes[i], usedMemBytes)
                lastUsedMemBytes[i] = usedMemBytes
            # don't update lastUsedMemBytes if we're decreasing
            if usedMemBytes > lastUsedMemBytes[i]:
                lastUsedMemBytes[i] = usedMemBytes
            
            # sum all individual stats
            for stat in n:
                if stat in statSum:
                    try: 
                        statSum[stat] += n[stat]
                    except TypeError:
                        # raise Exception("statSum[stat] should be number %s %s" % (statSum[stat], stat, n[stat]))
                        print "ERROR: statSum[stat] should be number %s %s %s" % (statSum[stat], stat, n[stat])
                        # do nothing
                else:
                    try: 
                        statSum[stat] = n[stat] + 0.0
                    except TypeError:
                        pass # ignore non-numbers

        trials += 1
        if trials%5 == 0:
            h2o.check_sandbox_for_errors()

        time.sleep(retryDelaySecs)
        if ((time.time() - start) > timeoutSecs):
            raise Exception("Timeout while polling in pollStatsWhileBusy: %s seconds" % timeoutSecs)
    

    # now print man 
    print "Did %s polls" % polls
    statMean = {}
    for s in statSum:
        statMean[s] = round((statSum[s] + 0.0) / polls, 2)
        print "mean", s + ':', statMean[s]

    return  statMean
    # statMean['tot_mem_bytes'],
    # statMean['num_cpus'],
    # statMean['my_cpu_%'],
    # statMean['sys_cpu_%'],
    # statMean['system_load']

# poll the Jobs queue and wait if not all done. 
# Return matching keys to a pattern for 'destination_key"
# for a job (model usually)

# FIX! the pattern doesn't limit the jobs you wait for (sounds like it does)
# I suppose it's rare that we'd want to wait for a subset of jobs, but lets 
# 'key' 'description' 'destination_key' could all be interesting things you want to pattern match agains?
# what the heck, just look for a match in any of the 3 (no regex)
# if pattern is not None, only stall on jobs that match the pattern (in any of those 3)

def pollWaitJobs(pattern=None, errorIfCancelled=False, timeoutSecs=30, pollTimeoutSecs=30, retryDelaySecs=5, benchmarkLogging=None, stallForNJobs=None):
    wait = True
    waitTime = 0
    ignoredJobs = set()
    while (wait):
        a = h2o.nodes[0].jobs_admin(timeoutSecs=pollTimeoutSecs)
        h2o.verboseprint("jobs_admin():", h2o.dump_json(a))
        jobs = a['jobs']
        busy = 0
        for j in jobs:
            cancelled = j['cancelled'] or (j['result'].get('val', None)=='CANCELLED')
            description = j['description']
            destination_key = j['destination_key']
            end_time = j['end_time']
            key = j['key']
            progress = j['progress']
            # has exception and val?
            result = j['result']
            start_time = j['start_time']

            # for now, don't ignore any exceptions
            if 'exception' in result and result['exception']:
                h2o.check_sandbox_for_errors()
                msg = "ERROR: pollWaitJobs found a job with a exception result when it shouldn't have:\n %s" % h2o.dump_json(j)
                raise Exception(msg)

            if result:
                # ignore if 'val' is 'OK'
                if 'val' in result and result['val'] == 'OK':
                    pass
                else:
                    print "non-empty result: %s for %s" % (result, key)

            if errorIfCancelled and cancelled:
                h2o.check_sandbox_for_errors()
                print ("ERROR: not stopping, but: pollWaitJobs found a cancelled job when it shouldn't have:\n %s" % h2o.dump_json(j))
                print ("Continuing so maybe a json response will give more info")
                
            ### h2o.verboseprint(j)
            # don't include cancelled jobs here
            elif end_time=='' and not cancelled:
                if not pattern: 
                    # always print progress if busy job (no pattern used
                    print "time:", time.strftime("%I:%M:%S"), "progress:",  progress, destination_key
                    h2o.verboseprint("description:", description, "end_time:", end_time)
                    busy +=1
                    h2o.verboseprint("pollWaitJobs: found a busy job, now: %s" % busy)
                else:
                    if (pattern in key) or (pattern in destination_key) or (pattern in description):
                        ## print "description:", description, "end_time:", end_time
                        busy += 1
                        h2o.verboseprint("pollWaitJobs: found a pattern-matched busy job, now %s" % busy)
                        # always print progress if pattern is used and matches
                        print "time:", time.strftime("%I:%M:%S"), "progress:",  progress, destination_key
                    # we only want to print the warning message once
                    elif key not in ignoredJobs:
                        jobMsg = "%s %s %s" % (key, description, destination_key)
                        h2o.verboseprint(" %s job in progress but we're ignoring it. Doesn't match pattern." % jobMsg)
                        # I guess "key" is supposed to be unique over all time for a job id?
                        ignoredJobs.add(key)

        if stallForNJobs:
            waitFor = stallForNJobs
        else:
            waitFor = 0

        print " %s jobs in progress." % busy, "Waiting until %s in progress." % waitFor
        wait = busy > waitFor
        if not wait:
            break

        ### h2b.browseJsonHistoryAsUrlLastMatch("Jobs")
        if (wait and waitTime > timeoutSecs):
            print h2o.dump_json(jobs)
            raise Exception("Some queued jobs haven't completed after", timeoutSecs, "seconds")

        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(retryDelaySecs)
        waitTime += retryDelaySecs

        # any time we're sitting around polling we might want to save logging info (cpu/disk/jstack)
        # test would pass ['cpu','disk','jstack'] kind of list
        if benchmarkLogging:
            h2o.cloudPerfH2O.get_log_save(benchmarkLogging)

        # check the sandbox for stack traces! just like we do when polling normally
        h2o.check_sandbox_for_errors()

    patternKeys = []
    for j in jobs:
        # save the destination keys in progress that match pattern (for returning)
        if pattern and pattern in j['destination_key']:
            patternKeys.append(j['destination_key'])

    return patternKeys

def showAllJobs():
    print "Showing all jobs"
    a = h2o.nodes[0].jobs_admin(timeoutSecs=10)
    print h2o.dump_json(a)

#*******************************************************************************************
def cancelAllJobs(timeoutSecs=10, **kwargs): # I guess you could pass pattern
    # what if jobs had just been dispatched? wait until they get in the queue state correctly
    time.sleep(2)
    a = h2o.nodes[0].jobs_admin(timeoutSecs=120)
    print "jobs_admin():", h2o.dump_json(a)
    jobsList = a['jobs']
    for j in jobsList:
        if j['end_time'] == '':
            b = h2o.nodes[0].jobs_cancel(key=j['key'])
            print "jobs_cancel():", h2o.dump_json(b)

    # it's possible we could be in a bad state where jobs don't cancel cleanly
    pollWaitJobs(timeoutSecs=timeoutSecs, **kwargs) # wait for all the cancels to happen. If we missed one, we might timeout here.
