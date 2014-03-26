import unittest, time, sys, random, math
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_browse as h2b, h2o_util, h2o_import as h2i

# a truly uniform sphere
# http://stackoverflow.com/questions/5408276/python-uniform-spherical-distribution
# he offers the exact solution: http://stackoverflow.com/questions/918736/random-number-generator-that-produces-a-power-law-distribution/918782#918782
# In spherical coordinates, taking advantage of the sampling rule:
# http://stackoverflow.com/questions/2106503/pseudorandom-number-generator-exponential-distribution/2106568#2106568
CLUSTERS = 8
SPHERE_PTS = 10000
RANDOMIZE_SPHERE_PTS = True
DIMENSIONS = 4 # 1,2 or 3
JUMP_RANDOM_ALL_DIRS = True
# should do this, but does it make h2o kmeans fail?
SHUFFLE_SPHERES = True
R_NOISE = True
ALLOWED_CENTER_DELTA = 3

def get_xyz_sphere(R):
    u = random.random() # 0 to 1
    # add a little noise
    r = R * (u ** (1.0/3))
    if R_NOISE:
        rNoise = random.random() * .1 * r
        r += rNoise

    costheta = random.uniform(-1,1)
    theta = math.acos(costheta)

    phi = random.uniform(0, 2 * math.pi)
    # now you have a (r, theta, phi) group which can be transformed to (x, y, z) 
    x = r * math.sin(theta) * math.cos(phi)
    y = r * math.sin(theta) * math.sin(phi)
    z = r * math.cos(theta) 
    # use these for jump dimensions? (picture "time" and other dimensions)
    zz = 0
    yy = 0
    xyzzy = [x, y, z, zz, yy]
    ### print xyz[:DIMENSIONS]
    return xyzzy[:DIMENSIONS]

def write_spheres_dataset(csvPathname, CLUSTERS, n):
    dsf = open(csvPathname, "w+")

    # going to do a bunch of spheres, with differing # of pts and R
    # R is radius of the spheres
    # separate them by 3 * the previous R
    # keep track of the centers so we compare to a sorted result from H2O
    print "To keep life interesting:"
    print "make the multiplier, between 3 and 9 in just one direction"
    print "pick x, y, or z direction randomly"
    print "We tack the centers created, and assert against the H2O results, so 'correct' is checked"
    print "Increasing radius for each basketball. (R)"
    centersList = []
    currentCenter = None
    totalRows = 0
    for sphereCnt in range(CLUSTERS):
        R = 10 * (sphereCnt+1)
        if JUMP_RANDOM_ALL_DIRS:
            jump = random.randint(10*R,(10*R)+10)
            xyzChoice = random.randint(0,DIMENSIONS-1)
        else:
            jump = 10*R
            if DIMENSIONS==5:
                # limit jumps to yy
                xyzChoice = 4
            elif DIMENSIONS==3:
                # limit jumps to z
                # xyzChoice = 2
                xyzChoice = 0
            else:
                # limit jumps to x
                xyzChoice = 0

        zeroes = [0] * DIMENSIONS
        newOffset = zeroes
        # FIX! problems if we don't jump the other dimensions?
        # try jumping in all dimensions
        # newOffset[xyzChoice] = jump
        newOffset = [jump] * DIMENSIONS

        # figure out the next center
        if currentCenter is None:
            currentCenter = zeroes
        else:
            lastCenter = currentCenter
            currentCenter  = [a+b for a,b in zip(currentCenter, newOffset)] 
            if (sum(currentCenter) - sum(lastCenter) < (len(currentCenter)* ALLOWED_CENTER_DELTA)):
                print "ERROR: adjacent centers are too close for our sort algorithm"
                print "currentCenter:", currentCenter, "lastCenter:", lastCenter
                raise Exception
                
        centersList.append(currentCenter)

        # build a sphere at that center
        # fixed number of pts?
        if RANDOMIZE_SPHERE_PTS:
            # pick a random # of points, from .5n to 1.5n
            numPts = random.randint(int(.5*n), int(1.5*n))
        else:
            numPts = n

        print "currentCenter:", currentCenter, "R:", R, "numPts", numPts
        for i in range(numPts):
            xyz = get_xyz_sphere(R)
            xyzShifted  = [a+b for a,b in zip(xyz,currentCenter)] 
            dsf.write(",".join(map(str,xyzShifted))+"\n")
            totalRows += 1

    dsf.close()
    print "Spheres created:", len(centersList), "totalRows:", totalRows
    return centersList


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=7)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_sphere100(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = 'syn_spheres100.csv'
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        centersList = write_spheres_dataset(csvPathname, CLUSTERS, SPHERE_PTS)

        if SHUFFLE_SPHERES:
            # since we create spheres in order
            csvFilename2 = 'syn_spheres100_shuffled.csv'
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename2
            h2o_util.file_shuffle(csvPathname, csvPathname2)
        else:
            csvFilename2 = csvFilename
            csvPathname2 = csvPathname

        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(path=csvPathname2, schema='put', hex_key=csvFilename2 + ".hex")

        ### h2b.browseTheCloud()

        # try 5 times, to see if all inits by h2o are good
        # does it break if cols is not specified?
        cols = ",".join(map(str,range(DIMENSIONS)))
        for trial in range(10):
            kwargs = {
                'k': CLUSTERS, 
                'initialization': 'Furthest', 
                'cols': cols,
                'destination_key': 'syn_spheres100.hex'
            }
            timeoutSecs = 100
            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.',\
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            kmeansResult = h2o_cmd.runInspect(key='syn_spheres100.hex')
            # print h2o.dump_json(kmeansResult)

            ### print h2o.dump_json(kmeans)
            ### print h2o.dump_json(kmeansResult)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

            # cluster centers can return in any order
            clusters = kmeansResult['KMeansModel']['clusters']

            # the way we create the centers above, if we sort on the sum of xyz
            # we should get the order the same as when they were created.
            # to be safe, we'll sort the centers that were generated too, the same way
            clustersSorted = sorted(clusters, key=sum)
            centersSorted  = sorted(centersList, key=sum)
            ### print clustersSorted

            print "\ntrial #", trial, "h2o result, centers (sorted by key=sum)"
            cf = '{0:6.2f}'
            for c in clustersSorted:
                print ' '.join(map(cf.format,c))

            print "\ngenerated centers (sorted by key=sum)"
            for c in centersSorted:
                print ' '.join(map(cf.format,c))
            
            for i,center in enumerate(centersSorted):
                # Doing the compare of gen'ed/actual centers is kind of a hamming distance problem.
                # Assuming that the difference between adjacent sums of all center values, 
                # is greater than 2x the sum of all max allowed variance on each value, 
                # Then the sums will be unique and non-overlapping with allowed variance.
                # So a sort of the centers, keyed on sum of all values for a center.
                # will create an ordering that can be compared. 
                # sort gen'ed and actual separately.
                # Adjacent center hamming distance check is done during gen above.
                a = center
                b = clustersSorted[i]
                print "\nexpected:", a
                print "h2o:", b # h2o result
                aStr = ",".join(map(str,a))
                bStr = ",".join(map(str,b))
                iStr = str(i)

                for i, v in enumerate(a):
                    emsg = aStr+" != "+bStr+". Sorted cluster center "+iStr+" axis "+str(i)+" not correct."
                    self.assertAlmostEqual(a[i], b[i], delta=ALLOWED_CENTER_DELTA, msg=emsg)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
