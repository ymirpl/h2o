import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

initList = [
        ('r0.hex', 'r0.hex=c(1.3,0,1,2,3,4,5)'),
        ('r1.hex', 'r1.hex=c(2.3,0,1,2,3,4,5)'),
        ('r2.hex', 'r2.hex=c(3.3,0,1,2,3,4,5)'),
        ('r3.hex', 'r3.hex=c(4.3,0,1,2,3,4,5)'),
        ('r4.hex', 'r4.hex=c(5.3,0,1,2,3,4,5)'),
        ('r.hex', 'r.hex=i.hex'),
        ('z.hex', 'z.hex=c(0)'),
        ]

exprListFull = [
        'x= 3; r.hex[(x > 0) & (x < 4),]',    # all x values between 0 and 1
        'x= 3; r.hex[,(x > 0) & (x < 4)]',    # all x values between 0 and 1
        # 'z = if (any(r3.hex == 0) || any(r4.hex == 0)), "zero encountered"',

        # FALSE and TRUE don't exist?
        # 'x <- c(NA, FALSE, TRUE)',

        # 'names(x) <- as.character(x)'
        # outer(x, x, "&")## AND table
        # outer(x, x, "|")## OR  table
        "1.23",
        "!1.23",

        "1.23<2.34",
        "!1.23<2.34",
        "!1.23<!2.34",
        "1.23<!2.34",

        "1.23<=2.34",
        "1.23>2.34",
        "1.23>=2.34",
        "1.23==2.34",
        "1.23!=2.34",

        "r.hex",
        "!r.hex",

        # Not supported
        # "+(1.23,2.34)",
        "x=0; x+2",
        "x=!0; !x+2",
        "x=!0; x+!2",

        "x=1",
        "x=!1",

        "x<-1",
        "x<-!1",
        "c(1,3,5)",
        "!c(1,3,5)",
        "!c(!1,3,5)",
        "!c(1,!3,5)",
        "!c(1,3,!5)",

        "a=0; x=0",
        "a=!0; x=!0",

        "r.hex[2,3]",
        # "r.hex[!2,3]",
        # no cols selectd
        # "r.hex[2,!3]",

        "r.hex[2+4,-4]",
        "r.hex[1,-1]; r.hex[1,-1]; r.hex[1,-1]",
        "r.hex[1,]",
        "r.hex+1",
        "r.hex[,1]",
        "r.hex[,1]+1",

        "r.hex-r.hex",
        "1.23+(r.hex-r.hex)",
        "(1.23+r.hex)-r.hex",

        "is.na(r.hex)",

        "nrow(r.hex)*3",
        "r.hex[nrow(r.hex)-1,ncol(r.hex)-1]",
        "r.hex[nrow(r.hex),]",
        "a=ncol(r.hex); r.hex[,c(a+1,a+2)]=5",
        "r.hex[,ncol(r.hex)+1]=4",
        "r.hex[,1]=3.3; r.hex",
        "r.hex[,1]=r.hex[,1]+1",

        "function(x,y,z){x[]}(r.hex,1,2)",
        "function(x){x+1}(2)",
        "function(x){y=x*2; y+1}(2)",
        "function(x){y=1+2}(2)",
        "function(funy){function(x){funy(x)*funy(x)}}(sgn)(-2)",
        "a=1; a=2; function(x){x=a;a=3}",
        "a=r.hex; function(x){x=a;a=3;nrow(x)*a}(a)",

        "apply(r.hex,2,sum)",
        "apply(r.hex,2,function(x){ifelse(x==-1,1,x)})",

        # doesn't work
        # "cbind(c(1), c(2), c(3))",
        # "cbind(c(1,2,3), c(4,5,6))",
        # "cbind(c(1,2,3), c(4,5,6), c(7,8,9))",
        # "cbind(c(1,2,3,4), c(5,6,7))",
        # "cbind(c(1,2,3), c(4,5,6,7))",
        "cbind(c(1,2,3,4), c(5,6,7,8))",

        "r.hex[c(1,3,5),]",
        "a=c(11,22,33,44,55,66); a[c(2,6,1),]",
        "r.hex[r.hex[,1]>4,]",
        # fails?
        # "a=c(1,2,3); a[a[,1]>10,1]",

        "ifelse(0,1,2)",
        "ifelse(0,r.hex+1,r.hex+2)",
        "ifelse(r.hex>3,99,r.hex)",
        "ifelse(0,+,*)(1,2)",

        "(0 ? + : *)(1,2)",
        "(1 ? r.hex : (r.hex+1))[1,2]",

        "sum(1,2)",
        "sum(1,2,3)",
        "sum(c(1,3,5))",
        "sum(4,c(1,3,5),2,6)",
        "sum(1,r.hex,3)",

        "min(1,2)",
        # doesn't work
        # "min(1,2,3)",
        # doesn't work. only 2 params?
        # "min(c(1,3,5))",
        # doesn't work. only 2 params?
        # "min(4,c(1,3,5),2,6)",
        # doesn't work
        # "min(1,r.hex,3)",

        "max(1,23)",
        # doesn't work
        # Passed 3 args but expected 2
        # "max(1,2,3)",
        # doesn't work
        # "max(c(1,3,5))",

        # doesn't work
        # Passed 4 args but expected 2
        # "max(4,c(1,3,5),2,6)",
        # doesn't work
        # "max(1,r.hex,3)",

        "factor(r.hex[,5])",
        "r0.hex[,1]==1.0",
        "runif(r4.hex[,1])",
    
        # doesn't work
        "mean=function(x){apply(x,1,sum)/nrow(x)};mean(r.hex)",

        ]


# concatenate some random choices to make life harder
exprList = []
for i in range(100):
    expr = ""
    for j in range(1):
        expr += random.choice(exprListFull)
    exprList.append(expr)
        

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_operators2(self):
        bucket = 'smalldata'
        csvPathname = 'iris/iris2.csv'
        hexKey = 'i.hex'
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        for resultKey, execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey=resultKey, timeoutSecs=4)
        start = time.time()
        h2e.exec_expr_list_rand(len(h2o.nodes), exprList, None, maxTrials=200, timeoutSecs=10)

        # now run them just concatenating each time. We don't do any template substitutes, so don't need
        # exec_expr_list_rand()
        
        bigExecExpr = ""
        for execExpr in exprList:
            bigExecExpr += execExpr + ";"
            h2e.exec_expr(h2o.nodes[0], bigExecExpr, resultKey=None, timeoutSecs=4)

        h2o.check_sandbox_for_errors()
        print "exec end on ", "operators" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
