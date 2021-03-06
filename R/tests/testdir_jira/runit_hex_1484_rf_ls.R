######################################################################
# Test for HEX-1484
# h2o.ls() fails as json response from Store view returns junk (/u0000)
######################################################################

# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_jira")

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
options(echo=TRUE)
source('../findNSourceUtils.R')

heading("BEGIN TEST")
conn <- new("H2OClient", ip=myIP, port=myPort)

path = locate("smalldata/logreg/prostate.csv")
prostate.hex = h2o.uploadFile.VA(conn, path, key="prostate.hex")
h2o.ls(conn)
    
rf = h2o.randomForest.VA(x=c(1,4), y="CAPSULE", data=prostate.hex, ntree=5)
h2o.ls(conn)

PASS_BANNER()
