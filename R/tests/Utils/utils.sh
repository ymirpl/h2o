#!/bin/bash/

#disperse findNSourceUtils.R

disperse() {
    #$1 is directory path to cp findNSourceUtils.R to
    cp findNSourceUtils.R $1
}

#directories:
disperse "../"
disperse "../testdir_munging/testdir_binop/"
disperse "../testdir_munging/testdir_unop/"
disperse "../testdir_munging/testdir_exec/"
disperse "../testdir_munging/testdir_summary/"
disperse "../testdir_munging/testdir_histograms/"
disperse "../testdir_munging/testdir_slice/"
disperse "../testdir_algos/"
disperse "../testdir_munging/"
disperse "../testdir_golden/"
disperse "../testdir_autoGen/"
disperse "../testdir_misc/"
disperse "../../../py/testdir_release/
"
