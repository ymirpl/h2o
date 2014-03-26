import h2o_cmd, h2o, h2o_util, h2o_gbm
import re, random, math

def pickRandGlmParams(paramDict, params):
    colX = 0
    randomGroupSize = random.randint(1,len(paramDict))
    for i in range(randomGroupSize):
        randomKey = random.choice(paramDict.keys())
        randomV = paramDict[randomKey]
        randomValue = random.choice(randomV)
        params[randomKey] = randomValue
        if (randomKey=='x'):
            colX = randomValue

        # force legal family/ink combos
        if 'family' in params and 'link' in params: 
            if params['family'] is not None:
                if params['family'] == 'poisson':
                    if params['link'] is not None and params['link'] not in ('identity', 'log', 'inverse', 'familyDefault'):
                        params['link'] = None 
                # only tweedie/tweedie is legal?
                if params['family'] == 'tweedie':
                    if params['link'] is not None and params['link'] not in ('tweedie'):
                        params['link'] = None
                if params['family'] == 'binomial':
                    if params['link'] is not None and params['link'] not in ('logit', 'identity', 'log', 'inverse', 'familyDefault'):
                        params['link'] = None
                if params['family'] == 'gaussian':
                    if params['link'] is not None and params['link'] not in ('logit', 'identity', 'log', 'inverse', 'familyDefault'):
                        params['link'] = None

        # case only used if binomial? binomial is default if no family
        # update: apparently case and case_mode always affect things
        # make sure the combo of case and case_mode makes sense
        # there needs to be some entries in both effective cases
        if ('case_mode' in params):
            if ('case' not in params) or (params['case'] is None):
                params['case'] = 1
            else:
                maxCase = max(paramDict['case'])
                minCase = min(paramDict['case'])
                if params['case_mode']=="<" and params['case']==minCase:
                    params['case'] += 1
                elif params['case_mode']==">" and params['case']==maxCase:
                    params['case'] -= 1
                elif params['case_mode']==">=" and params['case']==minCase:
                    params['case'] += 1
                elif params['case_mode']=="<=" and params['case']==maxCase:
                    params['case'] -= 1

    return colX


def simpleCheckGLMScore(self, glmScore, family='gaussian', allowFailWarning=False, **kwargs):
    warnings = None
    if 'warnings' in glmScore:
        warnings = glmScore['warnings']
        # stop on failed
        x = re.compile("failed", re.IGNORECASE)
        # don't stop if fail to converge
        c = re.compile("converge", re.IGNORECASE)
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w) and not allowFailWarning: 
                if re.search(c,w):
                    # ignore the fail to converge warning now
                    pass
                else: 
                    # stop on other 'fail' warnings (are there any? fail to solve?
                    raise Exception(w)

    validation = glmScore['validation']
    validation['err'] = h2o_util.cleanseInfNan(validation['err'])
    validation['nullDev'] = h2o_util.cleanseInfNan(validation['nullDev'])
    validation['resDev'] = h2o_util.cleanseInfNan(validation['resDev'])
    print "%15s %s" % ("err:\t", validation['err'])
    print "%15s %s" % ("nullDev:\t", validation['nullDev'])
    print "%15s %s" % ("resDev:\t", validation['resDev'])

    # threshold only there if binomial?
    # auc only for binomial
    if family=="binomial":
        print "%15s %s" % ("auc:\t", validation['auc'])
        print "%15s %s" % ("threshold:\t", validation['threshold'])

    err = False
    if family=="poisson" or family=="gaussian":
        if 'aic' not in validation:
            print "aic is missing from the glm json response"
            err = True

    if math.isnan(validation['err']):
        print "Why is this err = 'nan'?? %6s %s" % ("err:\t", validation['err'])
        err = True

    if math.isnan(validation['resDev']):
        print "Why is this resDev = 'nan'?? %6s %s" % ("resDev:\t", validation['resDev'])
        err = True

    if err:
        raise Exception ("How am I supposed to tell that any of these errors should be ignored?")

    # legal?
    if math.isnan(validation['nullDev']):
        ## emsg = "Why is this nullDev = 'nan'?? %6s %s" % ("nullDev:\t", validation['nullDev'])
        ## raise Exception(emsg)
        pass

def simpleCheckGLM(self, glm, colX, allowFailWarning=False, allowZeroCoeff=False,
    prettyPrint=False, noPrint=False, maxExpectedIterations=None, doNormalized=False, **kwargs):
    # if we hit the max_iter, that means it probably didn't converge. should be 1-maxExpectedIter

    # h2o GLM will verboseprint the result and print errors. 
    # so don't have to do that
    # different when cross validation  is used? No trainingErrorDetails?
    if h2o.beta_features:
        GLMModel = glm['glm_model']
    else:
        GLMModel = glm['GLMModel']

    if not GLMModel:
        raise Exception("GLMModel didn't exist in the glm response? %s" % h2o.dump_json(glm))
    

    warnings = None
    if 'warnings' in GLMModel and GLMModel['warnings']:
        warnings = GLMModel['warnings']
        # stop on failed
        x = re.compile("failed", re.IGNORECASE)
        # don't stop if fail to converge
        c = re.compile("converge", re.IGNORECASE)
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w) and not allowFailWarning: 
                if re.search(c,w):
                    # ignore the fail to converge warning now
                    pass
                else: 
                    # stop on other 'fail' warnings (are there any? fail to solve?
                    raise Exception(w)

    # for key, value in glm.iteritems(): print key
    # not in GLMGrid?

    # FIX! don't get GLMParams if it can't solve?
    if h2o.beta_features:
        GLMParams = GLMModel['glm']
    else:
        GLMParams = GLMModel["GLMParams"]

    family = GLMParams["family"]

    if h2o.beta_features:
        # number of submodels = number of lambda
        # min of 2. lambdaMax is first
        submodels = GLMModel['submodels']
        lambdas = GLMModel['lambdas']
        # since all our tests?? only use one lambda, the best_lamda_idx should = 1
        best_lambda_idx = GLMModel['best_lambda_idx']
        lambdaMax = lambdas[0]
        print "lambdaMax:", lambdaMax

        if 1==0:
            if len(submodels) < 2:
                raise Exception("Always should have a minimum of 2 submodels in GLM2 response", len(submodels))
            if len(lambdas) < 2:
                raise Exception("Always should have a minimum of 2 lambdas in GLM2 response", len(submodels))
            if best_lambda_idx != 1:
                raise Exception("best_lamda_idx %s should point to the one lamda we specified? %s" % (best_lamda_idx, lamdas[1]))
            if lambdaMax <= lambdas[-1]:
                raise Exception("lambdaMax %s should always be < the lambda result %s we're checking" % (lambdaMax, lambdas[1]))

        submodels0 = submodels[0]
        submodels1 = submodels[-1] # hackery to make it work when there's just one
        iterations = submodels1['iteration']

    else:
        iterations = GLMModel['iterations']

    print "GLMModel/iterations:", iterations

            # if we hit the max_iter, that means it probably didn't converge. should be 1-maxExpectedIter
    if maxExpectedIterations is not None and iterations  > maxExpectedIterations:
            raise Exception("Convergence issue? GLM did iterations: %d which is greater than expected: %d" % (iterations, maxExpectedIterations) )

    if h2o.beta_features:
        if 'validation' not in submodels1:
            raise Exception("Should be a 'validation' key in submodels1: %s" % h2o.dump_json(submodels1))
        validationsList = submodels1['validation']
        validations = validationsList
        
    else:
        # pop the first validation from the list
        if 'validations' not in GLMModel:
            raise Exception("Should be a 'validations' key in GLMModel: %s" % h2o.dump_json(GLMModel))
        validationsList = GLMModel['validations']
        # don't want to modify validationsList in case someone else looks at it
        validations = validationsList[0]

    # xval. compare what we asked for and what we got.
    n_folds = kwargs.setdefault('n_folds', None)

    # not checked in v2?
    if not h2o.beta_features:
        if not 'xval_models' in validations:
            if n_folds > 1:
                raise Exception("No cross validation models returned. Asked for "+n_folds)
        else:
            xval_models = validations['xval_models']
            if n_folds and n_folds > 1:
                if len(xval_models) != n_folds:
                    raise Exception(len(xval_models)+" cross validation models returned. Asked for "+n_folds)
            else:
                # should be default 10?
                if len(xval_models) != 10:
                    raise Exception(str(len(xval_models))+" cross validation models returned. Default should be 10")

    if h2o.beta_features:
        print "GLMModel/validations"
        validations['avg_err'] = h2o_util.cleanseInfNan(validations['avg_err'])
        validations['null_deviance'] = h2o_util.cleanseInfNan(validations['null_deviance'])
        validations['residual_deviance'] = h2o_util.cleanseInfNan(validations['residual_deviance'])
        print "%15s %s" % ("avg_err:\t", validations['avg_err'])
        print "%15s %s" % ("null_deviance:\t", validations['null_deviance'])
        print "%15s %s" % ("residual_deviance:\t", validations['residual_deviance'])

    else:
        print "GLMModel/validations"
        validations['err'] = h2o_util.cleanseInfNan(validations['err'])
        validations['nullDev'] = h2o_util.cleanseInfNan(validations['nullDev'])
        validations['resDev'] = h2o_util.cleanseInfNan(validations['resDev'])
        print "%15s %s" % ("err:\t", validations['err'])
        print "%15s %s" % ("nullDev:\t", validations['nullDev'])
        print "%15s %s" % ("resDev:\t", validations['resDev'])

    # threshold only there if binomial?
    # auc only for binomial
    if family=="binomial":
        print "%15s %s" % ("auc:\t", validations['auc'])
        if h2o.beta_features:
            print "%15s %s" % ("best_threshold:\t", validations['best_threshold'])
            # show the middle one? 
            print "We're just going to print the middle '_cms' ..that must be threshold 0.5?. this isn't above best_threshold"
            # cm = glm['glm_model']['submodels'][0]['validation']['_cms'][-1]
            cms = glm['glm_model']['submodels'][0]['validation']['_cms']
            # rounds to int
            mid = len(cms)/2
            cm = cms[mid]

            print "cm:", h2o.dump_json(cm['_arr'])
            predErr = cm['_predErr']
            classErr = cm['_classErr']
            # compare to predErr
            pctWrong = h2o_gbm.pp_cm_summary(cm['_arr']);
            print "predErr:", predErr
            print "calculated pctWrong from cm:", pctWrong
            print "classErr:", classErr

            # self.assertLess(pctWrong, 9,"Should see less than 9% error (class = 4)")

            print "\nTrain\n==========\n"
            print h2o_gbm.pp_cm(cm['_arr'])
        else:
            print "%15s %s" % ("threshold:\t", validations['threshold'])


    if family=="poisson" or family=="gaussian":
        print "%15s %s" % ("aic:\t", validations['aic'])

    if not h2o.beta_features:
        if math.isnan(validations['err']):
            emsg = "Why is this err = 'nan'?? %6s %s" % ("err:\t", validations['err'])
            raise Exception(emsg)

        if math.isnan(validations['resDev']):
            emsg = "Why is this resDev = 'nan'?? %6s %s" % ("resDev:\t", validations['resDev'])
            raise Exception(emsg)

        # legal?
        if math.isnan(validations['nullDev']):
            pass

    # get a copy, so we don't destroy the original when we pop the intercept
    if h2o.beta_features:
        coefficients_names = GLMModel['coefficients_names']
        idxs = submodels1['idxs']
        column_names = coefficients_names

        # always check both normalized and normal coefficients
        norm_beta = submodels1['norm_beta']
        if norm_beta and len(column_names)!=len(norm_beta):
            print len(column_names), len(norm_beta)
            raise Exception("column_names and normalized_norm_beta from h2o json not same length. column_names: %s normalized_norm_beta: %s" % (column_names, norm_beta))

        beta = submodels1['beta']
        if len(column_names)!=len(beta):
            print len(column_names), len(beta)
            raise Exception("column_names and beta from h2o json not same length. column_names: %s beta: %s" % (column_names, beta))


        # test wants to use normalized?
        if doNormalized:
            beta_used = norm_beta
        else:
            beta_used = beta

        coefficients = {}
        # create a dictionary with name, beta (including intercept) just like v1

        for n,b in zip(column_names, beta_used):
            coefficients[n] = b

        print  "coefficients:", coefficients
        print  "beta:", beta
        print  "norm_beta:", norm_beta

        print "intercept demapping info:", \
            "column_names[-i]:", column_names[-1], \
            "idxs[-1]:", idxs[-1], \
            "coefficients_names[[idxs[-1]]:", coefficients_names[idxs[-1]], \
            "beta_used[-1]:", beta_used[-1], \
            "coefficients['Intercept']", coefficients['Intercept']

        # idxs has the order for non-zero coefficients, it's shorter than beta_used and column_names
        for i in idxs:
            if beta_used[i]==0.0:
                raise Exception("idxs shouldn't point to any 0 coefficients i: %s beta_used[i]:" (i, beta_used[i]))

        intercept = coefficients.pop('Intercept', None)

        # intercept demapping info: idxs[-1]: 54 coefficient_names[[idxs[-1]]: Intercept beta_used[-1]: -6.6866753099
        # the last one shoudl be 'Intercept' ?
        column_names.pop()

    else:
        if doNormalized:
            coefficients = GLMModel['normalized_coefficients'].copy()
        else:
            coefficients = GLMModel['coefficients'].copy()
        column_names = GLMModel['column_names']
        # get the intercept out of there into it's own dictionary
        intercept = coefficients.pop('Intercept', None)
        print "First intercept:", intercept

    # have to skip the output col! get it from kwargs
    # better always be there!
    if h2o.beta_features:
        y = kwargs['response']
    else:
        y = kwargs['y']


    # the dict keys are column headers if they exist...how to order those? new: use the 'column_names'
    # from the response
    # Tomas created 'column_names which is the coefficient list in order.
    # Just use it to index coefficients! works for header or no-header cases
    # I guess now we won't print the "None" cases for dropped columns (constant columns!)
    # Because Tomas doesn't get everything in 'column_names' if dropped by GLMQuery before
    # he gets it? 
    def add_to_coefficient_list_and_string(c, cList, cString):
        if c in coefficients:
            cValue = coefficients[c]
            cValueString = "%s: %.5e   " % (c, cValue)
        else:
            print "Warning: didn't see '" + c + "' in json coefficient response.",\
                  "Inserting 'None' with assumption it was dropped due to constant column)"
            cValue = None
            cValueString = "%s: %s   " % (c, cValue)

        cList.append(cValue)
        # we put each on newline for easy comparison to R..otherwise keep condensed
        if prettyPrint: 
            cValueString = "H2O coefficient " + cValueString + "\n"
        # not mutable?
        return cString + cValueString

    # creating both a string for printing and a list of values
    cString = ""
    cList = []
    # print in order using col_names
    # column_names is input only now..same for header or no header, or expanded enums
    for c in column_names:
        cString = add_to_coefficient_list_and_string(c, cList, cString)

    if prettyPrint: 
        print "\nH2O intercept:\t\t%.5e" % intercept
        print cString
    else:
        if not noPrint:
            print "\nintercept:", intercept, cString

    print "\nTotal # of coefficients:", len(column_names)

    # pick out the coefficent for the column we enabled for enhanced checking. Can be None.
    # FIX! temporary hack to deal with disappearing/renaming columns in GLM
    if (not allowZeroCoeff) and (colX is not None):
        absXCoeff = abs(float(coefficients[str(colX)]))
        self.assertGreater(absXCoeff, 1e-26, (
            "abs. value of GLM coefficients['" + str(colX) + "'] is " +
            str(absXCoeff) + ", not >= 1e-26 for X=" + str(colX)
            ))

    # intercept is buried in there too
    absIntercept = abs(float(intercept))
    self.assertGreater(absIntercept, 1e-26, (
        "abs. value of GLM coefficients['Intercept'] is " +
        str(absIntercept) + ", not >= 1e-26 for Intercept"
                ))

    # this is good if we just want min or max
    # maxCoeff = max(coefficients, key=coefficients.get)
    # for more, just invert the dictionary and ...
    if (len(coefficients)>0):
        maxKey = max([(abs(coefficients[x]),x) for x in coefficients])[1]
        print "H2O Largest abs. coefficient value:", maxKey, coefficients[maxKey]
        minKey = min([(abs(coefficients[x]),x) for x in coefficients])[1]
        print "H2O Smallest abs. coefficient value:", minKey, coefficients[minKey]
    else: 
        print "Warning, no coefficients returned. Must be intercept only?"

    # many of the GLM tests aren't single column though.
    # quick and dirty check: if all the coefficients are zero, 
    # something is broken
    # intercept is in there too, but this will get it okay
    # just sum the abs value  up..look for greater than 0

    # skip this test if there is just one coefficient. Maybe pointing to a non-important coeff?
    if (not allowZeroCoeff) and (len(coefficients)>1):
        s = 0.0
        for c in coefficients:
            v = coefficients[c]
            s += abs(float(v))

        self.assertGreater(s, 1e-26, (
            "sum of abs. value of GLM coefficients/intercept is " + str(s) + ", not >= 1e-26"
            ))

    if h2o.beta_features:
        print "submodels1, run_time (milliseconds):", submodels1['run_time']
    else:

        print "GLMModel model time (milliseconds):", GLMModel['model_time']
        print "GLMModel validation time (milliseconds):", validations['val_time']
        print "GLMModel lsm time (milliseconds):", GLMModel['lsm_time']

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

    return (warnings, cList, intercept)


# compare this glm to last one. since the files are concatenations, 
# the results should be similar? 10% of first is allowed delta
def compareToFirstGlm(self, key, glm, firstglm):
    # if isinstance(firstglm[key], list):
    # in case it's not a list allready (err is a list)
    h2o.verboseprint("compareToFirstGlm key:", key)
    h2o.verboseprint("compareToFirstGlm glm[key]:", glm[key])
    # key could be a list or not. if a list, don't want to create list of that list
    # so use extend on an empty list. covers all cases?
    if type(glm[key]) is list:
        kList  = glm[key]
        firstkList = firstglm[key]
    elif type(glm[key]) is dict:
        raise Exception("compareToFirstGLm: Not expecting dict for " + key)
    else:
        kList  = [glm[key]]
        firstkList = [firstglm[key]]

    for k, firstk in zip(kList, firstkList):
        # delta must be a positive number ?
        delta = .1 * abs(float(firstk))
        msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
        self.assertAlmostEqual(float(k), float(firstk), delta=delta, msg=msg)
        self.assertGreaterEqual(abs(float(k)), 0.0, str(k) + " abs not >= 0.0 in current")


def simpleCheckGLMGrid(self, glmGridResult, colX=None, allowFailWarning=False, **kwargs):
    destination_key = glmGridResult['destination_key']
    inspectGG = h2o_cmd.runInspect(None, destination_key)
    h2o.verboseprint("Inspect of destination_key", destination_key,":\n", h2o.dump_json(inspectGG))

    # FIX! currently this is all unparsed!
    #type = inspectGG['type']
    #if 'unparsed' in type:
    #    print "Warning: GLM Grid result destination_key is unparsed, can't interpret. Ignoring for now"
    #    print "Run with -b arg to look at the browser output, for minimal checking of result"

    ### cols = inspectGG['cols']
    response = inspectGG['response'] # dict
    ### rows = inspectGG['rows']
    #value_size_bytes = inspectGG['value_size_bytes']

    # FIX! does error_0/1 only exist for binomial?
    for m, model in enumerate(glmGridResult['models']):
        alpha = model['alpha']
        area_under_curve = model['area_under_curve']
        # FIX! should check max error?
        error_0 = model['error_0']
        error_1 = model['error_1']
        model_key = model['key']
        print "#%s GLM model key: %s" % (m, model_key)
        glm_lambda = model['lambda']

    # now indirect to the GLM result/model that's first in the list (best)
    inspectGLM = h2o_cmd.runInspect(None, glmGridResult['models'][0]['key'])
    h2o.verboseprint("GLMGrid inspect GLMGrid model 0(best):", h2o.dump_json(inspectGLM))
    g = simpleCheckGLM(self, inspectGLM, colX, allowFailWarning=allowFailWarning, **kwargs)

    return g


# This gives me a comma separated x string, for all the columns, with cols with
# missing values, enums, and optionally matching a pattern, removed. useful for GLM
# since it removes rows with any col with NA

# get input from this.
#   (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
#                h2o_cmd.columnInfoFromInspect(parseResult['destination_key', 
#                exceptionOnMissingValues=False, timeoutSecs=300)

def goodXFromColumnInfo(y, 
    num_cols=None, missingValuesDict=None, constantValuesDict=None, enumSizeDict=None, 
    colTypeDict=None, colNameDict=None, keepPattern=None, key=None, 
    timeoutSecs=120, forRF=False, noPrint=False, returnStringX=True):

    y = str(y)

    # if we pass a key, means we want to get the info ourselves here
    if key is not None:
        (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
            h2o_cmd.columnInfoFromInspect(key, exceptionOnMissingValues=False, 
            max_column_display=99999999, timeoutSecs=timeoutSecs)
        num_cols = len(colNameDict)

    # now remove any whose names don't match the required keepPattern
    if keepPattern is not None:
        keepX = re.compile(keepPattern)
    else:
        keepX = None

    x = range(num_cols)
    # need to walk over a copy, cause we change x
    xOrig = x[:]
    ignore_x = [] # for use by RF
    for k in xOrig:
        name = colNameDict[k]
        # remove it if it has the same name as the y output
        if str(k)== y: # if they pass the col index as y
            if not noPrint:
                print "Removing %d because name: %s matches output %s" % (k, str(k), y)
            x.remove(k)
            # rf doesn't want it in ignore list
            # ignore_x.append(k)
        elif name == y: # if they pass the name as y 
            if not noPrint:
                print "Removing %d because name: %s matches output %s" % (k, name, y)
            x.remove(k)
            # rf doesn't want it in ignore list
            # ignore_x.append(k)

        elif keepX is not None and not keepX.match(name):
            if not noPrint:
                print "Removing %d because name: %s doesn't match desired keepPattern %s" % (k, name, keepPattern)
            x.remove(k)
            ignore_x.append(k)

        # missing values reports as constant also. so do missing first.
        # remove all cols with missing values
        # could change it against num_rows for a ratio
        elif k in missingValuesDict:
            value = missingValuesDict[k]
            if not noPrint:
                print "Removing %d with name: %s because it has %d missing values" % (k, name, value)
            x.remove(k)
            ignore_x.append(k)

        elif k in constantValuesDict:
            value = constantValuesDict[k]
            if not noPrint:
                print "Removing %d with name: %s because it has constant value: %s " % (k, name, str(value))
            x.remove(k)
            ignore_x.append(k)

        # this is extra pruning..
        # remove all cols with enums, if not already removed
        elif k in enumSizeDict:
            value = enumSizeDict[k]
            if not noPrint:
                print "Removing %d %s because it has enums of size: %d" % (k, name, value)
            x.remove(k)
            ignore_x.append(k)

    if not noPrint:
        print "x has", len(x), "cols"
        print "ignore_x has", len(ignore_x), "cols"

    # this is probably used in 'cols" in v2, which can take numbers
    if returnStringX:
        x = ",".join(map(str, x))

    if h2o.beta_features: # add the 'C" prefix because of ignored_cols_by_name (and the start-with-1 offset)
        ignore_x = ",".join(map(lambda x: "C" + str(x+1), ignore_x))
    else:
        ignore_x = ",".join(map(lambda x: str(x), ignore_x))

    if not noPrint:
        print "\nx:", x
        print "\nignore_x:", ignore_x

    if forRF:
        return ignore_x
    else:
        return x
