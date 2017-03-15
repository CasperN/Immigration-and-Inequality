import pandas as pd
import numpy as np
import multiprocessing as mp
import sys
import itertools as it
from functools import partial

def pMap(func, argGen, star=False):
    """
    for the embarassingly parallel stuffs
    """
    mp.set_start_method('spawn')
    p = mp.Pool(mp.cpu_count())
    if star:
        res =  p.starmap(func, argGen)
    else:
        res = p.map(func, argGen)
    p.close()
    p.terminate()
    return res

def splitOverGen():
    """
    Generator: with 3 categories to split over, split them in a way
    that respects order
    """
    return[ ((1),  (2)),
            ((1),  (3)),
            ((2),  (3)),
            ((1,2),(3)),
            ((1),  (2,3)) ]

def holdingConstantGen():
    """
    Given 3 categories
    """
    return [(1),(2),(3),(1,2),(2,3),(1,2,3)]

def categorizer(x,c,v,hc,g1,g2):
    """
    Template to be filled with buildCategorizer
    Read that function it'll make more sense
    """
    if x[c] not in hc:
        return 0
    if x[v] in group1:
        return 1
    if x[v] in group2:
        return 2

def buildCategorizer(splitOver,holdingConstant, education):
    """
    Returns a function that returns
        0 if we want to ignore a row
        1 if we want it in group 1
        2 if we want it in group 2
    """
    group1, group2 = splitOver
    if education:
        const = 'SCHL'
        vary = 'CIT'
    else:
        const = 'CIT'
        vary = 'SCHL'
    # Yes I actually have to do this. Because bad scoping.
#    cat = (lambda c,v,hc,g1,g2: (lambda x: categorizer(x,c,v,hc,g1,g2))
#    )(const, vary, holdingConstant, group1, group2)

    #cat = lambda x, c = const, v = vary, hc = holdingConstant,\
    #            g1 = group1, g2 = group2: categorizer(x,c,v,hc,g1,g2)

    cat = partial(categorizer, c = const, v = vary,hc = holdingConstant,
                               g1 = group1, g2 = group2)

    return cat

def twoGroupLinearData(df,categorizer):
    """
    returns log wage ratio and log labor input ratio
    """
    cat = df.apply(categorizer,axis = 1)
    if (1 not in cat) or (2 not in cat):
        # only 1 group present
        return
    # Wage ratio
    wage_g1  = df[cat == 1]['WAGP'].mean()
    wage_g2  = df[cat == 2]['WAGP'].mean()
    y = np.log(wage_g1 / wage_n)
    # Labor share
    labor_g1 = df[cat == 1]['EMPLOYED'].mean()
    labor_g2 = df[cat == 2]['EMPLOYED'].mean()
    x = np.log(labor_g1 / labor_n)
    # Return if real number
    inf = float('inf')
    if (-inf < y < inf) and (-inf < x < inf):
        return y,x

def nameHypothesis(splitOver,holdingConstant,education):
    """
    Names the hypothesis
    """
    if education:
        const = {
         (1)    :'below high school',
         (2)    :'high school equivalent',
         (3)    :'college equivalent',
         (1,2)  :'below college',
         (2,3)  :'high school or more',
         (1,2,3):'all'}
        vary = { ((1),  (2))  : 'non-citzens and naturalized citizens',
                 ((1),  (3))  : 'non-citizens and born citizens',
                 ((2),  (3))  : 'naturalized and born citizens',
                 ((1,2),(3))  : 'immigrants and born citizens',
                 ((1),  (2,3)): 'non-citizens and citizens'
                 }
    else:
        const = {
         (1)    :'non-citizen',
         (2)    :'naturalized citizen',
         (3)    :'born citizen',
         (1,2)  :'immigrant',
         (2,3)  :'all citizen',
         (1,2,3):'all'}
        vary = { ((1),  (2))  : 'below high school and high school equivalent education',
                 ((1),  (3))  : 'below high school and college equivalent',
                 ((2),  (3))  : 'high school equivalent and college equivalent education',
                 ((1,2),(3))  : 'below college and college equivalent education',
                 ((1),  (2,3)): 'below and above high school education'
                  }
    h = 'For '+const[holdingConstant] + ' workers, the elasticity of '+ \
    'substitution between '+vary[splitOver]+' is 1'
    return h


def Make_subdata(categorizer,g):
    """
    Cleans data for CES model
    groups data by location and year
    gets mean wage and employment
    """
    # I want to apply a function here that gets us our covariates to regress
    res = []
    for k,df in g:
        r = twoGroupLinearData(df,categorizer)
        if r != None:
            res.append(r)
    res = pd.DataFrame(res, columns = ['ys','xs'])
    return res

def go(datafile,outputfile='h'):
    """
    Generates all the hypothesis, numbers them, names them, makes data for them,
    and saves the data to file
    """
    data = pd.read_csv(datafile)
    groups = data.groupby(['YEAR','PUMA'])
    for n, params in  enumerate(it.product(splitOverGen(),
                                holdingConstantGen(), (True,False))):
        cat = buildCategorizer(* params)
        hyp = nameHypothesis(  * params)
        p = Make_subdata(categorizer,groups)
        p.to_csv('hypothesis/' + outputfile + str(n) + '.csv')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage:\ncd Data/ Then write\npython3 ../generateHypothesis.py [datafile]')
    else:
        go(sys.argv[1])
