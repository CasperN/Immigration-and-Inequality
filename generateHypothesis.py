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
    return[ ((1,),  (2,)),
            ((1,),  (3,)),
            ((2,),  (3,)),
            ((1,2),(3,)),
            ((1,),  (2,3)) ]

def holdingConstantGen():
    """
    Given 3 categories
    """
    return [(1,),(2,),(3,),(1,2),(2,3),(1,2,3)]

def categorizer(x,c,v,hc,g1,g2):
    """
    Template to be filled with buildCategorizer
    Read that function it'll make more sense
    """
    if not (x[c] in hc):
        return 0
    if x[v] in g1:
        return 1
    elif x[v] in g2:
        return 2
    else:
        return 0 # Neither in group 1 nor 2


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
    cat = partial(categorizer, c = const, v = vary, hc = holdingConstant,
                               g1 = group1, g2 = group2)
    return cat


def nameHypothesis(splitOver,holdingConstant,education):
    """
    Names the hypothesis
    """
    if education:
        const = {
         (1,)    :'below high school',
         (2,)    :'high school equivalent',
         (3,)    :'college equivalent',
         (1,2)  :'below college',
         (2,3)  :'high school or more',
         (1,2,3):'all'}
        vary = { ((1,),  (2,)) : 'non-citzens and naturalized citizens',
                 ((1,),  (3,)) : 'non-citizens and born citizens',
                 ((2,),  (3,)) : 'naturalized and born citizens',
                 ((1,2), (3,)) : 'immigrants and born citizens',
                 ((1,),  (2,3)): 'non-citizens and citizens'
                 }
    else:
        const = {
         (1,)    :'non-citizen',
         (2,)    :'naturalized citizen',
         (3,)    :'born citizen',
         (1,2)  :'immigrant',
         (2,3)  :'all citizen',
         (1,2,3):'all'}
        vary = { ((1,), (2,)) : 'below high school and high school equivalent educated',
                 ((1,), (3,)) : 'below high school and college equivalent',
                 ((2,), (3,)) : 'high school equivalent and college equivalent educated',
                 ((1,2),(3,)) : 'below college and college equivalent educated',
                 ((1,), (2,3)): 'below and at high school educated'
                  }
    h = 'For '+const[holdingConstant] + ' workers, '+vary[splitOver]+' are perfect substitutes'
    return h

def twoGroupLinearData(df,idx):
    """
    takes in a dataframe (1 part of a groupby object)
    returns log wage ratio and log labor input ratio
    """
    a = np.unique(idx)
    if (1 not in a) or (2 not in a):
        # only 1 group present
        return
    # Wage ratio
    wage_g1  = df[idx == 1]['WAGP'].mean()
    wage_g2  = df[idx == 2]['WAGP'].mean()
    y = np.log(wage_g1 / wage_g2)
    # Labor share
    labor_g1 = df[idx == 1]['EMPLOYED'].mean()
    labor_g2 = df[idx == 2]['EMPLOYED'].mean()
    x = np.log(labor_g1 / labor_g2)
    # Return if real number
    inf = float('inf')
    if (-inf < y < inf) and (-inf < x < inf):
        return y,x

def write_for_R(g,i,cat,hyp):
    res = []
    for a,df in g:
        idx = df.iloc[:,:].apply(cat,axis = 1)
        r = twoGroupLinearData(df,idx)
        if r != None:
            res.append(r)
    res = pd.DataFrame(res, columns = ['ys','xs'])
    res.to_csv('hypothesis/h'+ str(i+1) + '.csv')
    print(hyp)

def generateHypothesisAndData(datafile):
    data = pd.read_csv(datafile)
    g = data.groupby(['YEAR','PUMA'])
    params = it.product(splitOverGen(),holdingConstantGen(),(True,False))
    args = [(g, i, buildCategorizer(*x), nameHypothesis(*x))
                for i,x in enumerate(params)]
    pMap(write_for_R,args,star=True)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage:\ncd Data/ Then write\npython3 ../generateHypothesis.py [datafile]')
    else:
        #go(sys.argv[1])
        with open('hypothesis.txt','w') as f:
            params = it.product(splitOverGen(),holdingConstantGen(),(True,False))
            hyps = [nameHypothesis(*x) for x in params]
            for i,h in enumerate(hyps):
                f.write('H'+str(i+1)+': '+h+'\n')
        generateHypothesisAndData(sys.argv[1])
