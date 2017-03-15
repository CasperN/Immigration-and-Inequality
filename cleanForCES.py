import pandas as pd
import numpy as np
import multiprocessing as mp
import sys

def pMap(func, argGen, star=False):
    '''
    for the embarassingly parallel stuffs
    '''
    mp.set_start_method('spawn')
    p = mp.Pool(mp.cpu_count())
    if star:
        res =  p.starmap(func, argGen)
    else:
        res = p.map(func, argGen)
    p.close()
    p.terminate()
    return res

def twoGroupLinearData(df,categorizer):
    """
    returns log wage ratio and log labor input ratio
    """
    cat = df.apply(categorizer,axis = 1)
    if len(np.unique(cat)) == 1:
        return
    # Wage ratio
    wage_i  = df[ cat]['WAGP'].mean()
    wage_n  = df[~cat]['WAGP'].mean()
    y = np.log(wage_i / wage_n)
    # Labor share
    labor_i = df[ cat]['EMPLOYED'].mean()
    labor_n = df[~cat]['EMPLOYED'].mean()
    x = np.log(labor_i / labor_n)
    # Return if real number
    inf = float('inf')
    if (-inf < y < inf) and (-inf < x < inf):
        return y,x

def go(categorizer):
    '''
    Cleans data for CES model
    groups data by location and year
    gets mean wage and employment
    '''
    data = pd.read_csv('Data/ACSdataTrain.csv')
    g = data.groupby(['YEAR','PUMA'])
    # I want to apply a function here that gets us our covariates to regress
    res = []
    for k,df in g:
        r = twoGroupLinearData(df,categorizer)
        if r != None:
            res.append(r)
    res = pd.DataFrame(res, columns = ['ys','xs'])
    return res

def native_vs_immigrant(x):
    # Boolean: returns if the observed is native
    return x['IMM']

if __name__ == '__main__':
    # Clean the data such that
    #
    #
    pass
