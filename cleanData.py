import sys
import pandas as pd
import multiprocessing as mp
import numpy as np

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


def extractColumns(filename,year):
    """
    Get relevant columns from the csv file and return a pandas dataframe
    with year alongside. Also remap education, employment, and immigration
    status to values required for my analysis
    """
    print('\t\textracting: ' + filename)
    df = pd.read_csv(filename,usecols = ['WAGP','ST','CIT','NATIVITY','AGEP','SCHL','ESR','PUMA','DECADE'] )
    df['YEAR'] = year
    # filter out children and old people
    workingAge = (18 <= df['AGEP']) & (df['AGEP'] <= 60)
    df = df[workingAge]
    # filter out people not in labor force
    assert not any(df['ESR'].isnull()), 'ESR Nulls mean <16 yr olds'
    laborforce = df['ESR'] != 6
    df = df[laborforce]
    # Get employed or not, lose the extra info
    df['EMPLOYED'] = (df['ESR'] != 3) # 3 is unemployed
    df.drop('ESR',axis=1, inplace=True)
    # remap CIT so 1: non citizen, 2: naturalized citizen, 3: born citizen
    cit_map = {1:3, 2:3, 3:3, 4:2, 5:1} # See data dict
    df['CIT'] = df['CIT'].replace(cit_map)
    # remap SCHL so 1: less than HS educ, 2: HS equiv, 3: College educ
    schl_map =      {i:1 for i in range(1, 16)}     # < hs educs
    schl_map.update({i:2 for i in range(16,20)}) # hs equiv
    schl_map.update({i:3 for i in range(20,25)}) # college educ
    df['SCHL'] = df['SCHL'].replace(schl_map)
    return df

def get_dataframes():
    '''
    Generator: returns a list of dataframes with good columns
    '''
    args_to_map = []
    for year in range(2007,2016):
        y = '%02d'%(year % 1000)
        file_a = 'ss' + y + 'pusa.csv'
        file_b = 'ss' + y + 'pusb.csv'
        args_to_map.append((file_a,year))
        args_to_map.append((file_b,year))
    return pMap(extractColumns, args_to_map, star = True)

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('usage: Be in Data/ directory. Then run file with output filename:' +
            '\n python3 cleanData.py [outputfilename]')
    else:
        print('\tReading data')
        frames = get_dataframes()

        print('\tConcatinating data')
        full = pd.concat(frames)
        np.random.seed(1234)
        mask = np.random.random(len(full),) < .05
        test  = full[~mask]
        train = full[ mask]
        print('\t Writing data to file')
        for df, s in [(train,'Train'),(test,'Test'),(full,'Full')]:
            print('\t\tWriting '+s+' data. It has '+str(len(df))+' rows')
            df.to_csv(sys.argv[1] + s + '.csv')
