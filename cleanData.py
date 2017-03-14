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
    with year alongside
    """
    print('\t\textracting: ' + filename)
    df = pd.read_csv(filename,usecols = ['WAGP','ST','CIT','NATIVITY','AGEP','SCHL','ESR','PUMA','DECADE'] )
    df['YEAR'] = year
    workingAge = (18 <= df['AGEP']) & (df['AGEP'] <= 60)
    return df[workingAge]

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
        print('\tdata has ' + str(len(full)) + 'rows')
        mask = np.random.random(len(full)) < .01
        test  = full[~mask]
        train = full[ mask]
        for df, s in [(train,'Train'),(test,'Test'),(full,'Full')]:
            print('\tWriting '+s' data to file')
            df.to_csv(sys.argv[1] + s + '.csv')
