import sys
import pandas as pd
import multiprocessing as mp

def pMap(args,star=False):
    '''
    for the embarassingly parallel stuffs
    '''
    mp.set_start_method('spawn')
    p = mp.Pool(mp.cpu_count())
    if star:
        res =  p.starmap(args)
    else:
        res = p.map(args)
    p.close()
    p.terminate()
    return res


def extractColumns(filename,year):
    """
    Get relevant columns from the csv file and return a pandas dataframe
    with year alongside
    """
    print('processing: ' + filename)
    df = pd.read_csv(filename,usecols = ['WAGP','ST','CIT','NATIVITY','AGEP','SCHL','ESR','DECADE'] )
    df['YEAR'] = year
    return df

def get_dataframes():
    '''
    Generator: returns a list of dataframes with good columns
    '''
    args = []
    for year in range(2007,2016):
        y = '%02d'%(year % 1000)
        file_a = 'ss' + y + 'pusa.csv'
        file_b = 'ss' + y + 'pusb.csv'
        args.append((file_a,year))
        args.append((file_b,year))
    return pMap(args,star = True)

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('usage: Be in Data/ directory. Then run file with output filename:' +
            '\n python3 cleanData.py [outputfilename]')
    else:
        print('reading data')
        dfs = get_dataframes()
        print('concatinating data')
        df = pd.concat(dfs)
        print('writing results')
        df.to_csv(sys.argv[1])
        rs = df.sample(10000,random_state= 1234)
        print('writing random sample')
        rs.to_csv('Sample.csv')
