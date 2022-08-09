import glob
import pd


def Load_csv_from_path(path,d):
    files = glob.glob(path+'*.csv')    
    df = pd.DataFrame()
    for file in files:
        file_pd = pd.read_csv(file,header=None)
        df = pd.concat([df,file_pd],axis=0)
    df = df.rename(columns = d)
    return df
