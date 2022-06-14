#!/apps/anaconda3/bin/python3

# -----------------
# %% Program setups
# -----------------

# %% General packages
import os
import sys
import random
import numpy as np
import pandas as pd
from multiprocessing import Pool 


# ----------------
# %% Function defs
# ----------------
def analyzer(nn):    
    
    # % Import dataset
    df = pd.read_stata('../temp/prd.dta')

    # % Add empty column for random assignments
    df['g_rd'] = np.nan

    # % Random good & bad news assignment for each year and subperiod
    for ii, fyr in enumerate(df['fyear'].unique()):
        for jj, prd in enumerate([1, 2, 3]):
            # Length of the random vector
            ll = df[(df['fyear'] == fyr) & (df['prd'] == prd)].shape[0]
            # Set seed
            np.random.seed(int(nn)*100+ii*10+jj)
            # Generate the binary random vector
            gg = np.random.randint(2, size=ll)
            # Insert the random vector back to the dataframe
            df.loc[(df['fyear'] == fyr) & (df['prd'] == prd), 'g_rd'] = gg
    
    # Actual spread
    g_car_prd = df[df['g'] == 1].groupby(['fyear', 'prd'])['car_prd'].apply(np.mean) \
                    - df[df['g'] == 0].groupby(['fyear', 'prd'])['car_prd'].apply(np.mean) 
    # Hypothetical spread
    g_rd_car_prd = df[df['g_rd'] == 1].groupby(['fyear', 'prd'])['car_prd'].apply(np.mean) \
                    - df[df['g_rd'] == 0].groupby(['fyear', 'prd'])['car_prd'].apply(np.mean)
    
    # Boolean var =1 if actual spread > hypo spread
    (g_car_prd > g_rd_car_prd).to_frame('bool_'+str(nn)).reset_index().to_csv('../temp/'+str(nn)+'.csv', index=False)


# -------
# %% Main
# -------
if __name__ == "__main__":
    
    # % Input sampling Id
    # nn = int(sys.argv[1])
    idx = int(os.getenv('SGE_TASK_ID'))

    # % Get ncpus from env
    ncpus = int(os.getenv('NSLOTS'))
    # print("Number of CPU cores allocated: {}".format(ncpus))

    # Allocate analyzing jobs for each security on <ncpus> allocated cores
    with Pool(ncpus) as pool:
        pool.map(analyzer, range((idx-1)*ncpus, idx*ncpus))

