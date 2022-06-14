#!/apps/anaconda3/bin/python3

# -----------------
# %% Program setups
# -----------------

# %% General packages
import re
import time
import itertools
import numpy as np
import pandas as pd
from pandas.tseries.offsets import MonthEnd # Handy for adjusting CRSP dates to end-of-month dates


# %% Macro variables configuration
# Start and end date of WRDS data query
from datetime import date, timedelta
start_date = '1970/01/01' # Compustat starts in 1960, CRSP dates back to 1925
end_date = '2020/12/31' # str(date.today())


# ----------------
# %% Function defs
# ----------------
# %% Toolkit function to check pks of a given panel dataframe
def pk_integrity(df, primary_key):
    """ Check that df columns in primary_key consist no missing values or duplicates. """
    
    assert df[primary_key].notna().all().all(), 'Null values detected in primary key.'
    assert not df[primary_key].duplicated().any(), 'Duplicate values detected in primary key.'

    pass


# %% Function to query/initialize Compustat (daily-updated) Fundamental Annual/Quarterly data
def comp(db, varlist, start_date, end_date, freq, gvkey_list=None):
    """
    Query Compustat Fundamentals table (annual or quarterly).

    Primary key is gvkey-datadate.

    Parameters
    ----------
    db: WRDS connection
    varlist: list
        List of Compustat variables to be queried. See Compustat documentation for available variables.
    start_date: string
        Sample start date (fiscal period end date >= start_date, e.g. '1/31/1930').
    end_date: string
        Sample end date (fiscal period end date <= end_date, e.g. '12/31/2017').
    freq: string
        Data frequency. Must be 'quarterly' or 'annual'. First option queries Compustat's Fundamentals Quarterly table.
        Second one queries Compustat's Fundamentals Annual table.
    gvkey_list: list, optional
        List of gvkeys to be included in the output table. If None, include all.

    Examples
    --------
    df = download_compustat_fund(['saley', 'saleq'], '1960-01-01', '2017-12-31', 'quarterly')

    df = download_compustat_fund(['sale'], '1960-01-01', '2017-12-31', 'annual', gvkey_list=['001000', '315318'])
    """
    start_time = time.time()

    assert freq in ['quarterly', 'annual'], "Parameter freq must be either 'quarterly' or 'annual'."

    fund_table = 'funda' if freq == 'annual' else 'fundq'
    gvkeys = 'AND gvkey IN ({})'.format(', '.join("'" + item + "'" for item in gvkey_list)) if gvkey_list else ''
    varlist = ", ".join(varlist)

    sql = """
          SELECT gvkey, datadate, {}
          FROM comp.{}
          WHERE indfmt = 'INDL'
          AND datafmt = 'STD'
          AND popsrc = 'D'
          AND consol = 'C'
          {}
          AND datadate >= DATE '{}'
          AND datadate <= DATE '{}'
          """.format(varlist, fund_table, gvkeys, start_date, end_date)

    df = db.raw_sql(sql, date_cols=['datadate'])

    if freq == 'quarterly':
        pk_integrity(df, ['gvkey', 'fyr', 'datadate'])
    else:
        pk_integrity(df, ['gvkey', 'datadate'])

    print("Compustat data was successfully downloaded in {} seconds.".format(str(time.time() - start_time)))
    print('Number of observations in the %s dataset (%s to %s): %d' % (freq,
                                                                       str(df.datadate.dt.date.min()),
                                                                       str(df.datadate.dt.date.max()),
                                                                       df.shape[0]))

    return df


# %% Function to merge Compustat and CRSP data
def merge_comp_crsp(db, comp_data, how='left', primary_sec=False, drop_duplicates=True):
    """
    Query CRSP/Compustat merged table (annual or quarterly).

    Output table is Compustat-centric with gvkey-datadate being the primary key.
    Fiscal period end date must be within link date range.
    Quarterly table is a calendar view (in contrast to a fiscal view).

    Indicators of matching quality:
    1. LinkType: A code describing the connection between the CRSP and Compustat data.
        - LU: Link research complete. Standard connection between databases.
        - LC: Non-researched link to issue by CUSIP.
        - LD: Duplicate link to a security. Another GVKEY/IID is a better link to that CRSP record.

    2. LinkPrim: Primary issue indicator for the link.
        /* Optional condition, applied if specified */
        - P: Primary, identified by Compustat in monthly security data.
        - C: Primary, assigned by CRSP to resolve ranges of overlapping or missing primary markers from Compustat
            ... in order to produce one primary security throughout the company history.

    3. UsedFlag: Flag marking whether the link is used in creating the composite record.
        /* Legacy condition, no longer necessary */
        - 1: The link is applicable to the selected PERMNO and used to identify ranges of Compustat data
            ... from a GVKEY used to create a composite GVKEY record corresponding to the PERMNO.
        - -1: The link is informational, indirectly related to the PERMNO, and not used.

    Parameters
    ----------
    db: wrds connection
    comp_data: pandas data-frame
        compustat data
        [gvkey, datadate] as primary key
    how: string
        how to merge the link table with in the input data
    primary_sec: boolean
        whether to consider primary links only
    drop_duplicates: boolean
        whether to keep unique pks at the end

    Examples
    --------
    df = merge_comp_crsp(crsp_data)
    """

    assert how in ['inner', 'left'], "Parameter how must be either 'inner' or 'left'."

    linkprims = "AND linkprim in ('P', 'C')" if primary_sec else ''

    # Reassure pks
    pk_integrity(comp_data, ['gvkey', 'datadate'])

    # Query CCM link table
    sql = """
          SELECT gvkey, liid as ccmxpf_iid, lpermno as permno, lpermco as ccmxpf_permco, 
              linkdt, linkenddt, linktype, linkprim, usedflag
          FROM crspq.ccmxpf_linktable
          WHERE linktype IN ('LC', 'LU', 'LS')
          {}
          """.format(linkprims)

    link_table = db.raw_sql(sql, date_cols=['linkdt', 'linkenddt'])

    # Rank link types in case there are duplicates
    link_table.loc[link_table['linktype'] == 'LC', 'linkscore'] = 1
    link_table.loc[link_table['linktype'] == 'LU', 'linkscore'] = 2
    link_table.loc[link_table['linktype'] == 'LS', 'linkscore'] = 3

    # Merge link table with the CRSP dataset
    df = pd.merge(adata[['gvkey', 'datadate']], link_table, on=['gvkey'], how='inner')

    # Keep valid links only
    df = df[(df.datadate >= df.linkdt) | (df.linkdt.isnull())]
    df = df[(df.datadate <= df.linkenddt) | (df.linkenddt.isnull())]

    # Keep the one with the highest scores if enforced
    if df.duplicated(['gvkey', 'datadate']).sum() > 0:
        print('%d duplicated records due to CCM links.' % (df.duplicated(['gvkey', 'datadate']).sum()))
        if drop_duplicates:
            print('Keep the ones with highest link quality ...')
            df = df.sort_values(by=['gvkey', 'datadate', 'linkscore'])\
                .drop_duplicates(subset=['gvkey', 'datadate'], keep='first')
    else:
        pass

    # Merge valid matches back to the original dataset
    df = pd.merge(adata, df, on=['gvkey', 'datadate'], how=how)
    df.drop(columns=['linkdt', 'linkenddt'], inplace=True)

    return df


# %% Function (two parts) to pre-process Compustat annual and quarterly data
def preprocess(comp_data, id_vars, fpe_vars, datadate='datadate'):
    """
    The annual dataset is identified on ['gvkey', 'datadate'].
    We need 'fyear' data to calculate growth/change measures.
    There are missing values in 'fyear', these records do not have any other information as well.
    These records are likely coming from retrospective reporting of pre-IPO data.
    It makes sense for us to drop these records.
    The dataset would then be identified on ['gvkey', 'fyr', 'fyear'].

    The quarterly dataset is identified on ['gvkey', 'fyr', 'datadate'].
    There are multiple records for some gvkey-datadate pairs due to change of fiscal year-end.
    The idea is to drop duplicates after we quarterize and compute ttm data.
    We need 'fyearq' and 'fqtr' data to quarterize.
    While there is no missing in 'fyearq', 'fqtr' has missing values.
    Two approaches here:
        - Proceed with the dataset and fill missing 'fqtr' by rules; the dataset is still identified on ['gvkey', 'fyr', 'datadate']
        - Drop records with missing 'fqtr'; the dataset would then be identified on ['gvkey', 'fyr', 'fyearq', 'fqtr']
    We take the second approach.

    Parameters
    ----------
    id_vars: list
        set of company(stock) identifiers, e.g., ['gvkey', 'fyr']
    fpe_vars: list
        set of fiscal period identifiers, e.g., ['fyearq', 'fqtr']
    """

    # Create a copy of the original dataframe (it is necessary since we are dropping rows)
    comp_data = comp_data.copy()

    # Drop if fiscal period vars are missing
    c1 = comp_data.shape[0]
    for var in fpe_vars:
        comp_data = comp_data[comp_data[var].notnull()]
    c2 = comp_data.shape[0]
    print('Among %d observations in the raw Compustat dataset,' % c1)
    print('\t %d (%.4f %%) observations dropped for missing %s' % (c1-c2, (c1-c2)/c1*100, '/'.join(fpe_vars)))

    # Ensure intergrity of pks, again
    pk_integrity(comp_data, id_vars+[datadate])
    pk_integrity(comp_data, id_vars+fpe_vars)

    # Drop if sorts on datadate and fp are inconsistent
    comp_data.sort_values(id_vars+[datadate], inplace=True)
    comp_data = comp_data[comp_data.index == comp_data.sort_values(id_vars+fpe_vars).index]
    comp_data.reset_index(drop=True, inplace=True)
    c3 = comp_data.shape[0]
    print('\t %d (%.4f %%) observations dropped for inconsistent datadate and %s' % (c2-c3, (c2-c3)/c1*100, '+'.join(fpe_vars)))
    del c1, c2, c3

    return comp_data



# -------
# %% Main
# -------
if __name__ == "__main__":

    # % Set up WRDS
    import wrds
    db = wrds.Connection(wrds_username='rongchen')

    # %% Construct Compustat and dataframes
    compa = comp(db=db, 
                 varlist=['tic', 'cusip', 'conm', 'fyr', 'iid', 'exchg', 'cik', 'fic'] + ['curcd', 'fyear'] + ['epspi'],
                 start_date=start_date, 
                 end_date=end_date, 
                 freq='annual')
    compq = comp(db=db, 
                 varlist=['tic', 'cusip', 'conm', 'fyr', 'iid', 'exchg', 'cik', 'fic'] + ['curcdq', 'fyearq', 'fqtr'] + ['rdq'],
                 start_date=start_date, 
                 end_date=end_date, 
                 freq='quarterly')
    compq['rdq'] = pd.to_datetime(compq['rdq'])

    # % Preprocess Compustat data
    compa = preprocess(comp_data=compa, id_vars=['gvkey', 'fyr'], fpe_vars=['fyear'])
    compq = preprocess(comp_data=compq, id_vars=['gvkey', 'fyr'], fpe_vars=['fyearq', 'fqtr'])

    # % Assign fourth-quarter EA dates to annual dataframe
    compq = compq[compq['fqtr'] == 4]
    adata = compa.merge(compq[['gvkey', 'datadate']+['curcdq', 'fyearq', 'fqtr']+['rdq']], on=['gvkey', 'datadate'], how='left')
    adata = adata.sort_values(['gvkey', 'datadate'])
    pk_integrity(adata, ['gvkey', 'datadate'])
    del compq, compa

    # % Assign permnos to Compustat data
    adata = merge_comp_crsp(db=db, comp_data=adata, primary_sec=True)

    # % Filters: rdq, permno
    adata = adata[(adata.groupby(['gvkey'])['fyr'].shift().isnull()) | 
                  (adata.groupby(['gvkey'])['fyr'].diff() == 0)]
    adata = adata[adata['rdq'].notnull() & adata['permno'].notnull()]

    # % CRSP daily data
    sql = """
          SELECT permno, date, ret
          FROM crspq.dsf
          WHERE date >= DATE '{}'
          AND date <= DATE '{}'
          """.format(start_date, end_date)
    ddata = db.raw_sql(sql, date_cols=['date'])

    # ----------
    # % Assign effective event day for each EA
    # ----------
    adata['rdq'] = pd.to_datetime(adata['rdq'])
    ddata['date'] = pd.to_datetime(ddata['date'])
    adata['permno'] = adata['permno'].astype('int')
    ddata['permno'] = ddata['permno'].astype('int')
    # % Last avaiable trading day before the annoucement
    adata = pd.merge_asof(adata.sort_values(['rdq', 'permno']), 
                          ddata.sort_values(['date', 'permno']), 
                          left_on='rdq', 
                          right_on='date', 
                          by='permno', 
                          direction='backward'
                         )
    adata = adata.rename(columns={'date': 't0'}).drop(columns=['ret'])
    # % Next avaiable trading day after the annoucement
    adata = pd.merge_asof(adata.sort_values(['rdq', 'permno']), 
                          ddata.sort_values(['date', 'permno']), 
                          left_on='rdq', 
                          right_on='date', 
                          by='permno', 
                          direction='forward',
                          allow_exact_matches=False
                         )
    adata = adata.rename(columns={'date': 't1'}).drop(columns=['ret'])

    # % Output as dta
    adata.to_stata('../data/adata.dta', write_index=False)

    # % Event-view data
    edata = adata[['permno', 't0', 't1']].dropna().drop_duplicates()

    # % Expand to -360 to +180 windows
    edata['dt'] = edata['t1'] - timedelta(days=360)
    edata['enddt'] = edata['t1'] + timedelta(days=180)
    edata['date'] = edata.apply(lambda x: pd.date_range(x['dt'], x['enddt'], freq='d'), axis=1)
    wdata = edata[['permno', 't0', 't1', 'date']].explode('date')

    # % Merge CRSP daily stock returns
    wdata = wdata.merge(ddata, on=['permno', 'date'], how='left')
    del ddata

    # % Query CRSP equal-weighted returns
    ewret = db.raw_sql("""SELECT date, ewretd FROM crspq.dsi""", date_cols=['date'])
    wdata = wdata.merge(ewret, on=['date'], how='left')

    # % Output as dta
    wdata.to_stata('../data/wdata.dta', write_index=False)






