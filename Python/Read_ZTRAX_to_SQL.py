"""
Created on Feb 14, 2019
@author: matthew wibbenmeyer
"""

# Import ZTRAX data to SQLite database for easy merging and queries
####################################################################################################

import csv, sqlite3, os
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt

os.chdir("L:/Zillow/")

def read_ZAsmt_to_SQL(state_code, table_name, db, keepcols = []):

    print '\nReading {} table for state {} to MySql database...'.format(table_name, state_code)

    disk_engine = create_engine('sqlite://{}'.format(db))

    path = 'ZAsmtHist/{}.txt'.format(state_code, table_name)

    #Layout_mod is a layout table containing datatype column, downloaded from ZTRAX github
    layout = pd.read_excel('{}/ZAsmtHist/Layout_mod.xlsx'.format(state_code))
    layout_temp = layout.loc[layout['TableName'] == 'ut%s'%table_name, :].reset_index()
    names = layout_temp['FieldName']
    dtype = layout_temp['PandasDataType'].to_dict()
    encoding='ISO-8859-1'
    sep='|'
    header=None
    quoting=3
    chunksize=500000

    t0=dt.datetime.now()
    j=0
    index_start=1

    for chunk in pd.read_csv(path, names=names, quoting=quoting, dtype=dtype, encoding=encoding, sep=sep,
        header=header, iterator=True, chunksize = chunksize):

        chunk.index += index_start

        #Keep only specified columns
        if keepcols == []: keepcols=chunk.columns.values
        print keepcols
        print chunk.head()
        chunk = chunk[keepcols]

        #Append chunk to sql table, WARNING: if code is rerun, this will reappend rows to existing table
        dbtable = '{}'.format(table_name.lower())
        chunk.to_sql(dbtable, disk_engine, if_exists='append')

        j+= 1
        td = dt.datetime.now() - t0
        minutes, seconds = divmod(td.seconds, 60)
        elapsed = "%s:%s:%s" %(str(td.seconds//3600).zfill(2), str(minutes).zfill(2), str(seconds).zfill(2))
        index_start = chunk.index[-1]

        print '\t{}: completed {} rows'.format(elapsed, j*chunksize)
