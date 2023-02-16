# This script merges the annual NADAC files into one file

# Importing reuired modules

import pandas as pd
from glob import glob
from datetime import datetime

# Working directory info

direc = 'F:/NADAC/'

# Creating a list of all raw data files

files = glob(direc + 'data/raw_data/*')

# Creating one data file

df = pd.DataFrame()

colnames = ['NDC Description', 'NDC', 'NADAC_Per_Unit', 'Effective_Date', 'Pricing_Unit',
            'Pharmacy_Type_Indicator', 'OTC', 'Explanation_Code', 'Classification_for_Rate_Setting',
            'Corresponding_Generic_Drug_NADAC_Per_Unit','Corresponding_Generic_Drug_Effective_Date', 'As_of_Date']

for file in files:

    tmp = pd.read_csv(file)
    tmp.columns = colnames
    df = pd.concat([df, tmp], axis = 0).reset_index(drop = True)

# Remove data outside the scope of this study - yes I could've simply not downloaded the extra data and saved time in creating hydroxy, but I had an edible and don't know what I was dowing, so w/e

droppers = [1 if datetime.strptime(df.As_of_Date[i], '%m/%d/%Y') > datetime(2016, 12, 31) else 0 for i in range(len(df))]
df = pd.concat([df, pd.Series(droppers, name = 'DROP')], axis = 1)
df = df[df.DROP == 1].reset_index(drop = True)
df = df.drop(['DROP'], axis = 1)

# Remove all NDCs that do not extend the full time period of the study

ndcs = list(set(df.NDC.unique()))
periods = len(list(df.As_of_Date.unique()))
complete_obs = []

for ndc in ndcs:
    
    tmp = df[df.NDC == ndc]
    
    if len(tmp) == periods:
        
        complete_obs.append(ndc)

df = df[df.NDC.isin(complete_obs)].reset_index(drop = True)

# Set time index series

dates = list(sorted(set([datetime.strptime(x, '%m/%d/%Y') for x in list(df.As_of_Date.unique())])))
index = [dates.index(datetime.strptime(df.As_of_Date[i], '%m/%d/%Y')) for i in range(len(df))]
df = pd.concat([df, pd.Series(index, name = 'Week')], axis = 1)
df = df.sort_values(['Week'])

# Lagged prices

lagged_price = []
lagged_generic = []

for i in range(len(df)):
    
    try:
        
        tmp = df[df.NDC == df.NDC[i]]
        tmp = tmp[tmp.Week == tmp.Week[i] - 1].reset_index(drop = True)
        lagged_price.append(tmp.NADAC_Per_Unit[0])
        lagged_generic.append(tmp.Corresponding_Generic_Drug_NADAC_Per_Unit[0])
        
    except:
        
        lagged_price.append(None)
        lagged_generic.append(None)

df = pd.concat([df, pd.Series(lagged_price, name = 'NADAC_Per_Unit_Lag'), pd.Series(lagged_generic, name = 'Corresponding_Generic_NADAC_Per_Unit_Lag')], axis = 1)

# Reading in the file with hydroxychloroquine sulfate NDCs

hndcs = pd.read_csv(direc + 'data/ndcs.txt', sep = '\t')
clean = [int(n.replace('-','')) for n in hndcs.NDC]

# Adding hydroxychloroquine data to the main df

hydroxy = [1 if df.NDC[i] in clean else 0 for i in range(len(df))]
df = pd.concat([df, pd.Series(hydroxy, name = 'Hydroxychloroquine')], axis = 1)

# Save the dataframe

df.to_csv(direc + 'data/nadac.csv', index = False)

