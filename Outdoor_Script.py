
import sys

def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()

import time


# Above Function is used to create Progress bar





# importing basic libraries
import numpy as np
import pandas as pd

dff = pd.read_csv('WiSenseOutdoorData.csv', header=None)  # reading the data set

# Renaming the column names


df = dff.rename(columns={0: 'timeStamp', 1: 'nodeAddress', 2: 'packteID', 3: 'nodeRSSI', 4: 'nodeVolt', 5: 'temperature1', 6: 'temperature2', 7: 'temperature3', 8: 'pressure', 9: 'luminosity', 10: 'rainfall', 11: 'solarPanelVolt', 12: 'solarPanelBattVolt', 13: 'solarPanelCurr'})

data = df.copy()


#Converting datatype of 'timeStamp' to datetime type
data['timeStamp'] = pd.to_datetime(data['timeStamp'])  


# Now We will create two new columns in our Dataset namely, 'temp3_changed' and 'pressure_changed'
# These column will contain value '1' if temperature3 or pressure is changed else it will contain 0
data['temp3_changed'] = 0
data['pressure_changed'] = 0




#Checking Outliers
'''
# Following Scripts will deal with first value of each node if it is outlier

1. We'll just check if first value of each node for a particular column is outlier (i.e temperature > 100 or temperature < 0), if it is outlier then we'll change its value to next row value

'''


#from tqdm import tqdm_notebook

nodes = data['nodeAddress'].unique() # this line will create an array having total unique nodes

print('Checking Outlier for temperature3')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data.shape[0] - 1):
        if(data.loc[i , 'nodeAddress'] == n):
            val0 = float(data.loc[i,'temperature3'])
            if(val0 < 0 or val0 > 100):
                data.loc[i,'temperature3'] = data.loc[i+1,'temperature3']
                print('Outlier Found at', i , 'for node' , n)
                break
            else:
                break
                
                
                
print('Checking Outlier for pressure')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data.shape[0] - 1):
        if(data.loc[i , 'nodeAddress'] == n):
            val0 = float(data.loc[i,'pressure'])
            if(val0 < 750 or val0 > 1000):
                data.loc[i,'pressure'] = data.loc[i+1,'pressure']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break

                                




nodes = data['nodeAddress'].unique() # this line will create an array having total unique nodes

#Function to clean 'temperature3'

def temperature3_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'temperature3'])
            time0 = df.loc[i,'timeStamp' ]
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'temperature3'])
                time1 = df.loc[j , 'timeStamp']
                timedelta = (time1 - time0)
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'temperature3'] = val0
                  df.loc[j, 'temp3_changed'] = 1
                  k = j
                  break
                elif((abs(val1) > 100 or abs(val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'temperature3'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                    
                    
# Function to clean 'pressure'

def pressure_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'pressure'])
            time0 = df.loc[i,'timeStamp' ]
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'pressure'])
                time1 = df.loc[j , 'timeStamp']
                timedelta = (time1 - time0)
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'pressure'] = val0
                  df.loc[j, 'pressure_changed'] = 1
                  k = j
                  break
                elif((abs(val1) > 1000 or abs(val1) < 750 ) and minutes > 30.0):
                  df.loc[j,'pressure'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break



temperature3_clean(data)
pressure_clean(data)

data.to_csv('kaish.csv')
