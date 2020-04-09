'''
This is an example of working code
It is very much not finished  (and may not be runnabel from beginning to end)
Finished code should (at least) be well organized (ideally all in functions)
and run from start to finish without errors

Data file was too big for the version of Github that I have
You can run the first part of the code with no data
Download data from Ipums
https://usa.ipums.org/usa/index.shtml 
Dataset: PLACE OF WORK AND TRAVEL TIME VARIABLES -- PERSON  
Here are the variables I chose
'PWSTATE2', 'PWCOUNTY', 'PWTYPE', 'TRANWORK'
'''

import pandas as pd 
#import numpy as np
import os

#you'll need to update this to direct to where you have your data saved
path = '~Sarah/Desktop/py_working/us_commute/'

#test:
df =pd.DataFrame()
df['col1'] = [1,0,2,3]
df['col2'] = [5,5,6,7]
df['col3'] = [4,5,3,6]

cleanup_nums = {"col1": {1:'hi', 0:'ci', 2:'pi', 3:'bi'},
                "col2": {5:'be', 6:'pe', 7:'me'}}
df.replace(cleanup_nums, inplace=True)
df.head()
#cite: https://pbpython.com/categorical-encoding.html

df = df.applymap(str) 
#https://datatofish.com/integers-to-strings-dataframe/
for i, x in enumerate(df['col3']):
    df['col3'][i] = x.zfill(3)
    print(df['col3'][i])


df['new'] = df['col3'] + df['col1']
df 


#or in fewer lines:
df['col3'] = df['col3'].apply(lambda x: x.zfill(3))
#df['new'] = df['col2'].apply(lambda x: x.zfill(2)) + df['col1']


#with data
def read_data(path, filename):
    if filename.endswith('.csv'):
        df = pd.read_csv(os.path.join(path, filename))
    elif filename.endswith('.xls'):
        df = pd.read_excel(os.path.join(path, filename))
    else:
        return 'unexpected file type in read_data'
    return df

#2018 only!
df = read_data(path,'usa_00002.csv')
#df.columns

transwork = df.drop(columns= ['SAMPLE', 'SERIAL', 'CBSERIAL', 'HHWT', 'CLUSTER', 'STRATA',
       'GQ', 'PERNUM', 'PERWT'])

#add FIPS code: state+county
#x.ljust(3, '0')
#'PWCOUNTY'
#'PWSTATE2'
transwork = transwork.applymap(str) 

transwork.groupby('PWSTATE2')['PWCOUNTY'].nunique()

transwork['PWCOUNTY'] = transwork['PWCOUNTY'].apply(lambda x: x.zfill(3))
transwork['FIPS'] = transwork['PWSTATE2'].apply(lambda x: x.zfill(2)) + transwork['PWCOUNTY']

#https://datatofish.com/integers-to-strings-dataframe/
#for i, x in enumerate(transwork['PWSTATE2']):
#    transwork['PWSTATE2'][i] = x.zfill(2)
#for i, x in enumerate(transwork['PWCOUNTY']):
#    transwork['PWCOUNTY'][i] = x.zfill(3)

transwork['PWSTATE2'] = transwork['PWSTATE2'].astype(int)
transwork["TRANWORK"] = transwork["TRANWORK"].astype(int)



data_dict = {'PWSTATE2': {
0: None,
1:'Alabama',
2:'Alaska',
4:'Arizona',
5:'Arkansas',
6:'California',
8:'Colorado',
9:'Connecticut',
10:'Delaware',
11:'District of Columbia',
12:'Florida',
13:'Georgia',
15:'Hawaii',
16:'Idaho',
17:'Illinois',
18:'Indiana',
19:'Iowa',
20:'Kansas',
21:'Kentucky',
22:'Louisiana',
23:'Maine',
24:'Maryland',
25:'Massachusetts',
26:'Michigan',
27:'Minnesota',
28:'Mississippi',
29:'Missouri',
30:'Montana',
31:'Nebraska',
32:'Nevada',
33:'New Hampshire',
34:'New Jersey',
35:'New Mexico',
36:'New York',
37:'North Carolina',
38:'North Dakota',
39:'Ohio',
40:'Oklahoma',
41:'Oregon',
42:'Pennsylvania',
44:'Rhode Island',
45:'South Carolina',
46:'South Dakota',
47:'Tennessee',
48:'Texas',
49:'Utah',
50:'Vermont',
51:'Virginia',
53:'Washington',
54:'West Virginia',
55:'Wisconsin',
56:'Wyoming',
61:'Maine-New Hamp-Vermont',
62:'Massachusetts-Rhode Island',
63:'Minn-Iowa-Missouri-Kansas-S Dakota-N Dakota',
64:'Mayrland-Delaware',
65:'Montana-Idaho-Wyoming',
66:'Utah-Nevada',
67:'Arizona-New Mexico',
68:'Alaska-Hawaii',
72:'Puerto Rico',
73:'U.S. outlying area',
74:'United States (1980 Puerto Rico samples)',
80:'Abroad',
81:'Europe',
82:'Eastern Asia',
83:'South Central, South East, and Western Asia',
84:'Mexico',
85:'Other Americas',
86:'Other, nec',
87:'Iraq',
88:'Canada',
90:'Confidential',
99:None},

#'PWTYPE': {
#0:'N/A or abroad',
#1:'In metropolitan area: In central/principal city',
#2:'In metropolitan area: In central city: In CBD',
#3:'In metropolitan area: In central city: Not in CBD',
#4:'In metropolitan area: Not in central/principal city',
#5:'In metropolitan area: Central/principal city status indeterminable (mixed)',
#6:'Not in metropolitan area; abroad; or not reported',
#7:'Not in metropolitan area',
#8:'Not in metropolitan area; or abroad',
#9:'Metropolitan status indeterminable (mixed)',},

    "TRANWORK":	
{00:None,
10:'Auto, truck, or van',
11:'Auto',
12:'Driver',
13:'Passenger',
14:'Truck',
15:'Van',
20:'Motorcycle',
30:'Bus or streetcar',
31:'Bus or trolley bus',
32:'Streetcar or trolley car',
33:'Subway or elevated',
34:'Railroad',
35:'Taxicab',
36:'Ferryboat',
40:'Bicycle',
50:'Walked only',
60:'Other',
70:'Worked at home'}}

transwork.replace(data_dict, inplace=True)
transwork.head()
transwork.shape
transwork.dropna(axis=0, how='any', inplace = True)


#pd.unique(transwork['PWSTATE2'])

#transwork.to_csv(os.path.join(path,'usa_commute_2018.csv'))

#transwork.columns 
#transwork.head()

#metro only:
#df[(df.col1 != 0) & (df.col1 != 1)]
metro = transwork[(transwork.PWTYPE != 0) & (transwork.PWTYPE != 6) 
                   & (transwork.PWTYPE != 7) & (transwork.PWTYPE != 8)
                   & (transwork.PWTYPE != 9)]

def get_percent_by_transtype(transwork):

    state_count = transwork.groupby(['PWSTATE2', 'YEAR']).size().reset_index()
    state_count.rename(columns = {0: "Total"}, inplace = True)
    state_trans_count = transwork.groupby(['PWSTATE2', 'TRANWORK', 'YEAR']).size().reset_index()
    state_trans_count.rename(columns = {0: "Total_by_transit_type"}, inplace = True)
    state_trans_count.columns
    #state_trans_count.head(20)

    merged = state_count.merge(state_trans_count, on = ['PWSTATE2', 'YEAR'], how = 'outer')
    #merged.columns
    #merged.head()

    merged['percent'] = merged['Total_by_transit_type']/merged['Total']
    return merged 



all_trans = get_percent_by_transtype(transwork)
all_trans.to_csv(os.path.join(path,'usa_commute_08-18.csv'))

metro_only = get_percent_by_transtype(metro)
metro_only.to_csv(os.path.join(path,'usa_metro_commute_08-18.csv'))

def get_percent_by_transtype_FIPS(transwork):

    state_count = transwork.groupby(['FIPS']).size().reset_index()
    state_count.rename(columns = {0: "Total"}, inplace = True)
    state_trans_count = transwork.groupby(['FIPS', 'TRANWORK']).size().reset_index()
    state_trans_count.rename(columns = {0: "Total_by_transit_type"}, inplace = True)
    state_trans_count.columns
    #state_trans_count.head(20)

    merged = state_count.merge(state_trans_count, on = ['FIPS'], how = 'outer')
    #merged.columns
    #merged.head()

    merged['percent'] = merged['Total_by_transit_type']/merged['Total']
    return merged 

trans_18 = get_percent_by_transtype_FIPS(transwork)
trans_18.to_csv(os.path.join(path,'usa_commute_18.csv'))

