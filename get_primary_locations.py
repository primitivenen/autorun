import pandas as pd
# pd.set_option('display.max_columns', 100)
from scipy.stats import mode
import geocoder
import time
import os
import sys
sys.path.insert(0, '/home/stang/user-profile/stats-core')
from utils import load_header_config

def truncate(f, n):
    '''Truncates/pads a float f to n decimal places without rounding'''
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])

processed = set()
with open('res.txt', 'r') as inf:
    for line in inf:
        processed.add(line.split(',')[0])

base = '/gaei/gacrnd/data/ag_by_vin/'
files = sorted([f for f in os.listdir(base) if 'vin_L' in f])
header, configs = load_header_config('aglog_header.csv')
for i, f in enumerate(files):
    vin = f.split('.')[0].split('_')[1]
    
    if vin in processed:
        continue

    fname = base + f
    df = pd.read_csv(fname, header=None, names=header, index_col=False)
    df = df.iloc[:, 74:76].dropna()

    df.columns = ['lon', 'lat']
    df['lon'] = df['lon'].apply(lambda x: truncate(x, 2))
    df['lat'] = df['lat'].apply(lambda x: truncate(x, 2))
    df = df[(df['lat'] != '0.00') & (df['lon'] != '0.00')]
    df = df.iloc[-10000:, :]
    
    df['coord'] = df['lat'] + '+' + df['lon']
    
    lat, lon = df['coord'].mode()[0].split('+')
    with open('res.txt', 'a') as outf:
        outf.write('{},{},{}\n'.format(vin, lat, lon))
    print(i, vin, lat, lon)
    print('progress: {:.2f}%'.format(float(i)/len(files)*100))
    print('-----------------------')
print('done')

if not os.path.exists('veh_locs.csv'):
    d = {} # save address info, <vin, (city, province)>
    temp = pd.read_csv('final_veh_locs.csv', dtype='str')
    temp['coord']  = temp['lat'] + '+' + temp['lon']
    for key, city, province in zip(temp['coord'].tolist(), temp['city'].tolist(), temp['province'].tolist()):
        if key not in d:
            d[key] = (city, province)
    print(len(d))


    df1 = pd.read_csv('res.txt', dtype='str')
    df1.columns = ['vin', 'lat', 'lon']
    lats = df1['lat'].tolist()
    lons = df1['lon'].tolist()


    print('reverse geocoding coords..')
    for lat, lon in zip(lats, lons):
        key = lat + '+' + lon
        if key not in d:
            g = geocoder.gaode([float(lat), float(lon)], method='reverse', key='0716e5809437f14e3dd0793a5c6d2b13')
            while not g.ok:
                print('retrying..')
                time.sleep(5)
                g = geocoder.gaode([float(lat), float(lon)], method='reverse', key='0716e5809437f14e3dd0793a5c6d2b13')
            d[key] = (g.city, g.state)
    print('done.')


    fields = ['city', 'province']
    df1[fields] = df1.apply(lambda row: pd.Series(d[row['lat']+'+'+row['lon']]), axis=1)
    df1 = df1.sort_values(by='vin')
    df1 = df1.loc[:, ['vin', 'lat', 'lon', 'city', 'province']]
    df1.to_csv('veh_locs.csv', encoding='utf-8')

    print('done.')
