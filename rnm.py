import pandas as pd
import pickle as pk
import time
import numpy as np


def refine_data():  # Works as intended
    start = time.time()
    p = pd.read_csv('opendata_projects.csv')
    end = time.time()
    print('Done reading in project data! Took {} seconds!'.format(end-start))
    start = time.time()
    p = p[['_projectid', 'school_latitude', 'school_longitude', 'school_zip', 'total_donations', 'num_donors',
           'date_posted', 'date_expiration']]
    p['_projectid'] = p['_projectid'].apply(to_string)
    p['school_zip'] = p['school_zip'].apply(format_zip_code)
    p['date_posted'] = pd.to_datetime(p['date_posted'])
    p['date_expiration'] = pd.to_datetime(p['date_expiration'])
    end = time.time()
    print('Done refining project data! Took {} seconds!'.format(end-start))
    start = time.time()
    d = pd.read_csv('opendata_donations.csv')
    end = time.time()
    print('Done reading in donation data! Took {} seconds!'.format(end - start))
    start = time.time()
    d = d[['_donationid', '_projectid', 'donor_zip', 'donation_timestamp', 'donation_total', 'dollar_amount']]
    d['_donationid'] = d['_donationid'].apply(to_string)
    d['_projectid'] = d['_projectid'].apply(to_string)
    d['donor_zip'] = d['donor_zip'].apply(format_zip_code)
    d['donation_timestamp'] = pd.to_datetime(d['donation_timestamp'])
    end = time.time()
    print('Done refining donation data! Took {} seconds!'.format(end-start))
    print(p.dtypes)
    print(d.dtypes)
    return p, d


def format_zip_code(x):
    if np.isnan(x):
        return 'None'
    return str(int(x)).zfill(5)


def to_string(x):
    y = str(x)
    return y.replace('"', '')


def pickle_data(project, donation):
    start = time.time()
    with open('project_data.pkl', 'wb') as f:
        pk.dump(project, f)
    end = time.time()
    print('Done pickling project data! Took {} seconds!'.format(end-start))
    start = time.time()
    with open('donations_data.pkl', 'wb') as f:
        pk.dump(donation, f)
    end = time.time()
    print('Done pickling donation data! Took {} seconds!'.format(end - start))


def read_in_data():
    with open('project_data.pkl', 'rb') as f:
        p = pk.load(f)
    with open('donations_data.pkl', 'rb') as f:
        d = pk.load(f)
    return p, d


def read_and_refine_location_csv():  # Must CITE!!!! https://simplemaps.com/data/us-zips
    df = pd.read_csv('uszips.csv')
    df = df[['zip', 'lat', 'lng']]
    return df


def find_longitude(current_zip, location_data):
    start = time.time()
    if current_zip == 'None':
        end = time.time()
        print('Took {} seconds to find longitude!'.format(end-start))
        return 'N/A'
    else:
        longitude = location_data[location_data['zip'] == int(current_zip)]['lng'].values
        if len(longitude) == 0:
            end = time.time()
            print('Took {} seconds to find longitude!'.format(end - start))
            return 'N/A'
        else:
            end = time.time()
            print('Took {} seconds to find longitude!'.format(end - start))
            return longitude[0]


def find_latitude(current_zip, location_data):
    start = time.time()
    if current_zip == 'None':
        end = time.time()
        print('Took {} seconds to find latitude!'.format(end-start))
        return 'N/A'
    else:
        latitude = location_data[location_data['zip'] == int(current_zip)]['lat'].values
        if len(latitude) == 0:
            end = time.time()
            print('Took {} seconds to find latitude!'.format(end-start))
            return 'N/A'
        else:
            end = time.time()
            print('Took {} seconds to find latitude!'.format(end-start))
            return latitude[0]


def read_and_refine_income_metrics():  # Works
    df = pd.read_csv('acs/income.csv')
    df = df[['Geography', 'Median income (dollars); Estimate; Households']]
    df.columns = ['zip', 'median_income']
    df['zip'] = df['zip'].apply(format_geography)
    df['median_income'] = df['median_income'].apply(format_income)
    keys = df['zip'].tolist()
    values = df['median_income'].tolist()
    dictionary = dict(zip(keys, values))
    return dictionary


def format_geography(x):
    spl = x.split(' ')
    return int(spl[1])


def format_income(x):
    if x == '-':
        return 'NaN'
    else:
        return x


def find_income(current_zip, income_data):
    start = time.time()
    if current_zip == 'None':
        end = time.time()
        print("Took {} seconds to find income!".format(end-start))
        return np.nan
    elif int(current_zip) not in income_data.keys():
        end = time.time()
        print('Took [} seconds to find income!'.format(end-start))
        return np.nan
    else:
        end = time.time()
        print('Took {} seconds to find income!'.format(end-start))
        return income_data[int(current_zip)]


def add_income_metrics(df, income_data):
    df['donor_median_income'] = df.apply(lambda row: find_income(row['donor_zip'], income_data), axis=1)
    df['project_median_income'] = df.apply(lambda row: find_income(row['school_zip'], income_data), axis=1)
    return df


start = time.time()
projects, donations = read_in_data()
donor_locations_data = read_and_refine_location_csv()
income_data = read_and_refine_income_metrics()
end = time.time()
print('Done loading in all sets of data! Took {} seconds!'.format(end-start))
start = time.time()
donations['donor_longitude'] = donations.apply(lambda row: find_longitude(row['donor_zip'], donor_locations_data), axis=1)
donations['donor_latitude'] = donations.apply(lambda row: find_latitude(row['donor_zip'], donor_locations_data), axis=1)
donations = add_income_metrics(donations, income_data)
end = time.time()
print('Located all necessary information about donors! Took {} seconds!'.format(end - start))
print(donations.head())
project_ids = projects["_projectid"].tolist()
projects.index = project_ids
corresponding_school_latitude = []
corresponding_school_longitude = []
corresponding_school_zip = []
corresponding_school_total_donations = []
corresponding_school_num_donors = []
corresponding_school_date_posted = []
corresponding_school_date_expired = []
donation_specific_project_ids = donations['_projectid'].tolist()
x = 0
for d_to_p_id in donation_specific_project_ids:
    start = time.time()
    to_add = projects.loc[d_to_p_id]
    k = to_add.to_dict()
    corresponding_school_latitude.append(k['school_latitude'])
    corresponding_school_longitude.append(k['school_longitude'])
    corresponding_school_zip.append(k['school_zip'])
    corresponding_school_total_donations.append(k['total_donations'])
    corresponding_school_num_donors.append(k['num_donors'])
    corresponding_school_date_posted.append(k['date_posted'])
    corresponding_school_date_expired.append(k['date_expiration'])
    end = time.time()
    x += 1
    print('Located project for donation {}. Took {} seconds!'.format(x, end-start))
start = time.time()
donations['school_latitude'] = corresponding_school_latitude
donations['school_longitude'] = corresponding_school_longitude
donations['school_zip'] = corresponding_school_zip
donations['project_total_donations'] = corresponding_school_total_donations
donations['project_num_donors'] = corresponding_school_num_donors
donations['project_posted'] = corresponding_school_date_posted
donations['project_expired'] = corresponding_school_date_expired
end = time.time()
print('Done adding project information column to donation data! Took {} seconds!'.format(end-start))
start = time.time()
print(donations.head())
donations.to_csv("combined.csv", index=False)
end = time.time()
print('Done creating .CSV file of combined data! Took {} seconds!'.format(end-start))
# start = time.time()
# df = pd.read_csv('combined.csv')
# end = time.time()
# print('Done reading in DataFrame! Took {} seconds!'.format(end-start))
# start = time.time()
# id = read_and_refine_income_metrics()
# f = add_income_metrics(df, id)
# print('Done finding median incomes for both donor and school! Took {} seconds!'.format(end-start))
# f.to_csv('merged.csv', index=False)
# project_data, donation_data = refine_data()
# pickle_data(project_data, donation_data)
# main()