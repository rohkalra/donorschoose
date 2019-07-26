# Donors Choose: File #1
# Author: Rohan Kalra, Summer 2019


# Import necessary packages
import time
import numpy as np
import pandas as pd
import pickle as pk


# Reads data from original .csv file and formats to our liking
def read_data():

    start = time.time()
    p = pd.read_csv('opendata_projects.csv')
    p = p[['_projectid', 'school_latitude', 'school_longitude', 'school_zip', 'total_donations', 'num_donors','date_posted', 'date_expiration']]
    p['_projectid'] = p['_projectid'].apply(format_id)
    p['school_zip'] = p['school_zip'].apply(format_zip)
    p['date_posted'] = pd.to_datetime(p['date_posted'])
    p['date_expiration'] = pd.to_datetime(p['date_expiration'])
    end = time.time()
    print('Done reading in project data! TT: {}'.format(end-start))

    start = time.time()
    d = pd.read_csv('opendata_donations.csv')
    d = d[['_donationid', '_projectid', 'donor_zip', 'donation_timestamp', 'donation_total']]
    d = d.dropna()
    d['_donationid'] = d['_donationid'].apply(format_id)
    d['_projectid'] = d['_projectid'].apply(format_id)
    d['donor_zip'] = d['donor_zip'].apply(format_zip)
    d['donation_timestamp'] = pd.to_datetime(d['donation_timestamp'])
    end = time.time()
    print('Done reading in donation data! TT: {}'.format(end-start))

    return p, d


# Pickle data for quicker access later
def pickle_data(project, donation):

    start = time.time()
    with open('project.pkl', 'wb') as f:
        pk.dump(project, f)
    end = time.time()
    print('Done pickling project data! TT: {}'.format(end-start))

    start = time.time()
    with open('donation.pkl', 'wb') as f:
        pk.dump(donation, f)
    end = time.time()
    print('Done pickling donation data! TT: {}'.format(end-start))


# Read in previously pickled data
def read_pickled_data():

    start = time.time()
    with open('project.pkl', 'rb') as f:
        p = pk.load(f)
    end = time.time()
    print('Done reading in project data! TT {}'.format(end-start))

    start = time.time()
    with open('donation.pkl', 'rb') as f:
        d = pk.load(f)
    end = time.time()
    print('Done reading in donation data! TT: {}'.format(end-start))

    return p, d


# Random helper Methods
def format_id(x):
    y = str(x)
    return y.replace('"', '')


def format_zip(x):
    if np.isnan(x):
        return np.nan
    else:
        return str(int(x)).zfill(5)


def find_longitude(current_zip, location_data):

    start = time.time()
    longitude = location_data[location_data['zip'] == int(current_zip)]['lng'].values
    if len(longitude) == 0:
        end = time.time()
        print('Found donor longitude! TT: {}'.format(end-start))
        return 'N/A'
    else:
        end = time.time()
        print('Found donor longitude! TT: {}'.format(end-start))
        return longitude[0]


def find_latitude(current_zip, location_data):

    start = time.time()
    latitude = location_data[location_data['zip'] == int(current_zip)]['lat'].values
    if len(latitude) == 0:
        end = time.time()
        print('Found donor latitude! TT: {}'.format(end-start))
        return 'N/A'
    else:
        end = time.time()
        print('Found donor latitude! TT: {}'.format(end-start))
        return latitude[0]


# Reads location .csv file and returns a dataframe with coordinates for each zip code
def read_location_data():  # Must CITE!!!! https://simplemaps.com/data/us-zips
    
    start = time.time()
    df = pd.read_csv('uszips.csv')
    df = df[['zip', 'lat', 'lng']]
    end = time.time()
    print('Done reading in location data! TT: {}'.format(end-start))
    
    return df


# Finds longitude and then latitude for each donation entry in data
def add_location_information(df, location_data):

    # Longitude
    start = time.time()
    df['donor_longitude'] = df.apply(lambda row: find_longitude(row['donor_zip'], location_data), axis=1)
    end = time.time()
    print('Found all donor longitudes! TT: {}'.format(end-start))

    # Latitude
    start = time.time()
    df['donor_latitude'] = df.apply(lambda row: find_latitude(row['donor_zip'], location_data), axis=1)
    end = time.time()
    print('Found all donor latitudes! TT: {}'.format(end-start))

    # Returns a dataframe with all the donor coordinates included as new columns
    return df


# Returns a combined dataframe with important project information for each donation entry
def combine_dataframes(projects, donations):

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
    donations['school_latitude'] = corresponding_school_latitude
    donations['school_longitude'] = corresponding_school_longitude
    donations['school_zip'] = corresponding_school_zip
    donations['project_total_donations'] = corresponding_school_total_donations
    donations['project_num_donors'] = corresponding_school_num_donors
    donations['project_posted'] = corresponding_school_date_posted
    donations['project_expired'] = corresponding_school_date_expired
    print('Done adding project information column to donation data!')

    return donations


# Main method
def main():
    # project_data, donation_data = read_data()
    # pickle_data(project_data, donation_data)
    print("'TT' = 'Time Taken'")
    print('----------------------------------')
    projects, donations = read_pickled_data()
    print('----------------------------------')
    location_data = read_location_data()
    print('----------------------------------')
    donations = add_location_information(donations, location_data)
    print('----------------------------------')
    combined = combine_dataframes(projects, donations)
    print('----------------------------------')
    start = time.time()
    combined.to_csv("combined.csv", index=False)
    end = time.time()
    print('Done creating .CSV file of combined data! TT: {}'.format(end-start))


main()

