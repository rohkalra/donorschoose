# Donors Choose: File #3
# Author: Rohan Kalra, Summer 2019


# Import necessary packages
import pandas as pd
import numpy as np
import datetime as dt
import re
from geopy import distance
import time
import pickle as pk


# Calculates metrics for 1000 rows at a time
def add_distance_metrics():
    chunk_list = []
    for chunk in pd.read_csv('ready_for_metrics.csv', chunksize=1000):
        s = time.time()
        updated_data = update_data(chunk)
        e = time.time()
        print("Processed 1000 rows! Took {} seconds! At current rate, will take {} hours to complete this task...".format(e-s, float(4000*(e-s))/3600))
        chunk_list.append(updated_data)
    return pd.concat(chunk_list)


# Method that calculates the metrics for given data and adds them to the dataframe
def update_data(df):
    v_gcd = np.vectorize(great_circle_distance)
    df['gcd'] = v_gcd(df['school_latitude'].tolist(), df['school_longitude'].tolist(), df['donor_latitude'].tolist(), df['donor_longitude'].tolist())
    df['gcd'] = pd.to_numeric(df['gcd'], downcast='float')
    v_absolute = np.vectorize(absolute)
    df['absolute_temporal'] = v_absolute(df['project_posted'].tolist(), df['project_expired'].tolist(),
                                         df['donation_timestamp'].tolist())
    df['absolute_temporal'] = pd.to_numeric(df['absolute_temporal'], downcast='float')
    v_normalized = np.vectorize(normalized)
    df['normalized_temporal'] = v_normalized(df['project_posted'].tolist(), df['project_expired'].tolist(),
                                         df['donation_timestamp'].tolist())
    df['normalized_temporal'] = pd.to_numeric(df['normalized_temporal'], downcast='float')
    v_income = np.vectorize(income)
    df['income_metric'] = v_income(df['project_median_income'].tolist(), df['donor_median_income'])
    v_education = np.vectorize(education)
    df['education_metric'] = v_education(df['donor_less_than_HS_grad'].tolist(), df['donor_HS_grad'].tolist(), 
    df['donor_some_college_or_assoc'].tolist(), df['donor_bach_degree_or_higher'].tolist(), df['project_less_than_HS_grad'].tolist(), df['project_HS_grad'].tolist(), df['project_some_college_or_assoc'].tolist(), df['project_bach_degree_or_higher'].tolist())
    v_year = np.vectorize(year)
    df['year'] = v_year(df['donation_timestamp'].tolist())
    return df


# Random helper methods
def great_circle_distance(lat1, long1, lat2, long2):
    if np.isnan(lat2) or np.isnan(long2):
        return np.nan
    else:
        x = distance.great_circle((lat2, long2), (lat1, long1)).miles
        return x


def absolute(posted, expired, timestamp):
    if isinstance(posted, float) or isinstance(expired, float):
        return np.nan
    else:
        start = re.findall(r"[\w']+", posted)
        end = re.findall(r"[\w']+", expired)
        if end[0] == 'NaT':
            return np.nan
        else:
            timestamp = re.findall(r"[\w']+", timestamp)
            date_posted = dt.datetime(int(start[0]), int(start[1]), int(start[2]), 0, 0, 0)
            donation_timestamp = dt.datetime(int(timestamp[0]), int(timestamp[1]), int(timestamp[2]),
                                             int(timestamp[3]), int(timestamp[4]), int(timestamp[5]))
            time_after_start = (donation_timestamp - date_posted).total_seconds()
            return time_after_start


def normalized(posted, expired, timestamp):
    if isinstance(posted, float) or isinstance(expired, float):
        return np.nan
    else:
        start = re.findall(r"[\w']+", posted)
        end = re.findall(r"[\w']+", expired)
        if end[0] == 'NaT':
            return np.nan
        else:
            timestamp = re.findall(r"[\w']+", timestamp)
            date_posted = dt.datetime(int(start[0]), int(start[1]), int(start[2]), 0, 0, 0)
            date_expiration = dt.datetime(int(end[0]), int(end[1]), int(end[2]), 0, 0, 0)
            donation_timestamp = dt.datetime(int(timestamp[0]), int(timestamp[1]), int(timestamp[2]),
                                             int(timestamp[3]), int(timestamp[4]), int(timestamp[5]))
            time_after_start = (donation_timestamp - date_posted).total_seconds()
            time_project_available = (date_expiration - date_posted).total_seconds()
            return abs(float(time_after_start) / float(time_project_available))


def income(school_income, donor_income):
	if np.isnan(float(school_income)) or np.isnan(float(donor_income)):
		return np.nan
	else:
		return abs(float(school_income) - float(donor_income))


def year(timestamp):
    timestamp = re.findall(r"[\w']+", timestamp)
    return int(timestamp[0])


def convert(x):
	if x == '-':
		return np.nan
	else:
		return float(x)


def education(d1, d2, d3, d4, s1, s2, s3, s4):
    if d1 == '-' or s1 == '-' or d1 == np.nan or s1 == np.nan:
        return np.nan
    else:
        return abs(float(d1)-float(s1)) + abs(float(d2)-float(s2)) + abs(float(d3)-float(s3)) + abs(float(d4)-float(s4))


# Main Method
def main():
    start = time.time()
    finalized_data = add_distance_metrics()
    end = time.time()
    print(finalized_data.head())
    start = time.time()
    finalized_data.to_csv("finalized.csv", index=False)
    end = time.time()
    print('Done creating .CSV file of finalized data! Took {} seconds!'.format(end-start))


main()

