# Donors Choose: File #4
# Author: Rohan Kalra, Summer 2019

# Import necessary packages
import re 
import time
import math
import statistics
import numpy as np
import pandas as pd
import pickle as pk
import statsmodels.api as sm
import matplotlib.pyplot as plt

from sklearn import metrics
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split 
from statsmodels.genmod.families import Poisson, NegativeBinomial


# Pickles finalized.csv as a dataframe for quicker access later
def pickle_data(data):
    with open('finalized_data.pkl', 'wb') as f:
        pk.dump(data, f)
    print('Done pickling finalized data!')


# Loads in previously pickled finalized.csv dataframe
def load_finalized_data():
	start = time.time()
	with open('finalized_data.pkl', 'rb') as f:
		p = pk.load(f)
	end = time.time()
	print('Done opening data! TT: {}'.format(end-start))
	print('-------------------------------------------')
	return p


# Helper Method
def log(value):
	return np.log(value)

def mean(lst): 
    return statistics.mean(lst)

def standard_deviation(lst):
	return statistics.stdev(lst)

def standardize_list(lst):
	average = mean(lst)
	stdev = standard_deviation(lst)
	lst = [float(x-average)/stdev for x in lst]
	return lst

def removeOutliers(x):
    a = np.array(x)
    upper_quartile = np.percentile(a, 75)
    lower_quartile = np.percentile(a, 25)
    IQR = (upper_quartile - lower_quartile) * 1.5
    quartileSet = (lower_quartile - IQR, upper_quartile + IQR)
    
    result = a[np.where((a >= quartileSet[0]) & (a <= quartileSet[1]))]
    
    return result.tolist()

# Main Method
def main():
	# dataset = pd.read_csv('finalized.csv', index_col=0)
	# pickle_data(dataset)

	# Select part of dataset we wish to work with
	dataset = load_finalized_data()
	dataset = dataset[['donation_total', 'absolute_temporal', 'gcd', 'education_metric', 'income_metric', 'year']]
	dataset = dataset.dropna()
	dataset = dataset.loc[dataset['donation_total'] > 0]

	v_log = np.vectorize(log)
	dataset['donation_total'] = v_log(dataset['donation_total'].tolist())

	# dataset['donation_total'] = standardize_list(dataset['donation_total'].tolist())
	# dataset['absolute_temporal'] = standardize_list(dataset['absolute_temporal'].tolist())
	# dataset['gcd'] = standardize_list(dataset['gcd'].tolist())
	# dataset['education_metric'] = standardize_list(dataset['education_metric'].tolist())
	# dataset['income_metric'] = standardize_list(dataset['income_metric'].tolist())

	# Visualize all input/ouput pairs
	# plt.figure(figsize=(15,15))
	# plt.scatter(dataset['absolute_temporal'], dataset['donation_total'],  color='red')
	# plt.xlabel('absolute_temporal')
	# plt.ylabel('donation_total')
	# plt.show()

	# plt.figure(figsize=(15,15))
	# plt.scatter(dataset['gcd'], dataset['donation_total'],  color='red')
	# plt.xlabel('gcd')
	# plt.ylabel('donation_total')
	# plt.show()

	# plt.figure(figsize=(15,15))
	# plt.scatter(dataset['education_metric'], dataset['donation_total'],  color='red')
	# plt.xlabel('education_metric')
	# plt.ylabel('donation_total')
	# plt.show()

	# plt.figure(figsize=(15,15))
	# plt.scatter(dataset['income_metric'], dataset['donation_total'],  color='red')
	# plt.xlabel('income_metric')
	# plt.ylabel('donation_total')
	# plt.show()

	list_of_absolute_temporal = []
	list_of_gcd = []
	list_of_education_metric = []
	list_of_income_metric = []
	list_of_absolute_temporal_year = []
	list_of_gcd_year = []
	list_of_education_metric_year = []
	list_of_income_metric_year = []

	iteration = 0
	for year, df_year in dataset.groupby('year'):
		if iteration == 0:
			y = df_year['donation_total']
			x = df_year[['absolute_temporal', 'gcd', 'education_metric', 'income_metric']]
			x = sm.add_constant(x)
			model = sm.OLS(y, x)
			# model = sm.GLM(y, x, family=sm.families.Poisson())
			result = model.fit()
			print(result.summary())
			l = result.params.tolist()
			list_of_absolute_temporal.append(l[0])
			list_of_gcd.append(l[1])
			list_of_education_metric.append(l[2])
			list_of_income_metric.append(l[3])
			list_of_absolute_temporal_year.append(year)
			list_of_gcd_year.append(year)
			list_of_education_metric_year.append(year)
			list_of_income_metric_year.append(year)
			print('Finished {} Regression!'.format(year))
			iteration += 1
		else:
			y = df_year['donation_total']
			x = df_year[['absolute_temporal', 'gcd', 'education_metric', 'income_metric']]
			x = sm.add_constant(x)
			model = sm.OLS(y, x)
			# model = sm.GLM(y, x, family=sm.families.Poisson())
			result = model.fit()
			print(result.summary())
			l = result.params.tolist()
			list_of_absolute_temporal.append(l[1])
			list_of_gcd.append(l[2])
			list_of_education_metric.append(l[3])
			list_of_income_metric.append(l[4])
			list_of_absolute_temporal_year.append(year)
			list_of_gcd_year.append(year)
			list_of_education_metric_year.append(year)
			list_of_income_metric_year.append(year)
			print('Finished {} Regression!'.format(year))

	plt.figure(figsize=(15,15))
	plt.scatter(list_of_gcd_year, list_of_gcd,  color='blue')
	plt.xlabel('Year')
	plt.ylabel('GCD Coefficient')
	plt.show()

	plt.figure(figsize=(15,15))
	plt.scatter(list_of_education_metric_year, list_of_education_metric,  color='blue')
	plt.xlabel('Year')
	plt.ylabel('Education Metric Coefficient')
	plt.show()

	plt.figure(figsize=(15,15))
	inc = plt.scatter(list_of_income_metric_year, list_of_income_metric,  color='blue')
	plt.xlabel('Year')
	plt.ylabel('Income Metric Coefficient')
	plt.show()

	plt.figure(figsize=(15,15))
	at = plt.scatter(list_of_absolute_temporal_year, list_of_absolute_temporal,  color='blue')
	plt.xlabel('Year')
	plt.ylabel('Absolute Temporal Coefficient')
	plt.show()

main()




