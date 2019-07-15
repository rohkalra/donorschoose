import pandas as pd
import numpy as np
import time


chunk_list = []


def update_columns(filename):
	race_data = read_and_refine_race_metrics()
	for chunk in pd.read_csv(filename, chunksize=1000):
		s = time.time()
		updated_data = add_race_metrics(chunk, race_data)
		e = time.time()
		print(updated_data[['donor_total_households', 'project_total_households']])
		print("Processed 1000 rows! Took {} seconds! At current rate, will take {} hours to complete this task...".format(e-s, float(4000*(e-s))/3600))
		chunk_list.append(updated_data)
	return pd.concat(chunk_list)


def add_income_metrics(df, income_data):
	df['donor_median_income'] = df.apply(lambda row: find_income(row['donor_zip'], income_data), axis=1)
	df['project_median_income'] = df.apply(lambda row: find_income(row['school_zip'], income_data), axis=1)
	return df


def add_education_metrics(df, education_data):
	df['donor_less_than_HS_grad'] = df.apply(lambda row: find_education(row['donor_zip'], education_data)[0], axis=1)
	df['donor_HS_grad'] = df.apply(lambda row: find_education(row['donor_zip'], education_data)[1], axis=1)
	df['donor_some_college_or_assoc'] = df.apply(lambda row: find_education(row['donor_zip'], education_data)[2], axis=1)
	df['donor_bach_degree_or_higher'] = df.apply(lambda row: find_education(row['donor_zip'], education_data)[3], axis=1)
	df['project_less_than_HS_grad'] = df.apply(lambda row: find_education(row['school_zip'], education_data)[0], axis=1)
	df['project_HS_grad'] = df.apply(lambda row: find_education(row['school_zip'], education_data)[1], axis=1)
	df['project_some_college_or_assoc'] = df.apply(lambda row: find_education(row['school_zip'], education_data)[2], axis=1)
	df['project_bach_degree_or_higher'] = df.apply(lambda row: find_education(row['school_zip'], education_data)[3], axis=1)
	return df

def add_race_metrics(df, race_data):
	# 'white', 'black', 'american_indian', 'asian', 'pacific_islander', 'other', 'two_or_more', 'two_or_more_including_other', 'three_or_more'
	df['donor_total_households'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[0], axis=1)
	df['donor_white'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[1], axis=1)
	df['donor_black'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[2], axis=1)
	df['donor_american_indian'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[3], axis=1)
	df['donor_asian'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[4], axis=1)
	df['donor_pacific_islander'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[5], axis=1)
	df['donor_other'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[6], axis=1)
	df['donor_two_or_more'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[7], axis=1)
	df['donor_two_or_more_including_other'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[8], axis=1)
	df['donor_three_or_more'] = df.apply(lambda row: find_race(row['donor_zip'], race_data)[9], axis=1)
	df['project_total_households'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[0], axis=1)
	df['project_white'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[1], axis=1)
	df['project_black'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[2], axis=1)
	df['project_american_indian'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[3], axis=1)
	df['project_asian'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[4], axis=1)
	df['project_pacific_islander'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[5], axis=1)
	df['project_other'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[6], axis=1)
	df['project_two_or_more'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[7], axis=1)
	df['project_two_or_more_including_other'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[8], axis=1)
	df['project_three_or_more'] = df.apply(lambda row: find_race(row['school_zip'], race_data)[9], axis=1)
	return df


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
		return np.nan
	elif '-' in x or '+' in x:
		substring = x[:len(x)-1]
		substring = substring.replace(',', '')
		return float(substring)
	else:
		return float(x)


def find_income(current_zip, income_data):
	if current_zip == 'None':
		# print("Took {} seconds to find income!".format(end-start))
		return np.nan
	elif int(current_zip) not in income_data.keys():
		# print('Took {} seconds to find income!'.format(end-start))
		return np.nan
	else:
		# print('Took {} seconds to find income!'.format(end-start))
		return income_data[int(current_zip)]


def read_and_refine_education_metrics():
	df = pd.read_csv('acs/education.csv')
	df = df[['Geography', 'Total; Estimate; Less than high school graduate', 'Total; Estimate; High school graduate (includes equivalency)', "Total; Estimate; Some college or associate's degree", "Total; Estimate; Bachelor's degree or higher"]]
	df.columns = ['zip', 'less_than_HS_grad', 'HS_grad', "some_college_or_assoc", "bach_degree_or_higher"]
	df['educational_info'] = df.apply(lambda row: combine_educations(row['less_than_HS_grad'], row['HS_grad'], row['some_college_or_assoc'], row['bach_degree_or_higher']), axis=1)
	df['zip'] = df['zip'].apply(format_geography)
	keys = df['zip'].tolist()
	values = df['educational_info'].tolist()
	dictionary = dict(zip(keys, values))
	return dictionary


def combine_educations(a, b, c, d):
	return([a, b, c, d])


def find_education(current_zip, education_data):
	if current_zip == 'None':
		# print("Took {} seconds to find income!".format(end-start))
		return [np.nan, np.nan, np.nan, np.nan]
	elif int(current_zip) not in education_data.keys():
		# print('Took {} seconds to find income!'.format(end-start))
		return [np.nan, np.nan, np.nan, np.nan]
	else:
		# print('Took {} seconds to find income!'.format(end-start))
		return education_data[int(current_zip)]


def read_and_refine_race_metrics():
	df = pd.read_csv('acs/race.csv')
	df = df[['Geography', 'Estimate; Total:', 'Estimate; Total: - White alone', 'Estimate; Total: - Black or African American alone', 'Estimate; Total: - American Indian and Alaska Native alone', 'Estimate; Total: - Asian alone', 'Estimate; Total: - Native Hawaiian and Other Pacific Islander alone', 'Estimate; Total: - Some other race alone', 'Estimate; Total: - Two or more races:', 'Estimate; Total: - Two or more races: - Two races including Some other race', 'Estimate; Total: - Two or more races: - Two races excluding Some other race, and three or more races']]
	df.columns = ['zip', 'total_households', 'white', 'black', 'american_indian', 'asian', 'pacific_islander', 'other', 'two_or_more', 'two_or_more_including_other', 'three_or_more']
	df['race_info'] = df.apply(lambda row: combine_races(row['total_households'], row['white'], row['black'], row['american_indian'], row['asian'], row['pacific_islander'], row['other'], row['two_or_more'], row['two_or_more_including_other'], row['three_or_more']), axis=1)
	df['zip'] = df['zip'].apply(format_geography)
	keys = df['zip'].tolist()
	values = df['race_info'].tolist()
	dictionary = dict(zip(keys, values))
	return dictionary


def combine_races(a, b, c, d, e, f, g, h, i, j):
	return([a, b, c, d, e, f, g, h, i, j])


def find_race(current_zip, race_data):
	if current_zip == 'None':
		# print("Took {} seconds to find income!".format(end-start))
		return [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
	elif int(current_zip) not in race_data.keys():
		# print('Took {} seconds to find income!'.format(end-start))
		return [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
	else:
		# print('Took {} seconds to find income!'.format(end-start))
		return race_data[int(current_zip)]


df = update_columns('incomeinfo_with_educationinfo.csv')
start = time.time()
df.to_csv("all_three_info.csv", index=False)
end = time.time()
