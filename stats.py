import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt  


dataset = pd.read_csv('finalized.csv', index_col=0)
plt.scatter(dataset['education_metric'].tolist()[:50], dataset['donation_total'].tolist()[:50])  
plt.show()