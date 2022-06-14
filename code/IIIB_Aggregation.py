#!/apps/anaconda3/bin/python3

# -----------------
# %% Program setups
# -----------------

# %% General packages
import os
import re
import numpy as np
import pandas as pd

# %% Macro vars
N = 10000

# -------
# %% Main
# -------
if __name__ == "__main__":

	# % Loop through the N distributed results
	paths = [f for f in os.listdir('../temp/') if re.search(r'\d\.csv', f)]
	print("%d iterations are successfully processed." % len(paths))
	# % Bootstrapping statistics
	df = pd.concat([pd.read_csv('../temp/'+x).set_index(['fyear', 'prd']) for x in paths], axis=1)\
			.mean(axis=1).to_frame('stat').unstack().reset_index()
	df.columns = ['fyear', 'stat1', 'stat2', 'stat3']
	# % Formatting
	df['fyear'] = df['fyear'].astype(int)
	df.set_index(['fyear'], inplace=True)
	df = df.round(3)
	df[df<.001] = "$<$0.001"
	df.to_csv('t2c.csv')