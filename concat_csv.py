import os
import glob
import pandas as pd

csv_files = glob.glob('hashtags/*.{}'.format('csv'))

df_concat = pd.concat([pd.read_csv(f,usecols=(1,2)) for f in csv_files], ignore_index=True)

print(df_concat)

df_concat.to_csv('hashtags_merged.csv')