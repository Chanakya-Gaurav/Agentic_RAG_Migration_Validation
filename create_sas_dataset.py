import pandas as pd
from sas7bdat import SAS7BDAT

# Read the creditscores.csv file into a pandas DataFrame
df = pd.read_csv('./sample_data/SAS_customers.csv')

# Define the output file name
output_file = './sample_data/SAS_customers.sas7bdat'

# Use the SAS7BDAT library to save the DataFrame to a .sas7bdat file
with SAS7BDAT(output_file, 'w') as sasfile:
    sasfile.write_dataframe(df)

print(f"'{output_file}' has been created.")
