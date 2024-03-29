# %% [markdown]
# ### import packages

# %%
from DataSynthesizer.DataDescriber import DataDescriber
from DataSynthesizer.DataGenerator import DataGenerator
from DataSynthesizer.ModelInspector import ModelInspector
from DataSynthesizer.lib.utils import read_json_file, display_bayesian_network
import pandas as pd
import csv
import os

# %% [markdown]
# ### user-defined parameteres
# 
# the input_data_files list of dictionaries consists of 4 keys.
# - input_file_path
# - candidate_keys
# - primary_entity

# %%
import json
   
# Opening JSON file
f = open('/data/datafiles/datasynthesizer.json',)

input_data_files = json.load(f)

f.close()
# A filter of number of Rows, use this when you want to filter a number of rows 
# in a big dataset in order to use the Data Synthesizer Frontend. Use 0 if you want to retrieve all rows
numberOfRowsFilter = 20000

# location of two output files
mode = 'independent_attribute_mode'
destination_file_folder = f'/data/datafiles/out/{mode}'


# %%
# An attribute is categorical if its domain size is less than this threshold.
# Here modify the threshold to adapt to the domain size of "education" (which is 14 in input dataset).
threshold_value = 20 


# Number of tuples generated in synthetic dataset.
num_tuples_to_generate = 20000 # Here 32561 is the same as input dataset, but it can be set to another number.

# %% [markdown]
# ### Prepare Dataset for Data Synthesizer
# 
# 1. Data Synthesizer has some requirements for the dataset (for example, no boolean datatypes or columns which have 0 values)
# 2. To accomodate to these requirements, the dataset will be prepared in the next step.

# %%
from pandas import DataFrame


filtered_output_files: list = []

#filter input files based on numberOfRows Parameter
for input_data_file in input_data_files:
    inputfile="_".join(input_data_file['input_file_path'].split("_")[0:-1]) + ".csv"
    filtered_output_data = {"filtered_output_path": f"{os.path.dirname(inputfile)}/{os.path.basename(inputfile).split('.')[0]}_filtered.csv"
                                            ,"candidate_keys": input_data_file['candidate_keys']
                                            ,"description_file_path": f"{destination_file_folder}/{os.path.basename(inputfile).split('.')[0]}_description.json"
                                            ,"synthetic_file_path": f"{destination_file_folder}/{os.path.basename(inputfile).split('.')[0]}_synthentic.csv"
                                            }
    filtered_output_files.append(filtered_output_data)  

# %% [markdown]
# ### DataDescriber
# 
# 1. Instantiate a DataDescriber.
# 2. Compute the statistics of the dataset.
# 3. Save dataset description to a file on local machine.

# %%
for filtered_data_file in filtered_output_files:
        filteredDf: pd.DataFrame = pd.read_csv(filepath_or_buffer=filtered_data_file['filtered_output_path']
                                                ,sep=","
                                                ,low_memory=False) 
        
        columns: pd.Index = filteredDf.columns

        categorical_attributes = {x:True for x in filteredDf.columns if x not in filtered_data_file['candidate_keys']}
        candidate_keys = {x:True for x in filtered_data_file['candidate_keys']}
        print(threshold_value)
        describer: DataDescriber = DataDescriber(category_threshold=threshold_value)
        print(categorical_attributes)
        print(filtered_data_file['filtered_output_path'])
        describer.describe_dataset_in_independent_attribute_mode(dataset_file=filtered_data_file['filtered_output_path'],
                                                                epsilon=10,
                                                                attribute_to_is_categorical=categorical_attributes,
                                                                attribute_to_is_candidate_key=candidate_keys
                                                                ,seed=0)
        describer.save_dataset_description_to_file(filtered_data_file['description_file_path'])

        ### Step 4 generate synthetic dataset

        #1. Instantiate a DataGenerator.
        #2. Generate a synthetic dataset.
        #3. Save it to local machine.
        generator: DataGenerator = DataGenerator()

        generator.generate_dataset_in_independent_mode(num_tuples_to_generate, filtered_data_file['description_file_path'])
        generator.save_synthetic_data(filtered_data_file['synthetic_file_path'])


