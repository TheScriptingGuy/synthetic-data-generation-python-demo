# %% [markdown]
# # DataSynthesizer Usage (correlated attribute mode)
# 
# > This is a quick demo to use DataSynthesizer in correlated attribute mode.
# 
# ### Step 1 import packages

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
f = open('/data/datafiles/dataset_configuration.json',)

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

# A parameter in Differential Privacy. It roughly means that removing a row in the input dataset will not 
# change the probability of getting the same output more than a multiplicative difference of exp(epsilon).
# Increase epsilon value to reduce the injected noises. Set epsilon=0 to turn off differential privacy.
epsilon = 1

# The maximum number of parents in Bayesian network, i.e., the maximum number of incoming edges.
degree_of_bayesian_network = 2

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

def prepare_dataframe_for_synthesizer(df: DataFrame, dropColumns: list, foreignKeys) -> DataFrame:
    #replace boolean values by bit, because synthesizer does not accept boolean type
    df =df.replace(True,1)
    df = df.replace(False,0)
    df = df.drop(columns=dropColumns) if len(dropColumns) > 0 else df
    #drop columns which are empty
    df.dropna(how='all', axis=1, inplace=True)
    return df

def get_input_df(input_data_file) -> DataFrame:
    inputDf: pd.DataFrame()
    foreignKeys =input_data_file['foreign_keys']
    if len(foreignKeys) == 0:
        inputDf = pd.read_csv(filepath_or_buffer=input_data_file['input_file_path']
                                                ,sep=";"
                                                ,nrows=numberOfRowsFilter
                                                ,low_memory=False) 
    if len(foreignKeys) > 0:
        #only get related foreign key records when foreign keys are defined
        for foreignKey in foreignKeys:
            foreign_key_reference_file_path = f"{os.path.dirname(foreignKey['reference_file'])}/{os.path.basename(foreignKey['reference_file']).split('.')[0]}_filtered.csv"
            foreign_key_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=foreign_key_reference_file_path
                                                ,sep=","
                                                ,low_memory=False)[foreignKey['reference_key']]

            foreign_key_df_list = foreign_key_df.to_list()

            for chunk in pd.read_csv(filepath_or_buffer=input_data_file['input_file_path']
                                                ,sep=";"
                                                , chunksize=10000):
                if 'inputDf' in locals():           
                    inputDf = pd.concat([inputDf,chunk[chunk[foreignKey['foreign_key']].isin(foreign_key_df_list)]])
                else:
                    inputDf = chunk[chunk[foreignKey['foreign_key']].isin(foreign_key_df_list)]

            


    return inputDf

#filter input files based on numberOfRows Parameter
for input_data_file in input_data_files:
    filtered_output_data = {"filtered_output_path": f"{os.path.dirname(input_data_file['input_file_path'])}/{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_filtered.csv"
                                            ,"candidate_keys": input_data_file['candidate_keys']
                                            ,"description_file_path": f"{destination_file_folder}/{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_description.json"
                                            ,"synthetic_file_path": f"{destination_file_folder}/{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_synthentic.csv"
                                            }
    filtered_output_files.append(filtered_output_data)  
    if numberOfRowsFilter > 0:
        inputDf = get_input_df(input_data_file=input_data_file)                              
        inputDf = prepare_dataframe_for_synthesizer(inputDf,dropColumns=input_data_file['drop_columns'], foreignKeys=input_data_file['foreign_keys'])
        inputDf.to_csv(f"{filtered_output_data['filtered_output_path']}"
                        ,sep=","
                        ,index=False
                        ,quoting = csv.QUOTE_NONNUMERIC)


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
        describer.describe_dataset_in_correlated_attribute_mode(dataset_file=filtered_data_file['filtered_output_path'], 
                                                        epsilon=epsilon, 
                                                        k=degree_of_bayesian_network,
                                                        attribute_to_is_categorical=categorical_attributes,
                                                        attribute_to_is_candidate_key=candidate_keys)
        describer.save_dataset_description_to_file(filtered_data_file['description_file_path'])

        ### Step 4 generate synthetic dataset

        #1. Instantiate a DataGenerator.
        #2. Generate a synthetic dataset.
        #3. Save it to local machine.
        generator: DataGenerator = DataGenerator()

        generator.generate_dataset_in_correlated_attribute_mode(num_tuples_to_generate, filtered_data_file['description_file_path'])
        generator.save_synthetic_data(filtered_data_file['synthetic_file_path'])

# %%
display_bayesian_network(describer.bayesian_network)
