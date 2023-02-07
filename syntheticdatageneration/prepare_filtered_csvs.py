# %% [markdown]
# ### import packages

# %%
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
    print(df)
    df =df.replace(True,1)
    df = df.replace(False,0)
    df = df.drop(columns=dropColumns) if len(dropColumns) > 0 else df
    #drop columns which are empty
    df.dropna(how='all', axis=1, inplace=True)
    return df

def get_input_df(input_data_file) -> DataFrame:
    inputDf: pd.DataFrame()
    foreignKeys =input_data_file['foreign_keys']
    inputfile="_".join(input_data_file['input_file_path'].split("_")[0:-1]) + ".csv"
    print(inputfile)
    if len(foreignKeys) == 0:
        inputDf = pd.read_csv(filepath_or_buffer=inputfile
                                                ,sep=";"
                                                ,nrows=numberOfRowsFilter
                                                ,low_memory=False) 
    if len(foreignKeys) > 0:
        #only get related foreign key records when foreign keys are defined
        for foreignKey in foreignKeys:
            foreign_key_reference_file_path = f"{os.path.dirname(foreignKey['reference_file'])}/{os.path.basename(foreignKey['reference_file']).split('.')[0]}.csv"
            foreign_key_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=foreign_key_reference_file_path
                                                ,sep=","
                                                ,low_memory=False)[foreignKey['reference_key']]

            foreign_key_df_list = foreign_key_df.to_list()

            for chunk in pd.read_csv(filepath_or_buffer=inputfile
                                                ,sep=";"
                                                , chunksize=10000):
                if 'inputDf' in locals():           
                    inputDf = pd.concat([inputDf,chunk[chunk[foreignKey['foreign_key']].isin(foreign_key_df_list)]])
                else:
                    inputDf = chunk[chunk[foreignKey['foreign_key']].isin(foreign_key_df_list)]

            


    return inputDf

#filter input files based on numberOfRows Parameter
for input_data_file in input_data_files:
    inputfile="_".join(input_data_file['input_file_path'].split("_")[0:-1]) + ".csv"
    filtered_output_data = {"filtered_output_path": f"{os.path.dirname(inputfile)}/{os.path.basename(inputfile).split('.')[0]}_filtered.csv"
                                            ,"candidate_keys": input_data_file['candidate_keys']
                                            ,"description_file_path": f"{destination_file_folder}/{os.path.basename(inputfile).split('.')[0]}_description.json"
                                            ,"synthetic_file_path": f"{destination_file_folder}/{os.path.basename(inputfile).split('.')[0]}_synthentic.csv"
                                            }
    filtered_output_files.append(filtered_output_data)  
    if numberOfRowsFilter > 0:
        inputDf = get_input_df(input_data_file=input_data_file)                              
        inputDf = prepare_dataframe_for_synthesizer(inputDf,dropColumns=input_data_file['drop_columns'], foreignKeys=input_data_file['foreign_keys'])
        inputDf.to_csv(f"{filtered_output_data['filtered_output_path']}"
                        ,sep=","
                        ,index=False
                        ,quoting = csv.QUOTE_NONNUMERIC)


# %%
