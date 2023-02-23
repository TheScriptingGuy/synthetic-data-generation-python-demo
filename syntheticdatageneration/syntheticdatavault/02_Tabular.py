# %% [markdown]
# GaussianCopula Model
# ====================
# 
# In this guide we will go through a series of steps that will let you
# discover functionalities of the `GaussianCopula` model, including how
# to:
# 
# -   Create an instance of a `GaussianCopula`.
# -   Fit the instance to your data.
# -   Generate synthetic versions of your data.
# -   Use `GaussianCopula` to anonymize PII information.
# -   Specify the column distributions to improve the output quality.
# 
# What is GaussianCopula?
# -----------------------
# 
# The `sdv.tabular.GaussianCopula` model is based on [copula
# funtions](https://en.wikipedia.org/wiki/Copula_%28probability_theory%29).
# 
# In mathematical terms, a *copula* is a distribution over the unit cube
# ${\displaystyle [0,1]^{d}}$ which is constructed from a multivariate
# normal distribution over ${\displaystyle \mathbb {R} ^{d}}$ by using the
# probability integral transform. Intuitively, a *copula* is a
# mathematical function that allows us to describe the joint distribution
# of multiple random variables by analyzing the dependencies between their
# marginal distributions.
# 
# Let\'s now discover how to learn a dataset and later on generate
# synthetic data with the same format and statistical properties by using
# the `GaussianCopula` model.
# 
# Quick Usage
# -----------
# 
# We will start by loading one of our demo datasets, the
# `student_placements`, which contains information about MBA students that
# applied for placements during the year 2020.

# %%
from sdv import Metadata
metadata = Metadata('/data/datafiles/out/sdv/sdv_metadata_2.json')
metadata
# %%
import dask.dataframe as dd
import json

# import CSV Function
# %%

def readCSVFile(input_data_file):
      original_datafile_path= input_data_file['input_file_path']
      
      originalDfOut: dd

      #Read into Dask Dataframes
      originalDf: dd = dd.read_csv(urlpath=original_datafile_path,sep=",")
      return originalDf

# Add a Table
# -----------
# 
# Once you have your `Metadata` instance ready you can start adding
# tables.
# 
# In this example, you will add the table `users`, which is the parent
# table of your dataset, indicating which is its Primary Key field,
# `user_id`.
# 
# Note that indicating the Primary Key is optional and can be skipped if
# your table has none, but if a table does not have one, you will not be
# able to add any child tables to it.

# %%
#add metadata to earlier added metadata variable
def add_sdv_metadata(input_data_file):
    if len(input_data_file['foreign_keys']) == 0:
        metadata.add_table(
        name=input_data_file['name'],
        data=tables[input_data_file['name']],
        primary_key=input_data_file['candidate_keys'][0]
        )

# Add a Child Table
# -----------------
# 
# 
# With this, apart from analyzing all the columns and indicating the
# primary key like in the previous step, the `Metadata` instance will
# specify a relationship between the two tables by adding a property to
# the `user_id` field that indicates that it is related to the `user_id`
# field in the `users` table.


    else:
        metadata.add_table(

            name=input_data_file['name'],
            data=tables[input_data_file['name']],
            primary_key=input_data_file['candidate_keys'][0],
            parent=input_data_file['foreign_keys'][0]['reference_table_name'],
            foreign_key=input_data_file['foreign_keys'][0]['foreign_key']

        )


# %% [markdown]
# As you can see, this table contains information about students which
# includes, among other things:
# 
# -   Their id and gender
# -   Their grades and specializations
# -   Their work experience
# -   The salary that they were offered
# -   The duration and dates of their placement
# 
# You will notice that there is data with the following characteristics:
# 
# -   There are float, integer, boolean, categorical and datetime values.
# -   There are some variables that have missing data. In particular, all
#     the data related to the placement details is missing in the rows
#     where the student was not placed.
# 
# Let us use the `GaussianCopula` to learn this data and then sample
# synthetic data about new students to see how well the model captures the
# characteristics indicated above. In order to do this you will need to:
# 
# -   Import the `sdv.tabular.GaussianCopula` class and create an instance
#     of it.
# -   Call its `fit` method passing our table.
# -   Call its `sample` method indicating the number of synthetic rows
#     that you want to generate.

# %%
from sdv.tabular import GaussianCopula,  CTGAN, CopulaGAN
from sdv.lite import TabularPreset


print(metadata)

json_path = f"/data/datafiles/syntheticdatavault.json"


try:
   f = open(json_path,)
   input_data_files = json.load(f)
   f.close()
         
except:
   print('Could not find selected file:', json_path)

if input_data_files is not None:
   tables = {}
   for file in input_data_files:
        df = readCSVFile(file)
        name = file["name"]
        tablekeyvaluepair = {file["name"]:df.compute()}
        tables.update(tablekeyvaluepair)
        model = GaussianCopula(
                    primary_key=file["candidate_keys"][0]
                )
        
        # Use the FAST_ML preset to optimize for modeling time
        #model = TabularPreset(name='FAST_ML', metadata=metadata.get_table_meta(name))
        model.fit(df.compute())
        distributions = model.get_distributions()
        print(distributions)
        model.save(f'/data/datafiles/out/sdv/tabular_{file["name"]}.pkl')
        model.sample(num_rows=1000).reset_index(drop=True).to_csv(file['output_file_path'],index=False)



