#!/usr/bin/env python
# coding: utf-8

# Relational Metadata
# ===================
# 
# In order to work with complex dataset structures you will need to pass
# additional information about your data to SDV using `Metadata`.
# 
# Let\'s go over an example to see how to use it.
# 
# %%
import dask.dataframe as dd
import json

from sdv import Metadata

# Let us now see how to build a `Metadata` object that represents this
# dataset.
# 
# The Metadata class
# ------------------
# 
# In SDV, the structure of a dataset is represented using the class
# `sdv.Metadata`, which contains all the information that SDV needs in
# order to optimally learn the dataset and generate synthetic versions of
# it.
# 
# In order to create a `Metadata` for our dataset, you will first need to
# import the class and create an empty instance:

# %%
metadata = Metadata()
metadata

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
        i:int = 1
        for foreign_key in input_data_file['foreign_keys']:
            if i == 1:
                metadata.add_table(

                    name=input_data_file['name'],
                    data=tables[input_data_file['name']],
                    primary_key=input_data_file['candidate_keys'][0],
                    parent=foreign_key['reference_table_name'],
                    foreign_key=foreign_key['foreign_key']

                )
            if i > 1:
                metadata.add_relationship(
                    parent=foreign_key['reference_table_name'],
                    child=input_data_file['name'],
                    foreign_key=foreign_key['foreign_key']
                )
            i = i + 1
        

        



# Load the metadata file and go through every json record

# In[1]:

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
        
        tablekeyvaluepair = {file["name"]:df.compute()}
        tables.update(tablekeyvaluepair)
        add_sdv_metadata(file)


# The output of this function will be a dict that contains several tables
# as `pandas.DataFrames`.

# In[2]:


print(tables.keys())



# At this point, our metadata only contains one table and, of course, no
# relationships:

# In[5]:


metadata


# However, the `Metadata` instance will have already analyzed all the
# columns in the passed table and identified the different data types and
# subtypes, and will properly indicate that the `user_id` column is the
# table primary key.
# 
# You can see so by calling the `visualize` method of your `metadata`
# instance:

# In[6]:


#metadata.visualize()


# Or you can obtain this information in a machine-readable format by
# calling the `get_table_meta` method:

# In[7]:


metadata.get_table_meta('business')



# Now we can see how the table and the relationship have been registered:

# In[9]:


metadata



# Metadata JSON format
# --------------------
# 
# The `Metadata` objects can also be saved as a JSON file and later on
# loaded from them.
# 
# In order to save the current `metadata` as a JSON file, all you need to
# do is call the `to_json` method passing the path to the JSON file that
# you want to create.

# In[13]:

from pathlib import Path

Path('/data/datafiles/out/sdv/sdv_metadata.json').touch()
metadata.to_json('/data/datafiles/out/sdv/sdv_metadata.json')


# You can see that the contents of the created file are very similar to
# the `dict` representation of the metadata:

# In[14]:


with open('/data/datafiles/out/sdv/sdv_metadata.json') as meta_file:
    print(meta_file.read())


# After creating the JSON file, loading it back as a `metadata` object is
# as simple as passing it to the `Metadata` constructor:

# In[15]:


metadata = Metadata('/data/datafiles/out/sdv/sdv_metadata_persisted.json')
metadata
# For more details about how to build the `Metadata` for your own dataset,
# please refer to the [relational_metadata](relational_metadata.ipynb)
# Guide.
# 
# 2.  A dictionary containing three `pandas.DataFrames` with the tables
#     described in the metadata object.

# %%

from sdv.utils import display_tables

display_tables(tables)

# %%

for name, table in tables.items():
    print(name, table.shape)

# Let us now use the `HMA1` class to learn this data to be ready to sample
# synthetic data about new users. In order to do this you will need to:
# 
# -   Import the `sdv.relational.HMA1` class and create an instance of it
#     passing the `metadata` that we just loaded.
# -   Call its `fit` method passing the `tables` dict.

# %%

from sdv.relational import HMA1

model = HMA1(metadata)

#model = HMA1(metadata)
model.fit(tables)


# <div class="alert alert-info">
# 
# **Note**
# 
# During the previous steps SDV walked through all the tables in the
# dataset following the relationships specified by the metadata, learned
# each table using a [gaussian_copula](gaussian_copula.ipynb) and
# then augmented the parent tables using the copula parameters before
# learning them. By doing this, each copula model was able to learn how
# the child table rows were related to their parent tables.
# 
# </div>


# ### Save and Load the model
# 
# In many scenarios it will be convenient to generate synthetic versions
# of your data directly in systems that do not have access to the original
# data source. For example, if you may want to generate testing data on
# the fly inside a testing environment that does not have access to your
# production database. In these scenarios, fitting the model with real
# data every time that you need to generate new data is feasible, so you
# will need to fit a model in your production environment, save the fitted
# model into a file, send this file to the testing environment and then
# load it there to be able to `sample` from it.
# 
# Let's see how this process works.
# 
# #### Save and share the model
# 
# Once you have fitted the model, all you need to do is call its `save`
# method passing the name of the file in which you want to save the model.
# Note that the extension of the filename is not relevant, but we will be
# using the `.pkl` extension to highlight that the serialization protocol
# used is [cloudpickle](https://github.com/cloudpipe/cloudpickle).

# In[10]:


model.save('my_model.pkl')


# This will have created a file called `my_model.pkl` in the same
# directory in which you are running SDV.
# 
# <div class="alert alert-info">
# 
# **Important**
# 
# If you inspect the generated file you will notice that its size is much
# smaller than the size of the data that you used to generate it. This is
# because the serialized model contains **no information about the
# original data**, other than the parameters it needs to generate
# synthetic versions of it. This means that you can safely share this
# `my_model.pkl` file without the risk of disclosing any of your real
# data!
# 
# </div>
# 
# #### Load the model and generate new data
# 
# The file you just generated can be sent over to the system where the
# synthetic data will be generated. Once it is there, you can load it
# using the `HMA1.load` method, and then you are ready to sample new data
# from the loaded instance:

# In[11]:


model = HMA1.load('my_model.pkl')


# <div class="alert alert-warning">
# 
# **Warning**
# 
# Notice that the system where the model is loaded needs to also have
# `sdv` installed, otherwise it will not be able to load the model and use
# it.
# 
# </div>
# 
# ### How to control the number of rows?
# 
# In the steps above we did not tell the model at any moment how many rows
# we wanted to sample, so it produced as many rows as there were in the
# original dataset.
# 
# If you want to produce a different number of rows you can pass it as the
# `num_rows` argument and it will produce the indicated number of rows:

# In[12]:


dictData = model.sample(num_rows=1000)

for input_data_file in input_data_files:
    dictData[input_data_file['name']].to_csv(input_data_file['output_file_path'],index=False)
