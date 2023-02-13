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
        metadata.add_table(

            name=input_data_file['name'],
            data=tables[input_data_file['name']],
            primary_key=input_data_file['candidate_keys'][0],
            parent=input_data_file['foreign_keys'][0]['reference_table_name'],
            foreign_key=input_data_file['foreign_keys'][0]['foreign_key']

        )



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


metadata.visualize()


# Or you can obtain this information in a machine-readable format by
# calling the `get_table_meta` method:

# In[7]:


metadata.get_table_meta('user')



# Now we can see how the table and the relationship have been registered:

# In[9]:


metadata


# ### Add a table specifying field properties
# 
# There are situations where the `Metadata` analysis is not able to figure
# out some data types or subtypes, or to deduce some properties of the
# field such as the datetime format.
# 
# In these situations, you can pass a dictionary with the exact metadata
# of those fields, which will overwrite the deductions from the analysis
# process.
# 
# In this next example, you will be adding a `transactions` table, which
# is related to the previous `sessions` table, and contains a `datetime`
# field which needs to have the datetime format specified.

# In[10]:


transactions_fields = {
    'timestamp': {
        'type': 'datetime',
        'format': '%Y-%m-%d'
    }
}

metadata.add_table(
    name='transactions',
    data=tables['transactions'],
    fields_metadata=transactions_fields,
    primary_key='transaction_id',
    parent='sessions'
)


# Let\'s see what our Metadata looks like right now:

# In[11]:


metadata
metadata.to_dict()


# In[12]:


metadata.visualize()


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


metadata.to_json('demo_metadata.json')


# You can see that the contents of the created file are very similar to
# the `dict` representation of the metadata:

# In[14]:


with open('demo_metadata.json') as meta_file:
    print(meta_file.read())


# After creating the JSON file, loading it back as a `metadata` object is
# as simple as passing it to the `Metadata` constructor:

# In[15]:


loaded = Metadata('demo_metadata.json')
loaded

