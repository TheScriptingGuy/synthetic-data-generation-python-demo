# %%
from sdmetrics.reports.single_table import QualityReport
report = QualityReport()
import json
import dask.dataframe as dd
json_path = f"/data/datafiles/syntheticdatavault.json"
from sdv import Metadata
metadata = Metadata('/data/datafiles/out/sdv/sdv_metadata_2.json')


def readCSVFile(filepath):
      original_datafile_path= filepath
      
      originalDfOut: dd

      #Read into Dask Dataframes
      originalDf: dd = dd.read_csv(urlpath=original_datafile_path,sep=",")
      return originalDf


try:
   f = open(json_path,)
   input_data_files = json.load(f)
   f.close()
         
except:
   print('Could not find selected file:', json_path)
tables = {}
if input_data_files is not None:
   tables = {}
   for file in input_data_files:
        real_data_df = readCSVFile(file['input_file_path'])
        output_data_df = readCSVFile(file['output_file_path'])
        tablekeyvaluepair = {file["name"] + "_real":real_data_df.compute()}
        tables.update(tablekeyvaluepair)
        tablekeyvaluepair = {file["name"] + "_synth":output_data_df.compute()}
        tables.update(tablekeyvaluepair)
        tablekeyvaluepair = {file["name"] + "_meta":metadata.get_table_meta(file['name'])}
        tables.update(tablekeyvaluepair)
        

# %%
print(f"creating report on business")

report.generate(tables["business_real"], tables["business_synth"], tables["business_meta"])
report.get_details(property_name='Column Shapes')
report.get_visualization(property_name='Column Shapes')

from sdmetrics.reports.utils import get_column_plot


# %%

fig = get_column_plot(
    real_data=tables["business_real"],
    synthetic_data= tables["business_synth"],
    metadata=tables["business_meta"],
    column_name='city'
)

fig.show()

# %%

report.get_visualization(property_name='Column Pair Trends')

# %% 

from sdmetrics.reports.utils import get_column_pair_plot

fig = get_column_pair_plot(
    real_data=tables["business_real"],
    synthetic_data= tables["business_synth"],
    metadata=tables["business_meta"],
    column_names=['categories1', 'categories0']
)

fig.show()

# %%
# calculate whether the synthetic data respects the min/max bounds
# set by the real data
from sdmetrics.single_column import BoundaryAdherence

BoundaryAdherence.compute(
    tables["business_real"]['city'],
    tables["business_synth"]['city']
)
# %%

from sdmetrics.single_table import NewRowSynthesis

NewRowSynthesis.compute(tables["business_real"], tables["business_synth"], tables["business_meta"])

# %%

from sdmetrics.column_pairs import CorrelationSimilarity

CorrelationSimilarity.compute(
    real_data=tables["business_real"][['stars', 'review_count']],
    synthetic_data=tables["business_synth"][['stars', 'review_count']],
    coefficient='Pearson'
)
# %%

tables["business_synth"][['categories0', 'categories1']]
# %%
