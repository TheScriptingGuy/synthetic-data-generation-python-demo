from dataset_compare import ModelInspector, read_json_file
import dask.dataframe as dd

from DataSynthesizer.lib.utils import pairwise_attributes_mutual_information
import numpy as np
import holoviews as hv
import hvplot.pandas  # noqa
import hvplot.dask  # noqa
import pandas as pd
import json
import streamlit as st
import os
hvplot.extension('bokeh')

f = open('/data/datafiles/dataset_configuration.json',)

input_data_files = json.load(f)

f.close()
       

def processCSVFiles(input_data_file):
      original_datafile_path= f"/data/datafiles/{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_filtered.csv"
      synthetic_datafile_path = f"/data/datafiles/out/independent_attribute_mode/{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_synthentic.csv"

      originalDfOut: dd
      syntheticDfOut: dd

      #Read into Dask Dataframes
      originalDf: dd = dd.read_csv(urlpath=original_datafile_path,sep=",")
      syntheticDf: dd = dd.read_csv(urlpath=synthetic_datafile_path,sep=",")
      st.write(f"# datafile: {os.path.basename(original_datafile_path)}")
      #Iterate through all attributes
      labels = []
      for label, content in originalDf.items(): 
         if(label not in input_data_file['candidate_keys']):
            labels.append(label)
      for label in labels: 

            #construct histogram
            attributeSeries_Org: dd.Series  = originalDf[label].value_counts(sort=False)
            attributeSeries_Synth: dd.Series = syntheticDf[label].value_counts(sort=False)
            attrOriginalDf = attributeSeries_Org.to_frame().reset_index().rename(columns={label:"count","index":label}).assign(type="original")
            attrSyntheticDf = attributeSeries_Synth.to_frame().reset_index().rename(columns={label:"count","index":label}).assign(type="synthetic")  
            unionDf = dd.concat([attrOriginalDf, attrSyntheticDf]).sort_values(by="count", ascending=False)       
            plot= unionDf.head(10).hvplot.bar(y="count",x=label, by="type",stacked=False, width=1200).opts(title=f"Histogram by {label}")
            st.write(hv.render(plot, backend='bokeh'))

            #construct heatmap by categorizing attributes
            if 'originalDfOut' in locals():  
               originalDfOut = originalDfOut.astype({label: 'category'}).categorize(columns=[label])
            else:
               originalDfOut = originalDf.astype({label: 'category'}).categorize(columns=[label])
            originalDfOut[label]=originalDfOut[label].cat.codes
            
            if 'syntheticDfOut' in locals():  
               syntheticDfOut = syntheticDfOut.astype({label: 'category'}).categorize(columns=[label])
            else:
               syntheticDfOut = syntheticDf.astype({label: 'category'}).categorize(columns=[label])
            syntheticDfOut[label]=syntheticDfOut[label].cat.codes
      #Do a pearson coefficient
      heatmap_orgDf = originalDfOut.corr(method="pearson").compute()
      heatmap_org = heatmap_orgDf.hvplot.heatmap(width=1200).opts(title="Heatmap original")
      heatmap_synthDf = syntheticDfOut.corr(method="pearson").compute()
      heatmap_synth = heatmap_synthDf.hvplot.heatmap(width=1200).opts(title="Heatmap synthethic")
      st.write(hv.render(heatmap_org, backend='bokeh'))
      st.write(hv.render(heatmap_synth, backend='bokeh'))
         
processCSVFiles(input_data_files[0])

#data synthesizer methods for generating heatmap, is very slow.
#pairwise_attributes_mutual_information(syntheticDf.compute())
#pairwise_attributes_mutual_information(filteredDf.compute())






