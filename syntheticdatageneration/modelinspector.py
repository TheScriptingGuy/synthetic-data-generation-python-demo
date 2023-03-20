
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
 

def hvHeatmap (df, title):
   """
    Creates a masked hvplot heatmap

            Parameters:
               df : dataframe
                  the dataframe containing the data
               title : str
                  the title of the heatmap

            Returns:
                    heatmap: hvplot HeatMap
   """
   #calculate a pearson coefficient
   corr = df.corr(method="pearson").compute().abs()
   corr.values[np.triu_indices_from(corr, 0)] = np.nan

   heatmap = hv.HeatMap((corr.columns, corr.index, corr))\
         .opts(tools=['hover'],  height=400, width=400, fontsize=9,
               toolbar='above', colorbar=False, cmap='Blues',
               invert_yaxis=True, xrotation=90, xlabel='', ylabel='',
               title=title)
   return heatmap

listOfOriginalDfs = {}
listOfSyntheticDfs = {}

def processCSVFiles(input_data_file):
      original_datafile_path= input_data_file['input_file_path']
      synthetic_datafile_path = input_data_file['output_file_path']

      analyse_columns = []
      if 'analyse_columns' in input_data_file:
         analyse_columns = input_data_file['analyse_columns']

      originalDfOut: dd
      syntheticDfOut: dd

      #Read into Dask Dataframes
      originalDf: dd = dd.read_csv(urlpath=original_datafile_path,sep=",")
      syntheticDf: dd = dd.read_csv(urlpath=synthetic_datafile_path,sep=",")

      listOfOriginalDfs.update({f"{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_filtered":originalDf})
      listOfSyntheticDfs.update({f"{os.path.basename(input_data_file['input_file_path']).split('.')[0]}_synthentic":syntheticDf})
      originalDf_joined = None
      syntheticDf_joined = None
      if 'foreign_keys' in input_data_file:
         if len(input_data_file['foreign_keys'])> 0:
            for foreign_key in input_data_file['foreign_keys']:
               originalDf_joined = dd.multi.merge(left=originalDf
                              ,right=listOfOriginalDfs.get(f"{os.path.basename(foreign_key['reference_file']).split('.')[0]}_filtered")
                              ,how='left'
                              ,left_on=f"{os.path.basename(foreign_key['reference_key'])}"
                              ,right_on=f"{os.path.basename(foreign_key['foreign_key'])}")
               
               syntheticDf_joined = dd.multi.merge(left=syntheticDf
                              ,right=listOfSyntheticDfs[f"{os.path.basename(foreign_key['reference_file']).split('.')[0]}_synthentic"]
                              ,how='left'
                              ,left_on=f"{os.path.basename(foreign_key['reference_key'])}"
                              ,right_on=f"{os.path.basename(foreign_key['foreign_key'])}")

      st.write(f"## datafile: {os.path.basename(original_datafile_path)}")
      col1, col2 = st.columns(2, gap="medium")
      col1.header("Original")
      col2.header("Synthetic")
      #Iterate through all attributes
      labels = []
      for label, content in originalDf.items():               
         if(label not in input_data_file['candidate_keys']):
            #if a set of analyse_columns is provided, only select these columns for the histograms. Otherwise, select all.
            if(label in analyse_columns or not analyse_columns):
               labels.append(label)
      for label in labels: 

            #construct histogram
            attributeSeries_Org: dd.Series  = originalDf[label].value_counts(sort=False)
            attributeSeries_Synth: dd.Series = syntheticDf[label].value_counts(sort=False)
            attrOriginalDf = attributeSeries_Org.to_frame().reset_index().rename(columns={label:"count","index":label}).assign(type="original")
            attrSyntheticDf = attributeSeries_Synth.to_frame().reset_index().rename(columns={label:"count","index":label}).assign(type="synthetic")  
            unionDf = dd.concat([attrOriginalDf, attrSyntheticDf]).sort_values(by="count", ascending=False)       

            plot1= attrOriginalDf.sort_values(by="count", ascending=False).head(10).hvplot.bar(y="count",x=label,stacked=False,rot=60, width=1000).opts(title=f"Histogram by {label}")
            plot2= attrSyntheticDf.sort_values(by="count", ascending=False).head(10).hvplot.bar(y="count",x=label,stacked=False,rot=60).opts(title=f"Histogram by {label}")
            
            #cache the list of top 10 attributes of the original dataset
            collist = list(attrOriginalDf.sort_values(by="count", ascending=False).head(10).iloc[:,0])
            col1.bar_chart(attrOriginalDf.sort_values(by="count", ascending=False).head(10), x=label, y="count")         
            #select the synthetic attributes that match the top 10 original attributes
            col2.bar_chart(attrSyntheticDf[attrSyntheticDf[label].isin(collist)].sort_values(by="count", ascending=False).head(10), x=label, y="count")

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
      
      #generate heatmaps
      heatmap_org = hvHeatmap(originalDfOut, 'Heatmap original')
      heatmap_synth = hvHeatmap(syntheticDfOut, 'Heatmap synthethic')
      if originalDf_joined is not None:
         heatmap_org_joined = hvHeatmap(originalDf_joined, f"Heatmap original joined to {os.path.basename(foreign_key['reference_file']).split('.')[0]}_filtered.csv")
         col1, col2 = st.columns(2)
         col1.write(hv.render(heatmap_org_joined, backend='bokeh'))
      if syntheticDf_joined is not None:
         heatmap_synth_joined = hvHeatmap(syntheticDf_joined, f"Heatmap synth joined to {os.path.basename(foreign_key['reference_file']).split('.')[0]}_synthentic.csv")
         col2.write(hv.render(heatmap_synth_joined, backend='bokeh'))
      
      col1.write(hv.render(heatmap_org, backend='bokeh'))
      col2.write(hv.render(heatmap_synth, backend='bokeh'))


option = st.sidebar.selectbox('Select output', ['faker','datasynthesizer','syntheticdatavault','trumania'])
json_path = f"/data/datafiles/{option}.json"


try:
   f = open(json_path,)
   input_data_files = json.load(f)
   f.close()
         
   st.write('Selected output:', option)
except:
   st.write ('Could not find selected file:', json_path)

if input_data_files is not None:
   for file in input_data_files:
      with st.spinner('Files are loading...'):
         processCSVFiles(file)





