from dataset_compare import ModelInspector, read_json_file
import dask.dataframe as dd
from dask.distributed import Client
from DataSynthesizer.lib.utils import pairwise_attributes_mutual_information
import numpy as np
import holoviews as hv
import hvplot.pandas  # noqa
import hvplot.dask  # noqa
import pandas as pd
hvplot.extension('bokeh')

#client = Client(n_workers=4)
#client
#inspector: ModelInspector = ModelInspector(filteredDf, syntheticDf, attribute_description)

filteredDf: dd = dd.read_csv(urlpath="/data/datafiles/yelp_academic_dataset_business_transformed_filtered.CSV",sep=",", dtype={'categories10': 'object',
       'categories11': 'object',
       'categories12': 'object',
       'categories13': 'object',
       'categories14': 'object',
       'categories15': 'object',
       'categories16': 'object',
       'categories17': 'object',
       'categories18': 'object',
       'categories19': 'object',
       'categories20': 'object',
       'categories21': 'object',
       'categories22': 'object',
       'categories7': 'object',
       'categories8': 'object',
       'categories9': 'object'})



syntheticDf: dd = dd.read_csv(urlpath="/data/datafiles/out/independent_attribute_mode/yelp_academic_dataset_business_transformed_synthentic.csv"
                                                ,sep=",")


attribute_description = read_json_file("/data/datafiles/out/independent_attribute_mode/yelp_academic_dataset_business_transformed_description.json")

series2: dd.Series  = filteredDf['city'].value_counts(sort=False)
series: dd.Series = syntheticDf['city'].value_counts(sort=False)


syntheticDf1 = series.to_frame().reset_index().rename(columns={"city":"count","index":"city"})
filteredDf1 = series2.to_frame().reset_index().rename(columns={"city":"count","index":"city"})

syntheticDf1 = syntheticDf1.assign(type="synthetic")  
filteredDf1 = filteredDf1.assign(type="original")

unionDf = dd.concat([syntheticDf1, filteredDf1]).sort_values(by="count", ascending=False)       

#unionDf.sort_values(by="city", ascending=False).compute()

#syntheticDf.sort_values(by="city").compute()
#filteredDf.sort_values(by="city").compute()

#syntheticDf.join(filteredDf, lsuffix='_synthetic', rsuffix='_original')  

niceplot1= unionDf.head(10).hvplot.bar(y="count",x="city", by="type",stacked=False, width=1200)

#pairwise_attributes_mutual_information(syntheticDf.compute())
#pairwise_attributes_mutual_information(filteredDf.compute())

#syntheticDf.map_partitions(lambda df: pairwise_attributes_mutual_information(df)).compute()
#filteredDf.map_partitions(lambda df: pairwise_attributes_mutual_information(df)).compute()

hv.render(niceplot1)
import streamlit as st
from holoviews import dim, opts
st.write(hv.render(niceplot1, backend='bokeh'))
#st.write(hv.render(niceplot2, backend='bokeh'))

#opts.Histogram()
#(niceplot1 + niceplot2).opts()
#(niceplot1 + niceplot2).opts(
#    opts.Points(tools=['hover'], size=5), opts.HeatMap(tools=['hover']),
#    opts.Image(tools=['hover']), opts.Histogram(tools=['hover']),
#    opts.Layout(shared_axes=False)).cols(2)df['profession']=df['profession'].astype('category').cat.codes

for label, content in filteredDf.items():  
    if 'filteredDfOut' in locals():     
       filteredDfOut = filteredDfOut.astype({label: 'category'}).categorize(columns=[label])
    else:
       filteredDfOut = filteredDf.astype({label: 'category'}).categorize(columns=[label])
    filteredDfOut[label]=filteredDfOut[label].cat.codes

heatmapDf = filteredDfOut.corr(method="pearson").compute()

heatmap = heatmapDf.hvplot.heatmap(width=1200)
st.write(hv.render(heatmap, backend='bokeh'))