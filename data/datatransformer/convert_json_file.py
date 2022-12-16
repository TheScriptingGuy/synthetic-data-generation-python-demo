import sys
sys.path.append("src")

from datatransformer.sourcefileservice import sourcefileservice
import datatransformer.datatypes
import json

sourceTestFilesPath = "/data/datafiles/yelp_academic_dataset_user.json"
targetTestFilesPath = "/data/datafiles/yelp_academic_dataset_review_modified.json"

sourceFilePath = sourceTestFilesPath

dt_service = sourcefileservice(sourceFilePath, datatransformer.datatypes.JSON, ["categories"], destinationFileType= datatransformer.datatypes.CSV)
dt_service.transformSourceData(flattenSourceData=True)
