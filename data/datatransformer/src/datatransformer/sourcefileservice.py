#from datatypes import dataTypes
import os
import json
from .datatypes import datatypes
import pandas as pd
import ast
import io


# Manage the connection to the database
# Call 
class sourcefileservice:
    sourceFilePath: str
    sourceDataType: datatypes
    _sourceDataCache: str

    def __init__(self
                ,sourceFilePath
                ,sourceDataType
                ,strSplitColumns = []
                ,flattenSourceData = True
                ,readerChunksize = 100000
                ,destinationFilePath = None
                ,destinationFileType = datatypes.CSV
                ):
        self.sourceFilePath = sourceFilePath
        self.sourceDataType = sourceDataType
        self.flattenSourceData = flattenSourceData
        self.strSplitColumns = strSplitColumns
        self.readerChunkSize = readerChunksize
        self.destinationFileType = destinationFileType
        self.destinationFilePath = f"{os.path.dirname(self.sourceFilePath)}/{os.path.basename(self.sourceFilePath).split('.')[0]}_transformed.{destinationFileType.name}" if destinationFilePath is None else destinationFilePath
       
        #remove destination file if exists
        os.remove(self.destinationFilePath) if os.path.exists(self.destinationFilePath) else None
    def _writeToDestination(self, chunk: pd.DataFrame):
        match self.destinationFileType:
            case datatypes.JSON:
                buffer = io.BytesIO()
                chunk.to_json(buffer,orient='records',lines=True)
                with open(self.destinationFilePath, "ab") as file:
                    file.write(buffer.getbuffer())
                    file.close()
            case datatypes.PARQUET:
                #create parquet if not exists
                if not os.path.isfile(self.destinationFilePath):
                    chunk.to_parquet(self.destinationFilePath, engine='fastparquet', index=False)
                #append data if parquet exists
                else:
                    chunk.to_parquet(self.destinationFilePath, engine='fastparquet', append=True, index=False)
            case datatypes.CSV:
                chunk.to_csv(path_or_buf=self.destinationFilePath,sep=';',mode="a", index=False)
    def transformSourceData(self):
 
        if(self.sourceDataType==datatypes.JSON):
            count = 0
            #Divide pandas in different chunks in order to manage big files
            with pd.read_json(self.sourceFilePath,orient='records', lines=True, chunksize=self.readerChunkSize) as reader:
                for chunk in reader:
                    #remove Pandas Index
                    if(self.flattenSourceData):
                        chunk = self.convertNestedJsonStrings(chunk)
                        
                    self._writeToDestination(chunk)
                    count = count + self.readerChunkSize
                    print(f"# {count} records written to {self.destinationFilePath}")

    def convertNestedJsonStrings(self,chunk):
        jsonBlob = chunk.to_json(orient="records")
        arrayOfJsonRecords= json.loads(jsonBlob)
        for i, JsonRecord in enumerate(arrayOfJsonRecords):
            for key, value in JsonRecord.items():
                #try parsing Json Like objects as string to dictionaries
                if(isinstance(value, dict)):
                    for nestedattribute, nestedvalue in value.items():
                        try:
                            nestedvalue = ast.literal_eval(nestedvalue)
                            arrayOfJsonRecords[i][key][nestedattribute] = nestedvalue
                            
                        except:
                            pass
                #try converting string split columns to list type
                if(isinstance(value, str) and "," in value and key in self.strSplitColumns):
                    arrayOfJsonRecords[i][key] = value.split(", ")
        return chunk.from_dict(arrayOfJsonRecords)

    @property
    def sourceFilePath(self):
        return self._sourceFilePath
    @property
    def sourceDataType(self):
        return self._sourceDataType
    @property
    def strSplitColumns(self):
        return self._strSplitColumns
    @property
    def flattenSourceData(self):
        return self._flattenSourceData
    @property
    def destinationFilePath(self):
        return self._destinationFilePath
    @property
    def readerChunkSize(self):
        return self._readerChunkSize
    @sourceFilePath.setter
    def sourceFilePath(self, value):
        self._sourceFilePath = value
    @sourceDataType.setter
    def sourceDataType(self, value):
        self._sourceDataType = value
    @strSplitColumns.setter
    def strSplitColumns(self, value):
        self._strSplitColumns = value
    @destinationFilePath.setter
    def destinationFilePath(self, value):
        self._destinationFilePath = value
    @readerChunkSize.setter
    def readerChunkSize(self, value):
        self._readerChunkSize = value
    @flattenSourceData.setter
    def flattenSourceData(self, value):
        self._flattenSourceData = value