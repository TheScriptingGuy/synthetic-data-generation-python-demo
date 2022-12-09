"""
This module contains step definitions for sourcefile.feature.
It uses the requests package:
http://docs.python-requests.org/
"""
import pytest
import os

import json
from pytest_bdd import scenarios, given, when, then, parsers
from datatransformer.sourcefileservice import sourcefileservice
import datatransformer.datatypes

# Constants

sourceTestFilesPath = "src/tests/testfiles/pre/"
validationTestFilesPath = "src/tests/testfiles/post/"
# Scenarios

scenarios('../features/sourcefile.feature')


# Given Steps

@given(parsers.parse('I have testfile "{sourcefile}" as a source file'), target_fixture='dt_service')
def dt_service(sourcefile):
    sourceFilePath = sourceTestFilesPath + sourcefile
    return sourcefileservice(sourceFilePath, datatransformer.datatypes.JSON, ["categories"], destinationFileType=datatransformer.datatypes.JSON)

@when(parsers.parse('I read, transform and flatten the source file'))
def dt_readSourceFile(dt_service: sourcefileservice):
    dt_service.transformSourceData(flattenSourceData=True)

@when(parsers.parse("I read, transform and don't flatten the source file"))
def dt_readSourceFile(dt_service: sourcefileservice):
    dt_service.transformSourceData(flattenSourceData=False)

# Then Steps

@then(parsers.parse('I expect the JSON data to look like testfile "{validationfile}"'))
def dt_compareSourceFile(dt_service, validationfile):
    validationFilePath = validationTestFilesPath + validationfile
    f = open(validationFilePath,)
    validationContent = json.load(f)
    f.close()
    print(dt_service.destinationFilePath)
    f = open(dt_service.destinationFilePath,)
    destinationContent = json.load(f)
    f.close()
    assert validationContent == destinationContent

