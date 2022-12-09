@service @dt_service
Feature: Source File Reader
  As an application developer,
  I want to read and interpret my source files in order to prepare for data transformation.

  Scenario Outline: I want to read and interpret a JSON file
    Given I have testfile "sourcefile_001.json" as a source file
    When I read and transform the source file
    Then I expect the JSON data to look like testfile "validationfile_001.json"

