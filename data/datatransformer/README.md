## Re-use our database connection logic
This is our own component to standarize the transformation of data in order to use the data in Synthentic Data Generators
### Usage


## Re-building the Wheel
If code changes, we need to be re-build this library to be usable within Databricks.

1. Install/upgrade wheel
`$ python3 -m pip install --upgrade build`

2. Build the wheel
`$ python3 -m build`

## Convert a JSON File
```python
from datatransformer.sourcefileservice import sourcefileservice
import datatransformer.datatypes

```