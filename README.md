# Jupyter notebook with pyspark

## instructions
Execute those steps in your terminal
1) ```docker pull jupyter/pyspark-notebook```
2) ```docker run -p 8888:8888 -p 4040:4040 -v ~/projects/jupyter:/home/jupyter/notebooks --name spark jupyter/pyspark-notebook```

for Windows change ```~/projects/jupyter``` to your path like ```C:\projects\jupyter```


## test example

```
from pyspark.sql import SparkSession
spark = SparkSession.\
        builder.\
        appName('abc').\
        getOrCreate()
```