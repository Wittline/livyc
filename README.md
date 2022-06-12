# livyc
## Apache Livy Client


<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/173258806-a1d55121-1d05-4ed3-9c6b-3b31d9b61f82.png"
  >
</p>


## Install library
```python
pip install livyc
```

## Import library
```python
from livyc import livyc
```

## Setting livy configuration 
```python
data_livy = {
    "livy_server_url": "localhost",
    "port": "8998",
    "jars": ["org.postgresql:postgresql:42.3.1"]
}
```

## Let's try launch a pySpark script to Apache Livy Server

```python
params = {"host": "localhost", "port":"5432", "database": "db", "table":"staging", "user": "postgres", "password": "pg12345"}
```

```python
pyspark_script = """

    from pyspark.sql.functions import udf, col, explode
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
    from pyspark.sql import Row
    from pyspark.sql import SparkSession


    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://{host}:{port}/{database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "{table}") \
        .option("user", "{user}") \
        .option("password", "{password}") \
        .load()
        
    n_rows = df.count()

    spark.stop()
"""
```

#### Creating an livyc Object
```python
lvy = livyc.LivyC(data_livy)
```

#### Creating a new session to Apache Livy Server
```python
session = lvy.create_session()
```

#### Send and execute script in the Apache Livy server
```python
lvy.run_script(session, pyspark_script.format(**params))
```

#### Accesing to the variable "n_rows" available in the session
```python
lvy.read_variable(session, "n_rows")
```

## Contributing and Feedback
Any ideas or feedback about this repository?. Help me to improve it.

## Authors
- Created by <a href="https://twitter.com/RamsesCoraspe"><strong>Ramses Alexander Coraspe Valdez</strong></a>
- Created on 2022

## License
This project is licensed under the terms of the MIT License.
