# etlzator
This library, implements, and abstract a way to construct ETL/ELT Pipelines

### Installation
```
pip install etlzator
```

### Extract
To create new Extract class you need to decorate the class with a builder and implements Extract default class
The Extract default class have two required methods: build_connection_string and execute

```Python
from pytterns.builder import Builder

@Builder
class NewExtract(Extract):
    def __init__(self):
        self.attribute_to_use = None
    
    
    def build_connection_string(self):
        # This method build's a connection string for your data origin
        pass

    def execute(self) -> Tuple[String, Any]:
        # This method call's the build connection and execute de process to consume the data
        return reference, df
```
The library has a fill implementations like:

## ExtractS3
This class consumes data from S3 and need to be fill with some attributes to compose the extract

```Python
from etlzator.extract import ExtractS3

extract = (ExtractS3()
            .plataform('The plataform to use to extract (now is only avaliable the Spark plataform)')
            .format('The data format (Avro, parquet, csv...)')
            .format_properties({'propertie1': 'A dict with the format properties'})
            .repository('The bucket repository on s3')
            .layer('the layer of the repository')
            .schema('the schema of the layer')
            .entity('the entity of the schema')
            .partition(['array', 'with', 'the partition of data'])
            .reference('df - the name of the dataframe is out')
            .columns(['column1', 'column2'])
            .build)

print(extract.execute())
print(extract.df)
```