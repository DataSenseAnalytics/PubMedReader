# Learning for reading in XML formatted files

## Tool

DataBricks' spark-xml library, https://github.com/databricks/spark-xml, is used
to read in XML files to spark DF.

### Client code
```python
df = SmvApp.getInstance().sqlContext\
    .read.format('com.databricks.spark.xml')\
    .options(rowTag='MedlineCitation')\
    .load(path, schema = schema)
```

### Command line
```
$ smv-run -m TestPubMedCitation2 -- --master local[*] --jars lib/spark-xml_2.10-0.4.1.jar
```

## Schema inferring and optimization on loading

Since XML is schema-less, spark's DF schema need to be inferred, not only on
simple data types, such as String vs. Integer, but also the complicated
structures, such as Array[Struct[String]] vs. Array[Struct[Array[String]]].

Also some records may not have all the elements other records may have, so
inferring from a small subset of data might be inaccurate.

All the complexity of dealing with different data schema while standardize the
PubMed data caused be the inaccurate (or inconsisted) schema inferring from
small PubMed XML files.

To `help` users to use PubMed data, the publisher partitioned the entire
database to small XML files. While the `spark-xml` tool infer schema based
on each individual file, and create DF based on the inferred schema. Without
get this schema inconsistency issue fixed, the original PubMed reader code
tried to handle it in the code.

Another issue of inferring schema is that the data have to pass through the
reader twice, with the first time infer schema, and the second time really
load the DF. For PubMed, the first pass takes significantly longer time.

Since the reader library support user specified schema, we should alway use
a predefined user specified schema, instead of let the reader to inner. It
solves both the inconsistency problem and the performance issue.

The best practice should be:

- Infer the schema on a relative large random sample of the XML records
- Print the inferred schema to a file
- Load the pre-calculated schema when use the reader to load DF

### Create the Schema
#### Load data with sample rate:

```python
a = sqlContext\
    .read.format('com.databricks.spark.xml')\
    .options(rowTag='MedlineCitation', samplingRatio=0.05)\
    .load('/data/pdda_raw/17_pubmed/all.xml.gz')
```

Above statement will perform the first pass of the data to infer schema with
sample rate 5%. Since I combined the full PubMed data to a single data file,
this step took multiple hours to finish.

#### Persist the schema to a file
Save the JSON string:

```python
import json
o=file('pubmed_all_schema.json', 'w+')
sch_str = json.dumps(a.schema.jsonValue(), indent=2, separators=(',', ': '), sort_keys=True)
o.write(sch_str)
o.close()
```

Above JSON file is already readable. However the tree view of the schema is more
human-friendly. Let's store a tree view version:
```python
o=file('pubmed_all_schema_tree.txt', 'w+')
sch_str = a._jdf.schema().treeString()
o.write(sch_str)
o.close()
```

#### Load the schema back
```python
import json
with open ("lib/pubmed_all_schema.json", "r") as sj:
    schema_st = sj.read()
schema = StructType.fromJson(json.loads(schema_st))
```

When read the schema back in, it can be used in the reader API to load the DF.

### Optimization

Simply by specifying the schema file, the run time on converting the entire
PubMed dropped from a day to about 2 hours.

Since not all fields are needed for downstream, a further optimization can be
done by simplify the predefined schema. One can simply removed the schema
element (the StructField) from whichever level of the schema JSON as long as
that element will not be used in the code.

With the minimal schema file, we further improved the run time to about 1.5
hours.
