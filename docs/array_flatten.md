# Optimized way to flat an array of array

Assume the following data schema:

```
|-- KeywordList: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- Keyword: array (nullable = true)
|    |    |    |-- element: struct (containsNull = true)
|    |    |    |    |-- _MajorTopicYN: string (nullable = true)
|    |    |    |    |-- _VALUE: string (nullable = true)
```
One can't select `'KeywordList.Keyword._VALUE'`, since both `KeywordList` and
`Keyword` are `array` types. If do so, an `pyspark.sql.utils.AnalysisException`
exception will be raised. User need to flat the nested array by himself.

Assume we want to create a string field to capture the keyword values using
a pipe as separator between keywords.

There could be 3 approaches to flat the array of array:
* Python UDF
* Pure DF functions (explode)
* Scala UDF

## Python UDF

Code:
```python
u=F.udf(
        lambda aa: '|'.join([e['_VALUE'] for s in aa for e in s if e is not None]) if isinstance(aa, (list, list)) else None
    )

res = df.select(u(F.col('KeywordList.Keyword')).alias('k'), F.col('PMID._VALUE').alias('ID'), 'Article.ArticleTitle')
```

Please note that we have to keep some other fields to reflect the real use cases.

**Pros**: Simple to implement and easy to understand
**Cons**: Slower (12:42 min on our test)


## DF functions

Code:
```python
dt = df.select('KeywordList.Keyword').schema.fields[0].dataType # dataType of array(array(something))
# Have to convert null to array(array(null)), otherwise explode will drop that record
df0 = df.select(F.col('PMID._VALUE').alias('ID'), 'Article.ArticleTitle', F.coalesce(
    F.col('KeywordList.Keyword'), F.array(F.array(F.lit(None).cast(dt.elementType.elementType)).cast(dt.elementType)).cast(dt)).alias('k0'))
# Need to explode twice
df1 = df0.select('ID', 'ArticleTitle', F.explode('k0').alias('k1'))
df2 = df1.select('ID', 'ArticleTitle', F.explode('k1._VALUE').alias('k2'))
# Un-do the explode, and put the keywords together:
df3 = df2.groupBy('ID').agg(F.first('ArticleTitle').alias('ArticleTitle'), F.collect_list('k2').alias('keys')).select('ID', f.smvArrayCat('|', F.col('keys')).alias('kstr'))
return df3
```

To abstract the functions:
```python
def explode_keep_null(df, c):
    dt = df.select(c).schema.fields[0].dataType
    return df.withColumn(c, F.explode(F.coalesce(
            F.col(c), F.array(F.lit(None).cast(dt.elementType)).cast(dt))))

def flatten_array_of_array(df, keys, c, elem):
    df1 = explode_keep_null(df, c)
    df2 = explode_keep_null(df1, c).withColumn(c, F.col(c)[elem])
    kept = [i for i in df.columns if i not in (keys + [c])]
    res = df2.groupBy(*keys).agg(*([F.first(i).alias(i) for i in kept] + [F.collect_list(c).alias(c)]))
    return res

df0 = df.select(F.col('PMID._VALUE').alias('ID'), 'Article.ArticleTitle', 'KeywordList.Keyword')
df1 = flatten_array_of_array(df0, ['ID'], 'Keyword', '_VALUE')
res = df1.select('ID', 'ArticleTitle', f.smvArrayCat('|', F.col('Keyword')).alias('k'))
```

**pros**: faster 10:40 min
**cons**: hard to understand, and not generic (depends on existence of key cols)

## Scala UDF

Create a Scala UDF in SMV, then,
```python
def m4(df):
    df0 = df.select(F.col('PMID._VALUE').alias('ID'), 'Article.ArticleTitle', 'KeywordList.Keyword')
    df1 = df0.withColumn('Keyword', F.col('Keyword').smvArrayFlatten(df0))
    res = df1.select('ID', 'ArticleTitle', f.smvArrayCat('|', F.col('Keyword._VALUE')).alias('k'))
    return res
```

Also pretty fast: 10:47 min, and easy to follow.

# Conclusion

For array flatten, we implement the helper in SMV, so will use it going forward.
One finding is that the `explode` method of DF is actually pretty efficient!

The benchmark was performed on a single machine. May need to check on clusters
to see whether the `explode` method is still as efficient.
