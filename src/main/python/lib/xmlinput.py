import abc
import sys
from smv import *
from smv.smvdataset import SmvDataSet
from pyspark.sql.types import StructType
from core import *
import urllib
import tempfile

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

class SmvXmlInput(SmvDataSet, ABC):
    """SmvDataSet representing external XML input
    """

    def isEphemeral(self):
        return True

    def dsType(self):
        return "Input"

    def requiresDS(self):
        return []

    def run(self, df):
        """Post-processing for input data

            Args:
                df (DataFrame): input data

            Returns:
                (DataFrame): processed data
        """
        return df

    @abc.abstractproperty
    def path(self):
        """
        XML data file path (directory or file, could be gz files)
        """

    @abc.abstractproperty
    def schemaPath(self):
        """
        User defined schema file (JSON)
        """

    @abc.abstractproperty
    def rowTag(self):
        """
        XML tag to identify a row
        """

    def doRun(self, validator, known):
        # Read in schema
        import json
        with open (self.schemaPath(), "r") as sj:
            schema_st = sj.read()
        schema = StructType.fromJson(json.loads(schema_st))

        # Load XML
        df = self.smvApp.sqlContext\
            .read.format('com.databricks.spark.xml')\
            .options(rowTag=self.rowTag())\
            .load(self.path(), schema=schema)

        result = self.run(df)
        self.assert_result_is_dataframe(result)
        return result._jdf


class PubMedQuery(SmvXmlInput, ABC):

    @abc.abstractproperty
    def queryTerms(self):
        """
        """

    @abc.abstractproperty
    def relDays(self):
        """
        """

    def rowTag(self):
        return 'MedlineCitation'

    def searchAndFetch(self):
        searchResult = pubmedSearch(self.queryTerms(), self.relDays())
        return fetchURL(searchResult)

    def run(self, df):
        print self.searchAndFetch()
        return normalizeDf(df)

    def schemaPath(self):
        return 'lib/pubmed_mini_schema.json'

    def path(self):
        qres = urllib.urlopen(self.searchAndFetch()).read()
        g = tempfile.NamedTemporaryFile(delete=False)
        g.write(qres)
        return g.name
