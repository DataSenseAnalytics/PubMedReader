import abc
import sys
from smv import *
from smv.smvdataset import SmvDataSet
from pyspark.sql.types import StructType

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
