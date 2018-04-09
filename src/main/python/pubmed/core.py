from smv import *
from smv.functions import *
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def _getCol(_df, path):
    """Return the column in `path` or return lit(None)"""
    try:
        _df[path]
        return _df[path]
    except AnalysisException:
        return F.lit(None)

def _getArrCat(_df, path):
    try:
        _df[path]
        # TODO: should have a better way to test whether is an array
        if ('ArrayType' in str(_df.select(path).schema)):
            return smvArrayCat('|', _df[path])
        else:
            return _df[path]
    except AnalysisException:
        return F.lit(None)

DataFrame.getCol = _getCol
DataFrame.getArrCat = _getArrCat

def readPubMedXml(path):
    """Read in PubMed XML file to df"""
    df = SmvApp.getInstance().sqlContext\
        .read.format('com.databricks.spark.xml')\
        .options(rowTag='MedlineCitation')\
        .load(path)

    res = df\
        .where(F.col('Article.AuthorList').isNotNull())\
        .where(df.getCol('KeywordList').isNotNull() | df.getCol('MeshHeadingList').isNotNull())

    return df

def pubMedCitation(path):
    df = readPubMedXml(path)
    return normalizeDf(df)

def normalizeDf(df):
    def applyMap(hashmap, key, default=''):
        """
        returns the map's value for the given key if present, and the default otherwise
        """
        def inner(k):
            if (k in hashmap):
                return hashmap[k]
            else:
                return default

        _fudf = F.udf(inner)
        return _fudf(key)

    seasonMap = {
        'Spring':'03',
        'Summer' :'06',
        'Autumn':'09',
        'Fall':'09',
        'Winter':'12'
    }

    monthMap = {
        'Jan':'01',
        'Feb':'02',
        'Mar':'03',
        'Apr':'04',
        'May':'05',
        'Jun':'06',
        'Jul':'07',
        'Aug':'08',
        'Sep':'09',
        'Oct':'10',
        'Nov':'11',
        'Dec':'12'
    }

    def getDate(d, prefix):
        return F.concat(
            F.coalesce(d.getCol(prefix + '.Year'), F.lit('')),
            F.lit('-'),
            F.coalesce(
                F.lpad(applyMap(monthMap, d.getCol(prefix + '.Month'), None), 2, '0'),
                applyMap(seasonMap, d.getCol(prefix + '.Season'), None), F.lit('01')
            ),
            F.lit('-'),
            F.coalesce(F.lpad(d.getCol(prefix + '.Day'), 2, '0'), F.lit('01'))
        ).cast('string')

    def getArticleDate(d):
        return F.when((d.getCol('Article.ArticleDate.Year').isNotNull()) & \
            (F.length(F.trim(d.getCol('Article.ArticleDate.Year'))) > 0),
            getDate(d, 'Article.ArticleDate')).otherwise(F.lit(None).cast('string'))

    articleDate = getArticleDate(df)

    journalDate = getDate(df, 'Article.Journal.JournalIssue.PubDate')

    res = df.select(
        F.concat(F.col('PMID._VALUE'), F.lit('_'), F.col('PMID._VERSION')).alias('PMID'),
        F.concat(F.col('Article.Journal.ISSN._IssnType'), F.lit('_'), F.col('Article.Journal.ISSN._VALUE')).alias('Journal_ISSN'),
        F.col('Article.ArticleTitle').alias('Article_Title'),
        F.col('Article.Journal.Title').alias('Journal_Title'),
        F.coalesce(articleDate, journalDate).alias('Journal_Publish_Date'),
        F.col('MedlineJournalInfo.Country').alias('Journal_Country'),
        F.coalesce(df.getArrCat('MeshHeadingList.MeshHeading.DescriptorName._UI'),
            df.getArrCat('MeshHeadingList.MeshHeading.DescriptorName._VALUE')).alias('Mesh_Headings'),
        F.explode('Article.AuthorList.Author').alias('Authors')
    ).where(F.col('Authors').isNotNull())

# The following info are not required. Might consider to add back if needed in the future
#        ListCol(df, 'Article.GrantList.Grant', '.GrantID', ['.GrantID']).alias('Grant_Ids'),
#        ListCol(df, 'ChemicalList.Chemical', '.NameOfSubstance._UI', ['.RegistryNumber', '.NameOfSubstance._UI']).alias('Chemicals'),

    return res.smvSelectPlus(
        res.getArrCat('Authors.AffiliationInfo.Affiliation').alias('Affiliation'),
        F.concat(
            res.getCol('Authors.Identifier.attr_Source'),
            F.lit('_'),
            res.getCol('Authors.Identifier._VALUE')
        ).cast('string').alias('Author_Identifier'),
        *[res.getCol('Authors.' + f).alias(f) for f in ['LastName', 'ForeName', 'Suffix', 'Initials']]
    ).drop('Authors')
