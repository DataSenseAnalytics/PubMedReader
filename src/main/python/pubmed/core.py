from smv import *
from smv.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException


def readPubMedXml(path):
    """Read in PubMed XML file to df"""
    df = SmvApp.getInstance().sqlContext\
        .read.format('com.databricks.spark.xml')\
        .options(rowTag='MedlineCitation')\
        .load(path)\
        .where(F.col('Article.AuthorList').isNotNull())\
        .where(F.col('KeywordList').isNotNull() or F.col('MeshHeadingList').isNotNull())

    return df

def pubMedCitation(path):
    df = readPubMedXml(path)
    return normalizeDf(df)

def normalizeDf(df):
    def exists(df, col):
        try:
            df[col]
            return True
        except AnalysisException:
            return False

    def getCol(df, path):
        if (exists(df, path)):
            return col(path)
        else:
            return lit(None)

    def getFields(df, path):
        if (path == ''):
            return df.columns
        else:
            return [f.name for f in df.select(path).schema.fields[0].dataType.fields]

    def arrCat(col):
        def _cat(c):
            if c is None or reduce(lambda x, y: x and y, [s is None for s in c]):
                return None
            else:
                a = [s for s in c if s is not None]
                return reduce(lambda x, y: x + '|' + y, a)
        return udf(_cat)(col)

    def ListCol(df, path, fieldOp1, fieldOp2):
        if (exists(df, path)):
            if ('ArrayType' in str(df.select(path).schema)):
                return arrCat(col(path + fieldOp1))
            else:
                return concat(*[col(path + f) for f in fieldOp2])
        else:
            return lit(None).cast('string')

    def applyMap(hashmap, key, default=''):
        """
        returns the map's value for the given key if present, and the default otherwise
        """
        def inner(k):
            if (k in hashmap):
                return hashmap[k]
            else:
                return default

        _fudf = udf(inner)
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
        return concat(
            coalesce(getCol(d, prefix + '.Year'), lit('')),
            lit('-'),
            coalesce(
                lpad(applyMap(monthMap, getCol(d, prefix + '.Month'), None), 2, '0'),
                applyMap(seasonMap, getCol(d, prefix + '.Season'), None), lit('01')
            ),
            lit('-'),
            coalesce(lpad(getCol(d, prefix + '.Day'), 2, '0'), lit('01'))
        ).cast('string')

    def getArticleDate(d):
        if exists(d, 'Article.ArticleDate'):
            return when((getCol(df, 'Article.ArticleDate.Year').isNotNull()) & (length(trim(getCol(df, 'Article.ArticleDate.Year'))) > 0), getDate(d, 'Article.ArticleDate')).otherwise(lit(None).cast('string'))
        else:
            return lit(None).cast('string')

    articleDate = getArticleDate(df)

    journalDate = getDate(df, 'Article.Journal.JournalIssue.PubDate')

    res = df.select(
        concat(col('PMID._VALUE'), lit('_'), col('PMID._VERSION')).alias('PMID'),
        concat(col('Article.Journal.ISSN._IssnType'), lit('_'), col('Article.Journal.ISSN._VALUE')).alias('Journal_ISSN'),
        col('Article.ArticleTitle').alias('Article_Title'),
        col('Article.Journal.Title').alias('Journal_Title'),
        coalesce(articleDate, journalDate).alias('Journal_Publish_Date'),
        col('MedlineJournalInfo.Country').alias('Journal_Country'),
        ListCol(df, 'Article.GrantList.Grant', '.GrantID', ['.GrantID']).alias('Grant_Ids'),
        ListCol(df, 'MeshHeadingList.MeshHeading', '.DescriptorName._UI', ['.DescriptorName._VALUE']).alias('Mesh_Headings'),
        ListCol(df, 'ChemicalList.Chemical', '.NameOfSubstance._UI', ['.RegistryNumber', '.NameOfSubstance._UI']).alias('Chemicals'),
        explode('Article.AuthorList.Author').alias('Authors')
    ).where(col('Authors').isNotNull())

    authorInfo = [col('Authors.' + f) for f in list(set(getFields(res, 'Authors')) - set(['Identifier', 'AffiliationInfo', 'CollectiveName']))]

    return res.smvSelectPlus(
        ListCol(res, 'Authors.AffiliationInfo.Affiliation', '', ['']).alias('Affiliation'),
        concat(getCol(df, 'Authors.Identifier.attr_Source'), lit('_'), getCol(df, 'Authors.Identifier._VALUE')).cast('string').alias('Author_Identifier'),
        *authorInfo
    ).drop('Authors')
