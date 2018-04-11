from smv import *
from smv.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def _getCol(_df, path):
    """
    Return the column in `path` if exist or return lit(None)
    Caller need to do the casting of lit(None) if needed
    """
    try:
        _df.select(path)
        return _df[path]
    except AnalysisException:
        return F.lit(None)

def _getArrCat(_df, path):
    """
    Return the column in `path`:
        - if it is a simple array, return as string with pip separated
        - if it is a array of array, return as string with pip separated flatten array
        - else return itself

    Always return as StringType
    """
    try:
        _df.select(path)
    except AnalysisException:
        pre, base = path.rsplit('.', 1)
        try:
            _df.select(pre)
        except AnalysisException:
            return F.lit(None).cast('string')
        else:
            _udf = lambda c: "|".join([e[base] for s in c for e in s]) if isinstance(c, (list, list)) else None
            return F.udf(_udf)(_df[pre]).cast('string')
    else:
        _udf = lambda c: "|".join([e for e in c]) if isinstance(c, list) else c
        return F.udf(_udf)(_df[path]).cast('string')

DataFrame.getCol = _getCol
DataFrame.getArrCat = _getArrCat

def toAscii(_col):
    """Convert Unicode to ascii, ignore errors
    """
    def conv(u):
        if (isinstance(u, str) or isinstance(u, unicode) or isinstance(u, bytes)):
            return u.encode("ascii","ignore")
        else:
            return u
    return F.udf(conv)(_col)

def readPubMedXml(path):
    """Read in PubMed XML file to df"""
    df = SmvApp.getInstance().sqlContext\
        .read.format('com.databricks.spark.xml')\
        .options(rowTag='MedlineCitation')\
        .load(path)

    # Only keep records with author and nonnull keyword or mesh, otherwise will not
    # be useful anyhow
    res = df\
        .where(F.col('Article.AuthorList').isNotNull())\
        .where(df.getCol('KeywordList').isNotNull() | df.getCol('MeshHeadingList').isNotNull())

    return df

def normalizeDf(df):
    """
    Normalize pubmed df from deep XML/JSON structure to flat CSV type of structure
    """
    def getDate(d, prefix):
        seasonMap = smvCreateLookUp({
            'Spring':'03',
            'Summer' :'06',
            'Autumn':'09',
            'Fall':'09',
            'Winter':'12'
        }, None, StringType())

        monthMap = smvCreateLookUp({
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
        }, None, StringType())

        # TODO: https://www.nlm.nih.gov/bsd/licensee/elements_article_source.html
        return F.concat(
            F.coalesce(d.getCol(prefix + '.Year').cast('string'), F.lit('')),
            F.lit('-'),
            F.coalesce(
                monthMap(d.getCol(prefix + '.Month').cast('string')),
                seasonMap(d.getCol(prefix + '.Season').cast('string')),
                F.lit('01')
            ),
            F.lit('-'),
            F.coalesce(F.lpad(d.getCol(prefix + '.Day').cast('string'), 2, '0'), F.lit('01'))
        )

    def getArticleDate(d):
        return F.when((d.getCol('Article.ArticleDate.Year').isNotNull()) & \
            (F.length(F.trim(d.getCol('Article.ArticleDate.Year'))) > 0),
            getDate(d, 'Article.ArticleDate')).otherwise(F.lit(None).cast('string'))

    articleDate = getArticleDate(df)

    # According to https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#pubdate
    # Need to handle MedlineDate case
    journalDate = getDate(df, 'Article.Journal.JournalIssue.PubDate')

    # Abstract: see "18. <Abstract> and <AbstractText>" on https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html
    res = df.select(
        F.concat(F.col('PMID._VALUE'), F.lit('_'), F.col('PMID._VERSION')).alias('PMID'), # PubMed uniq id
        F.concat(F.col('Article.Journal.ISSN._IssnType'), F.lit('_'), F.col('Article.Journal.ISSN._VALUE')).alias('Journal_ISSN'), # ISSN (optional)
        F.col('Article.ArticleTitle').alias('Article_Title'),
        F.col('Article.Journal.Title').alias('Journal_Title'),
        F.coalesce(articleDate, journalDate).alias('Journal_Publish_Date'), # yyyy-MM-dd format
        F.col('MedlineJournalInfo.Country').alias('Journal_Country'),
        df.getArrCat('MeshHeadingList.MeshHeading.DescriptorName._UI').alias('Mesh_Headings'),
        df.getArrCat('KeywordList.Keyword._VALUE').alias('Keywords'),
        F.regexp_replace(
            F.coalesce(df.getArrCat('Article.Abstract.AbstractText._VALUE'),
                df.getCol('Article.Abstract.AbstractText').cast('string')),
            '[\'"]', ''
        ).alias('Abstract'),
        F.explode('Article.AuthorList.Author').alias('Authors')
    ).withColumn('Abstract', toAscii('Abstract') # Convert Abstract to pure ASCII
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
        ).cast('string').alias('Author_Identifier'), #Identifier is added after 2013.  All data previous to 2013 have no such information
        *[res.getCol('Authors.' + f).alias(f) for f in ['LastName', 'ForeName', 'Suffix', 'Initials']]
    ).drop('Authors')

def pubMedCitation(path):
    df = readPubMedXml(path)
    return normalizeDf(df)
