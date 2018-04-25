from smv import SmvApp
import smv.functions as SF
from smv.error import SmvRuntimeError
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType
from Bio import Entrez

def toAscii(_col):
    """Convert Unicode to ascii, ignore errors
    """
    def conv(u):
        if (isinstance(u, str) or isinstance(u, unicode) or isinstance(u, bytes)):
            return u.encode("ascii", "ignore")
        else:
            return u
    return F.udf(conv)(_col)


def readPubMedXml(path, schemaPath, sqlContext):
    """Read in PubMed XML file to df"""

    # Read in schema
    import json
    with open(schemaPath, "r") as sj:
        schema_st = sj.read()
    schema = StructType.fromJson(json.loads(schema_st))

    # Load XML
    df = sqlContext\
        .read.format('com.databricks.spark.xml')\
        .options(rowTag='MedlineCitation')\
        .load(path, schema=schema)

    return df

def toQueryString(termList):
    return reduce(lambda h, t: h + ' OR ' + t, termList)


def pubmedSearch(meshTerms, numPastDays):
    query = toQueryString(meshTerms)
    Entrez.email = 'klu@twineanalytics.com'
    handle = Entrez.esearch(db='pubmed',
                            retmode='xml',
                            reldate=numPastDays,
                            term=query,
                            field='mesh',
                            usehistory='y')
    results = Entrez.read(handle)
    return results

def fetchResults(searchResult):
    env = searchResult['WebEnv']
    qkey = searchResult['QueryKey']
    handle = Entrez.efetch(db='pubmed',
                  retmode='xml',
                  WebEnv=env,
                  query_key=qkey)
    results = Entrez.read(handle)
    return results

def fetchURL(searchResult):
    env = searchResult['WebEnv']
    qkey = searchResult['QueryKey']
    return 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&WebEnv=' + env + '&query_key=' + qkey

# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&WebEnv=NCID_1_190852209_130.14.22.215_9001_1524516650_1189901983_0MetA0_S_MegaStore&query_key=1&retmode=xml

def getDate(prefix, date_type):
    """
    3 date_types: ArticleDate, PubDate, MedlineDate

    For ArticleDate, construct date from Year, Month and Day sub-fields
    For PubDate, construct date from Year, Month or Season, and Day sub-fields
    For MedlineDate, parse the field and construct

    According to https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#pubdate,
    all the followings are valid MedlineDate:

        1998 Dec-1999 Jan
        2000 Spring
        2000 Spring-Summer
        2000 Nov-Dec
        2000 Dec 23- 30

    Output as yyyy-MM-dd string
    """
    _seasonMap = {
        'Spring': '03',
        'Summer': '06',
        'Autumn': '09',
        'Fall': '09',
        'Winter': '12'
    }

    _monthMap = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }

    monthMap = SF.smvCreateLookUp(_monthMap, None, StringType())
    seasonMap = SF.smvCreateLookUp(_seasonMap, None, StringType())

    if (date_type == 'ArticleDate'):
        y = F.col(prefix + '.Year').cast('string')
        m = F.coalesce(
            monthMap(F.col(prefix + '.Month').cast('string')),
            F.lit('01')
        )
        d = F.coalesce(F.lpad(F.col(prefix + '.Day').cast('string'), 2, '0'), F.lit('01'))
    elif (date_type == 'PubDate'):
        y = F.col(prefix + '.Year').cast('string')
        m = F.coalesce(
            monthMap(F.col(prefix + '.Month').cast('string')),
            seasonMap(F.col(prefix + '.Season').cast('string')),
            F.lit('01')
        )
        d = F.coalesce(F.lpad(F.col(prefix + '.Day').cast('string'), 2, '0'), F.lit('01'))
    elif (date_type == 'MedlineDate'):
        mldcol = prefix + '.MedlineDate'
        y = F.substring(mldcol, 1, 4)                       # Always there
        m_or_s = F.regexp_extract(mldcol, '.... (\w+)', 1)  # Month or Season
        d_str = F.regexp_extract(mldcol, '.... ... (\d\d)', 1)  # could be empty
        m = F.coalesce(monthMap(m_or_s), seasonMap(m_or_s), F.lit('01'))
        d = F.when(d_str == '', F.lit('01')).otherwise(d_str)
    else:
        raise SmvRuntimeError("Unsuported date_type: {0}".format(date_type))

    return F.when((y.isNull()) | (F.length(y) == 0), F.lit(None).cast('string'))\
        .otherwise(F.concat_ws('-', y, m, d))


def normalizeDf(df):
    """
    Normalize pubmed df from deep XML/JSON structure to flat CSV type of structure
    """

    # Only keep records with author and nonnull keyword or mesh, otherwise will not
    # be useful anyhow
    df = df\
        .where(F.col('Article.AuthorList').isNotNull())\
        .where(F.col('KeywordList').isNotNull() | F.col('MeshHeadingList').isNotNull())

    year = F.coalesce(
        F.col('Article.ArticleDate.Year').cast('string'),
        F.col('Article.Journal.JournalIssue.PubDate.Year').cast('string'),
        F.substring('Article.Journal.JournalIssue.PubDate.MedlineDate', 1, 4)
    )

    journalDate = F.coalesce(
        getDate('Article.ArticleDate', 'ArticleDate'),
        getDate('Article.Journal.JournalIssue.PubDate', 'PubDate'),
        getDate('Article.Journal.JournalIssue.PubDate', 'MedlineDate')
    )

    # Abstract: see "18. <Abstract> and <AbstractText>" on https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html
    # TODO: InvestigatorList: "43. <InvestigatorList>" on https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html
    # TODO: Abstract is UTF8 and has quotation marks, etc. not friendly for SMV's csv persisting. Should add when SMV supports other format persist
    # SF.smvArrayCat('|', F.col('Article.Abstract.AbstractText._VALUE'))
    res = df.select(
        year.alias('Year'),
        F.concat(F.col('PMID._VALUE'), F.lit('_'), F.col('PMID._Version')).alias('PMID'),  # PubMed uniq id
        F.concat(F.col('Article.Journal.ISSN._IssnType'), F.lit('_'), F.col('Article.Journal.ISSN._VALUE')).alias('Journal_ISSN'),  # ISSN (optional)
        F.col('Article.ArticleTitle').alias('Article_Title'),
        F.col('Article.Journal.Title').alias('Journal_Title'),
        journalDate.alias('Journal_Publish_Date'),  # yyyy-MM-dd format
        F.col('MedlineJournalInfo.Country').alias('Journal_Country'),
        SF.smvArrayCat('|', F.col('MeshHeadingList.MeshHeading.DescriptorName._UI')).alias('Mesh_Headings'),
        F.col('KeywordList.Keyword').smvArrayFlatten(df)['_VALUE'].alias('Keywords'),
        F.explode('Article.AuthorList.Author').alias('Authors')
    )\
        .where(F.col('Authors').isNotNull())\
        .withColumn('Keywords', SF.smvArrayCat('|', F.col('Keywords')))\
        .withColumn('Affiliation', SF.smvArrayCat('|', F.col('Authors.AffiliationInfo.Affiliation')))\
        .withColumn('Author_Identifier', SF.smvStrCat('_', F.col('Authors.Identifier._Source'), F.col('Authors.Identifier._VALUE')))\
        .withColumn('LastName', F.col('Authors.LastName'))\
        .withColumn('ForeName', F.col('Authors.ForeName'))\
        .withColumn('Suffix', F.col('Authors.Suffix'))\
        .withColumn('Initials', F.col('Authors.Initials'))\
        .drop('Authors')

# The following info are not required. Might consider to add back if needed in the future
#        ListCol(df, 'Article.GrantList.Grant', '.GrantID', ['.GrantID']).alias('Grant_Ids'),
#        ListCol(df, 'ChemicalList.Chemical', '.NameOfSubstance._UI', ['.RegistryNumber', '.NameOfSubstance._UI']).alias('Chemicals'),

    return res


def pubMedCitation(path, sqlContext=None):
    sql_ctx = SmvApp.getInstance().sqlContext if (sqlContext is None) else sqlContext
    schemaPath = 'lib/pubmed_mini_schema.json'
    df = readPubMedXml(path, schemaPath, sql_ctx)
    return normalizeDf(df)
