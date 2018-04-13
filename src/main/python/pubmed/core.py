from smv import *
from smv.functions import *
from smv.error import SmvRuntimeError
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

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

    # Read in schema
    import json
    with open ("lib/pubmed_mini_schema.json", "r") as sj:
        schema_st = sj.read()
    schema = StructType.fromJson(json.loads(schema_st))

    # Load XML
    df = SmvApp.getInstance().sqlContext\
        .read.format('com.databricks.spark.xml')\
        .options(rowTag='MedlineCitation')\
        .load(path, schema = schema)

    # Only keep records with author and nonnull keyword or mesh, otherwise will not
    # be useful anyhow
    res = df\
        .where(F.col('Article.AuthorList').isNotNull())\
        .where(F.col('KeywordList').isNotNull() | F.col('MeshHeadingList').isNotNull())

    return df


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
        'Spring':'03',
        'Summer' :'06',
        'Autumn':'09',
        'Fall':'09',
        'Winter':'12'
    }

    _monthMap = {
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

    monthMap = smvCreateLookUp(_monthMap, None, StringType())
    seasonMap = smvCreateLookUp(_seasonMap, None, StringType())

    if (date_type  == 'ArticleDate'):
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
        d_str = F.regexp_extract(mldcol, '.... ... (\d\d)', 1) # could be empty
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

    journalDate = F.coalesce(
        getDate('Article.ArticleDate', 'ArticleDate'),
        getDate('Article.Journal.JournalIssue.PubDate', 'PubDate'),
        getDate('Article.Journal.JournalIssue.PubDate', 'MedlineDate')
    )

    year = F.coalesce(
        F.col('Article.ArticleDate.Year').cast('string'),
        F.col('Article.Journal.JournalIssue.PubDate.Year').cast('string'),
        F.substring('Article.Journal.JournalIssue.PubDate.MedlineDate', 1, 4)
    )

    def arrCat(col):
        return F.when(F.col(col).isNull(), F.lit('None').cast('string'))\
            .otherwise(smvArrayCat('|', F.col(col)))

    def flatArrCat(col, _elm):
        return F.udf(
            lambda aa: '|'.join([e[_elm] for s in aa for e in s]) if isinstance(aa, (list, list)) else None
        )(F.col(col)).cast('string')

    # Abstract: see "18. <Abstract> and <AbstractText>" on https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html
    # TODO: InvestigatorList: "43. <InvestigatorList>" on https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html
    res = df.select(
        year.alias('Year'),
        F.concat(F.col('PMID._VALUE'), F.lit('_'), F.col('PMID._VERSION')).alias('PMID'), # PubMed uniq id
        F.concat(F.col('Article.Journal.ISSN._IssnType'), F.lit('_'), F.col('Article.Journal.ISSN._VALUE')).alias('Journal_ISSN'), # ISSN (optional)
        F.col('Article.ArticleTitle').alias('Article_Title'),
        F.col('Article.Journal.Title').alias('Journal_Title'),
        journalDate.alias('Journal_Publish_Date'), # yyyy-MM-dd format
        F.col('MedlineJournalInfo.Country').alias('Journal_Country'),
        arrCat('MeshHeadingList.MeshHeading.DescriptorName._UI').alias('Mesh_Headings'),
        flatArrCat('KeywordList.Keyword', '_VALUE').alias('Keywords'),
#        F.regexp_replace(
#            F.coalesce(arrCat('Article.Abstract.AbstractText._VALUE'),
#                F.col('Article.Abstract.AbstractText').cast('string')),
#            '[^a-zA-Z]', ' '
#        ).alias('AbstractUDF'),
        F.explode('Article.AuthorList.Author').alias('Authors')
#    ).withColumn('Abstract', toAscii('AbstractUDF')).drop('AbstractUDF' # Convert Abstract to pure ASCII
    ).where(F.col('Authors').isNotNull())

# The following info are not required. Might consider to add back if needed in the future
#        ListCol(df, 'Article.GrantList.Grant', '.GrantID', ['.GrantID']).alias('Grant_Ids'),
#        ListCol(df, 'ChemicalList.Chemical', '.NameOfSubstance._UI', ['.RegistryNumber', '.NameOfSubstance._UI']).alias('Chemicals'),

    return res.smvSelectPlus(
        arrCat('Authors.AffiliationInfo.Affiliation').alias('Affiliation'),
        F.concat(
            F.col('Authors.Identifier._Source'),
            F.lit('_'),
            F.col('Authors.Identifier._VALUE')
        ).cast('string').alias('Author_Identifier'), #Identifier is added after 2013.  All data previous to 2013 have no such information
        *[F.col('Authors.' + f).alias(f) for f in ['LastName', 'ForeName', 'Suffix', 'Initials']]
    ).drop('Authors')

def pubMedCitation(path):
    df = readPubMedXml(path)
    return normalizeDf(df)
