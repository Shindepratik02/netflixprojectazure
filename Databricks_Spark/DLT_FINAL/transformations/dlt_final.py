import dlt

dlttable_rules = {
    "rule1": "show_id is not null"
}
@dlt.expect_all_or_drop(dlttable_rules)
@dlt.table
def gold_netflixdirectors():
    df=spark.readStream.format("delta").load(
        "abfss://silver@netflixstorageacct.dfs.core.windows.net/netflix_directors"
    )
    return df

@dlt.expect_all_or_drop(dlttable_rules)
@dlt.table
def gold_netflixcountries():
    df=spark.readStream.format("delta").load(
        "abfss://silver@netflixstorageacct.dfs.core.windows.net/netflix_countries"
    )
    return df

@dlt.expect_all_or_drop(dlttable_rules)
@dlt.table
def gold_netflixcast():
    df=spark.readStream.format("delta").load(
        "abfss://silver@netflixstorageacct.dfs.core.windows.net/netflix_cast"
    )
    return df

@dlt.expect_all_or_drop(dlttable_rules)
@dlt.table
def gold_netflixcategory():
    df=spark.readStream.format("delta").load("abfss://silver@netflixstorageacct.dfs.core.windows.net/netflix_category"
    )
    return df
from pyspark.sql.functions import lit
@dlt.table
def neflixtitles_stg():
    df=spark.readStream.format("delta").load(
        "abfss://silver@netflixstorageacct.dfs.core.windows.net/netflix_titles")
    df=df.withColumn("newflag",lit(1))
    return df
master_rules ={
    "rule1": "newflag is not null",
    "rule2": "show_id is not null"
}
@dlt.expect_all_or_drop(master_rules)
@dlt.table
def gold_netflixmaster():
    df=dlt.read_stream("neflixtitles_stg")
    return df


