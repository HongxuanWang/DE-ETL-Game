# import necessary libraries
import pandas as pd 
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
# create sparksession
spark = SparkSession \
    .builder \
    .appName("PysparkGame") \
    .getOrCreate()

#Build SQL context object
sqlContext = SQLContext(spark)

def process_data(file_path): 
    files = os.listdir(file_path)
    for f in files:

        df = spark.read.json('{}/'.format(file_path)+f)

        # add source file name
        regex_str = "[\/]([^\/]+[\/][^\/]+)$"
        df = df.withColumn("sourcefile", regexp_extract(input_file_name(),regex_str,1))

        # covert all string fields to lowercse
        fields = df.schema.fields
        stringFields = filter(lambda f: isinstance(f.dataType, StringType), fields)
        nonStringFields = map(lambda f: col(f.name), filter(lambda f: not isinstance(f.dataType, StringType), fields))
        stringFieldsTransformed = map(lambda f: lower(col(f.name)).alias(f.name), stringFields) 
        allFields = [*stringFieldsTransformed, *nonStringFields]
        df = df.select(allFields)


        df.createOrReplaceTempView('df')
        print('1')


        if spark.sql("select count(*) as count from df \
                        where event_type not in ('player_death','pickup_items', 'game_econ', 'login')")\
                        .toPandas()['count'][0]>0:
            spark.sql("\
                  select * from df \
                  where event_type \
                  not in ('player_death','pickup_items', 'game_econ', 'login') ")\
            .write.format('json').mode('append').save('./malformed.log/malformed_unidentified_events.json')

        if "_corrupt_record" in df.columns:
            spark.sql("\
                      select * from df \
                      where _corrupt_record like '{%'")\
            .write.format('json').mode('append').save('./malformed.log/malformed_broken.json')

        if "_corrupt_record" in df.columns:
            spark.sql("\
                      select _corrupt_record, sourcefile from df\
                      where _corrupt_record not like '{%' ")\
            .write.format('json').mode('append').save('./error.log/error.json')

        #export valid records in each event type table
        if not os.path.isdir('./Result'):
            os.mkdir('./Result')


        def save_to_csv(event_type, sql_code, source_file_name ):
            spark.sql(sql_code)\
            .write.csv('./Result/'+event_type +'_tb_'+source_file_name, header = 'True', mode = 'overwrite')

        login_sql = "\
            select userid, time,sourcefile from df \
            where event_type = 'login' "
        save_to_csv("login", login_sql, f)



        pick_up_items_sql = "\
            select userid, time, item_id, item_sku, gold_earned,sourcefile from df \
            where event_type = 'pickup_items'"
        save_to_csv("pick_up_items", pick_up_items_sql, f)


        game_econ_sql = "\
            select userid, time, item_sku, gold_spent,sourcefile from df \
            where event_type = 'game_econ' "
        save_to_csv("game_econ", game_econ_sql, f)
        print('2')


        dmg=df.select('userid', 'time', 'killer', 'dmg_table',"sourcefile")\
        .where(df['event_type']== 'player_death')\
        .withColumn("dmg_table", explode('dmg_table'))\
        .select('userid', 'time', 'killer', lower(col("dmg_table.npc")).alias("npc"), "dmg_table.dmg", "sourcefile")
        dmg.createOrReplaceTempView('dmg')
        player_death_sql = "\
            select userid, time,killer,npc,sum(dmg) as total_dmg, sourcefile from dmg\
            group by userid, time,killer, npc,sourcefile"
        save_to_csv("player_death", player_death_sql,f)
        print('Success.')


def main(file_path = 'Data'):
    process_data(file_path)


if __name__ == "__main__":
    main()