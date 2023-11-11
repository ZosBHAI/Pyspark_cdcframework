

from pyspark.sql.functions import lit,col,lead,expr
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

'''from  pycdc.utilities import logger
logger = logger.CustomAppLogger()'''

from pycdc.utilities.Validator import validation_schema
from pycdc.utilities import app_logger
logger = app_logger.setUpLogging(__name__)

def cdcType2Main(spark,hist,snap,start_dt,end_dt,end_value,unique_columns,end_dt_logic):



    if validation_schema(hist,snap) == False:
        raise Exception('Schema between History and SnapShot Did Not Match')


    validation(hist, snap, start_dt, end_dt,unique_columns)



    #Converting a string  to timestamp
    end_value_ts = lit(end_value).cast(TimestampType())

    active_records,closed_records = splitHistActiveClosed(hist, start_dt, end_dt,  end_value_ts)

    logger.info("ACTIVE Records ")
    active_records.show(truncate=False)

    logger.info("CLOSED RECORD ")
    closed_records.show(truncate=False)

    cdcLogicImpl(spark, active_records, snap, unique_columns, start_dt, end_dt, end_value, end_dt_logic)

    return 0


def validation(hist,snap,start_dt,end_dt,unique_columns):
    hist_schema = hist.schema
    snap_schema = snap.schema
    diff_schema = set(hist_schema).union(set(snap_schema)).difference(set(hist_schema).intersection(set(snap_schema)))
    logger.info("Schema is %s" ,str(diff_schema))
    if len(diff_schema) > 0 :
        raise Exception('Schema between History and SnapShot Did Not Match History : {} Snapshot : {} Difference : {}'.format(hist_schema,snap_schema,diff_schema))
    columns = [start_dt,end_dt]
    columns.extend(unique_columns.split(","))
    columns = set(columns) ### Append or Extend return None, it will  only update the first list

    for column in columns:
        hist.schema[column]


def cdcLogicImpl(spark,active_records,snap,unique_columns,start_dt,end_dt,end_value,end_dt_logic):
    logger.info("CDC Logic Implementation")
    union_data = active_records.union(snap)\
        .dropDuplicates([start_dt,end_dt] + unique_columns.split(","))

    uniq = map(lambda x: col(x),unique_columns.split(","))
    print(end_dt_logic)
    print(end_value)
    window = Window.partitionBy(*uniq).orderBy(start_dt)
    cdc_data = union_data.select("*",(lead(col(start_dt) - expr(f"INTERVAL {end_dt_logic}"),1,end_value).over(window)).alias("new_end_date"))
    cdc_data.drop("end_dt").withColumnRenamed("new_end_date",end_dt)
    cdc_data.show()


def splitHistActiveClosed(hist, start_dt, end_dt,end_value):
    logger.info("Entering Closed and Active Records")
    active_records = hist.filter(col("end_dt") != lit(end_value))
    closed_records = hist.filter(col("end_dt") == lit(end_value))
    return active_records,closed_records