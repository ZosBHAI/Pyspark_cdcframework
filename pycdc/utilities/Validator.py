
from pycdc.utilities import app_logger
logger = app_logger.setUpLogging(__name__)

def validation_schema(hist,snap):
    hist_schema = hist.schema
    snap_schema = snap.schema
    diff_schema = set(hist_schema).union(set(snap_schema)).difference(set(hist_schema).intersection(set(snap_schema)))
    logger.info("Schema is %s" ,str(diff_schema))
    if len(diff_schema) > 0 :
        return False
    return True
    raise Exception('Schema between History and SnapShot Did Not Match History : {} Snapshot : {} Difference : {}'.format(hist_schema,snap_schema,diff_schema))
