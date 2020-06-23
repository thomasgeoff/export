"""
    lambda to export from map or platform shared database into s3 bucket upon successful insert/upsert to table (fico_sourced_prospects, fico_sourced_customers)
    also will take an s3 file, transform it, and copy new file to new s3 location (mailing_list)
"""

import re
import csv
import logging
import traceback
import json
import os
import tempfile
import calendar
from datetime import datetime
from context import get_db_context
from kasasa_common.aws.s3 import get_s3
from kasasa_common.aws.sns import get_sns
from kasasa_common.database import get_connection

logger = logging.getLogger("handler")
logger.setLevel("INFO")


def get_query_map():
    config_path = os.environ.get("CONFIG_FILE", "export_config.json")
    with open(config_path, "r") as reader:
        query_map = json.loads(reader.read().replace("\n", ""))
    return query_map


def record_error(message, fi_id=None, export_file_key=None):
    # send sns slack message
    kwargs = dict()
    kwargs["Error"] = message
    kwargs["FIID"] = fi_id
    kwargs["ExportFileKey"] = export_file_key

    # log error
    logger.error(message)
    # send slack message
    send_sns_slack_notification(kwargs)


def send_sns_slack_notification(kwargs):
    # setup standard slack items
    if "Error" in kwargs:
        kwargs["Alarm"] = "FAILURE"
    else:
        kwargs["Alarm"] = "SUCCESS"
    if "Type" not in kwargs:
        kwargs["Type"] = "lambda"
    if "Name" not in kwargs:
        kwargs["Name"] = "map-lambda-export-s3"
    if "Product" not in kwargs:
        kwargs["Product"] = "map"

    # send the subject and message to slack
    logger.info("Sending Slack message")

    if os.getenv('SNS_SLACK_ARN'):
        sns_slack = get_sns(os.getenv('SNS_SLACK_ARN'))
        sns_slack.publish(message=kwargs)
    else:
        logger.error("SNS publishing unavailable, message would have been as follows:")

    logger.info(json.dumps(kwargs))


def write_and_upload_file(file_, destination, key, contents):
    # write to file
    with open(file_.name, "w") as write_to_file:
        file_writer = csv.writer(write_to_file, delimiter="\x1f")

        for row in contents:
            file_writer.writerow(row)

    # save file to s3 bucket/prefix
    with open(file_.name, "rb") as upload_file:
        destination.upload(localobj=upload_file, name=key, overwrite=True)


def parse_event(event):
    # NOTE: for historic purposes this value is called `Table` in the event
    # but it can more accurately be considered the export file key
    export_file_key = None
    fi_id = None

    # setup fi_id and export_file_key based on cloudwatch event
    if event.get("source", "nevergonnagiveyouup").lower() in "aws.events":
        logger.info("Scheduled Event based source")
        fi_id = event.get("FIID")
        export_file_key = event.get("Table")
    # setup fi_id and export_file_key based on s3 notification event
    elif event.get("Records", ""):
        logger.info("S3 SNS based source")
        message = json.loads(event["Records"][0]["Sns"]["Message"])

        fi_id = message.get("FIID")
        export_file_key = message.get("Table")
    else:
        error_message = "unknown event: {}".format(json.dumps(event))
        record_error(message=error_message)

    return fi_id, export_file_key


def find_last_date():
    # find the last date of the current month
    current_date = datetime.utcnow().date()
    current_month_last_day = calendar.monthrange(current_date.year, current_date.month)[1]
    current_month_last_date = current_date.replace(day=current_month_last_day)
    return current_month_last_date


def export_file(fi_id, export_file_key):
    return_string = ""
    query_map = get_query_map()

    try:
        # choose your own adventure depending on the data to export
        if export_file_key in query_map:
            for index in range(len(query_map[export_file_key])):
                # setup common replacement items
                replacement_dict = dict(fi_id=fi_id,
                                        export_name=query_map[export_file_key][index]["Export_Name"],
                                        query_name=query_map[export_file_key][index]["Query"],
                                        export_file_key=export_file_key,
                                        vendor=query_map[export_file_key][index]["Vendor"],
                                        date=datetime.utcnow().strftime('%Y-%m-%d'),
                                        env=os.environ["ENV"],
                                        region=os.environ["REGION"],
                                        creation_date_time=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                        current_month_last_date=find_last_date(),)

                # setup db connection for the cluster in the export file config
                cluster = query_map[export_file_key][index]["Cluster"]
                database = query_map[export_file_key][index]["Database"].format(**replacement_dict)
                context = get_db_context(cluster, database)
                connection = get_connection(context)

                # get the query to run
                with open("sqls/{query_name}".format(**replacement_dict), "r") as query_file:
                    sql = query_file.read().format(**replacement_dict)
                    logger.info(sql)

                with connection.cursor() as cursor:
                    # run query
                    cursor.execute(sql)
                    # store result
                    result = cursor.fetchall()
                    # throw error if we get no results back
                    if len(result) == 0:
                        raise ValueError("No results to export! FIID: {fi_id} Export Name: {export_name}".format(**replacement_dict))

                    # setup file to write to and upload
                    with tempfile.NamedTemporaryFile() as my_tmp_file:
                        destination = get_s3(query_map[export_file_key][index]["Bucket"].format(**replacement_dict))
                        key = "{prefix}/{export_name}.txt".format(
                            # prefix looks like "reservoir/{export_name}/{fi_id}/vendor_{vendor}/{date}"
                            prefix=query_map[export_file_key][index]["Prefix"].format(**replacement_dict),
                            **replacement_dict
                        )
                        write_and_upload_file(my_tmp_file, destination, key, result)

                    return_string = "\n".join([return_string,
                                               "successful export for FIID: {fi_id} and Export Name: {export_name}".format(**replacement_dict)]).strip()

        else:
            error_message = "export_file_key {} not found!\n".format(export_file_key)
            record_error(message=error_message, fi_id=fi_id, export_file_key=export_file_key)
            return_string = "\n".join([return_string, error_message]).strip()

    except Exception as e:
        logger.error(traceback.format_exc())
        error_message = "{exception} {args}".format(exception=e.__class__.__name__, args=str(e.args))
        record_error(message=error_message, fi_id=fi_id, export_file_key=export_file_key)
        return_string = "\n".join([return_string, error_message]).strip()

    finally:
        return return_string


def entry_point(event, context=None):
    fi_id, export_file_key = parse_event(event)

    logger.info("starting export for FIID: {fi_id} and Export File Key: {export_file_key}".format(fi_id=fi_id, export_file_key=export_file_key))
    ret = export_file(fi_id=fi_id, export_file_key=export_file_key)
    logger.info(ret)


if __name__ == "__main__":
    logger.warn("Use the pytest test_handler.py script to test/run locally")
    entry_point(event=None, context=None)
