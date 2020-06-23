import csv
import unittest
import json
import os
import string
import random
import datetime
from shutil import rmtree

from pymysql import ProgrammingError

from context import get_db_context
from handler import logger, parse_event, export_file, get_query_map, find_last_date
from kasasa_common.database import get_connection


class HandlerTests(unittest.TestCase):
    def setUp(self):
        # setup env vars that the handler will need
        os.environ["TEST"] = "True"
        os.environ["ENV"] = "sandbox"
        os.environ["REGION"] = "us-west-2"
        os.environ["DEST_BUCKET"] = "fi-data-archive-{env}-{region}"
        os.environ["DEST_PREFIX"] = "reservoir/{export_name}/{fi_id}/vendor_{vendor}/{date}"
        os.environ["MAILING_LIST_SOURCE_BUCKET"] = "test-map-data-sandbox-us-west-2"
        os.environ["MAILING_LIST_PREFIX"] = "archive/to_livetech"
        os.environ["LOCAL_SNS"] = "True"
        os.environ["CONFIG_FILE"] = "tests/test_export_config.json"

        # create one-time-use database with empty prospects table to test empty set failure
        self.one_time_use_fi_id = self.create_random_fi_id()

        # setup class variables to use throughout tests
        self.query_map = get_query_map()

        self.expected_success_string_customer = "successful export for FIID: {fi_id} and Export Name: {export_name}\n" + \
            "successful export for FIID: {fi_id} and Export Name: {export_name2}"
        self.expected_success_string_prospects = "successful export for FIID: {fi_id} and Export Name: {export_name}"
        self.expected_success_string_generic = "successful export for FIID: {fi_id} and Export Name: {export_name}"

        self.date_utc = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        self.cwd = os.path.dirname(os.path.realpath(__file__))

        self.vendor_fico = "fico"
        self.vendor_livetech = "livetech"

        self.good_fi_id = self.one_time_use_fi_id
        self.bad_fi_id = "no_such_fi_id"
        self.bad_table = "no_such_table"
        self.prospects_table = "fico_sourced_prospects"
        self.customer_table = "fico_sourced_customers"
        self.live_tech_mailing_table = "mailing_list"
        self.livetech_customer_table = "customers"

        self.prospects_export = self.query_map[self.prospects_table][0]["Export_Name"]
        self.fico_customer_export = self.query_map[self.customer_table][0]["Export_Name"]
        self.mapping_export = self.query_map[self.customer_table][1]["Export_Name"]
        self.mailing_export = self.query_map[self.live_tech_mailing_table][0]["Export_Name"]
        self.livetech_customer_export = self.query_map[self.livetech_customer_table][0]["Export_Name"]

        self.generic_file_name = "{cwd}/{bucket}/{prefix}/{export_name}.txt"

        self.good_message = json.dumps({
            "Name": "map-lambda-upsert",
            "FIID": self.good_fi_id,
            "Table": self.prospects_table
        })
        self.bad_message = json.dumps({
            "Name": "map-lambda-upsert"
        })

        self.good_event = {
            "Records": [
                {"Sns": {"Message": self.good_message}},
            ]
        }
        self.bad_event = {
            "Records": [
                {"Sns": {"Message": self.bad_message}},
            ]
        }

        db_check_sql = "show databases like 'fi_{fi_id}';"
        db_create_sql = "create database fi_{fi_id};"
        counter = 0

        # setup connection and then connect
        map_stack_context = get_db_context(cluster='map_stack', database=False)
        self.map_stack_connection = get_connection(map_stack_context, local_infile=1)

        with self.map_stack_connection.cursor() as cursor:
            # check for existing database
            cursor.execute(db_check_sql.format(fi_id=self.one_time_use_fi_id))
            result = cursor.fetchall()
            while result != () and counter < 3:
                # if we find a result, remake the random string and try again until successful or we hit a counter cap
                # we don't want infinite databases if something goes wrong
                counter += 1
                self.one_time_use_fi_id = self.create_random_fi_id()
                cursor.execute(db_check_sql)
                result = cursor.fetchall()
            if result == ():
                # create db
                result = cursor.execute(db_create_sql.format(fi_id=self.one_time_use_fi_id))
                logger.info("created db fi_{fi_id}".format(fi_id=self.one_time_use_fi_id))

                # be less strict, this is how prod is setup
                sql_strict1 = "SET GLOBAL sql_mode = 'NO_ENGINE_SUBSTITUTION';"
                sql_strict2 = "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION';"
                cursor.execute(sql_strict1)
                cursor.execute(sql_strict2)

                database = "fi_{}".format(self.one_time_use_fi_id)
                map_stack_context = get_db_context('map_stack', database)
                self.map_stack_connection = get_connection(map_stack_context, local_infile=1)  # one connection to rule them all

                # make schema changes
                self.run_flyway_changes()
            else:
                raise ValueError("unable to make a unique database")

    def tearDown(self):
        # remove the database we created from setUp
        with self.map_stack_connection.cursor() as cursor:
            db_drop = "drop database fi_{fi_id};"
            cursor.execute(db_drop.format(fi_id=self.one_time_use_fi_id))

        # clean up test folder
        bucket = os.environ["DEST_BUCKET"].format(env=os.getenv("ENV"), region=os.getenv("REGION"))
        self.clear_folder(bucket)

    def create_folder(self, folder_path):
        try:
            os.makedirs(folder_path)
        except Exception as e:
            pass

    def clear_folder(self, folder_path):
        try:
            rmtree(folder_path)
        except Exception as e:
            pass

    def create_random_fi_id(self):
        return "5" + "".join(random.choice(string.digits) for _ in range(8))

    def populate_fico_customers_table(self, kwargs):
        sql = """
            INSERT INTO fi_{fi_id}.fico_sourced_customers
                (financial_institution_id, customer_kasasa_key, fico_individual_id, kasasa_master_person_id, email, fico_file_name)
            VALUES ({fi_id}, '{ckk}', '{fico_id}', '{kmpid}', '{email}', '{file_name}');
        """.format(**kwargs)

        with self.map_stack_connection.cursor() as cursor:
            cursor.execute(sql)
        self.map_stack_connection.commit()

    def populate_fico_prospects_table(self, kwargs):
        sql = """
            INSERT INTO fi_{fi_id}.fico_sourced_prospects
                (prospect_kasasa_key, financial_institution_id, fico_individual_id, kasasa_master_person_id, email, fico_file_name)
            VALUES ('{pkk}', '{fi_id}', '{fico_id}', '{kmpid}', '{email}', '{file_name}');
        """.format(**kwargs)

        with self.map_stack_connection.cursor() as cursor:
            cursor.execute(sql)
        self.map_stack_connection.commit()

    def populate_livetech_mailing_list_table(self, kwargs):
        sql = """
            INSERT INTO map_campaign.livetech_mailing_list
                (list_id, customer_kasasakey, list_name, financial_institution_id)
            VALUES ('{list_id}', '{ckk}', '{list_name}', '{fi_id}');
        """.format(**kwargs)

        with self.map_stack_connection.cursor() as cursor:
            cursor.execute(sql)
        self.map_stack_connection.commit()

    def populate_livetech_customers_table(self, kwargs):
        sql = """
            INSERT INTO map_campaign.livetech_customers
                (financial_institution_id, customer_kasasakey, fico_individual_id, kasasa_master_person_id, email)
            VALUES ({fi_id}, '{ckk}', '{fico_id}', '{kmpid}', '{email}');
        """.format(**kwargs)

        with self.map_stack_connection.cursor() as cursor:
            cursor.execute(sql)
        self.map_stack_connection.commit()

    def populate_fico_prospects_customer_mapping_table(self, kwargs):
        sql = """
            INSERT INTO fi_{fi_id}.prospect_customer_mapping
                (prospect_kasasa_key, customer_kasasa_key, financial_institution_id)
            VALUES ('{pkk}', '{ckk}', '{fi_id}');
        """.format(**kwargs)

        with self.map_stack_connection.cursor() as cursor:
            cursor.execute(sql)
        self.map_stack_connection.commit()

    def run_flyway_changes(self):
        flyway_base_path = os.environ.get("FLYWAY_PATH", "")
        if flyway_base_path:
            if os.path.isdir(flyway_base_path):
                flyway_path = os.path.join(flyway_base_path, "src/s3files/fi_specific_migrations/")
                with self.map_stack_connection.cursor() as cursor:
                    fi_sql_script_list = [os.path.join(flyway_path, f) for f in os.listdir(flyway_path) if os.path.isfile(os.path.join(flyway_path, f))]
                    fi_sql_script_list.sort()
                    for sql_script in fi_sql_script_list:
                        logger.info("{fi}:Applying {sql_script}".format(fi=self.one_time_use_fi_id, sql_script=sql_script))
                        sql_file = open(os.path.join(flyway_path, sql_script), "r")
                        sql = sql_file.read()
                        for query in sql.split(";\n"):
                            if query.strip():
                                cursor.execute(query)
                                result = cursor.fetchall()
                        self.map_stack_connection.commit()
            else:
                ImportError("FLYWAY_PATH '{}' does not exist".format(os.environ["FLYWAY_PATH"]))
        else:
            raise ImportError("FLYWAY_PATH not provided")

    # BEGIN TESTS ###########################################################################

    def test_parse_event_happy_path(self):
        # Arrange
        message = json.loads(self.good_event["Records"][0]["Sns"]["Message"])
        expected_fi_id = message["FIID"]
        expected_table = message["Table"]
        replace_kwargs = dict(
            fi_id=expected_fi_id,
            export_name=self.query_map[expected_table][0]["Export_Name"],
            query_name=self.query_map[expected_table][0]["Query"],
            table=expected_table,
            vendor=self.query_map[expected_table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[expected_table][0]["Bucket"].format(**replace_kwargs)
        prefix = self.query_map[expected_table][0]["Prefix"].format(**replace_kwargs)
        test_folder = os.path.join(bucket)
        self.create_folder(test_folder)

        # Act
        actual_fi_id, actual_table = parse_event(self.good_event)

        # Assert
        self.assertEqual(actual_fi_id, expected_fi_id)
        self.assertEqual(actual_table, expected_table)

    def test_parse_event_unhappy_path(self):
        # Arrange
        message = json.loads(self.bad_event["Records"][0]["Sns"]["Message"])
        expected_fi_id = None
        expected_table = None

        # Act
        actual_fi_id, actual_table = parse_event(self.bad_event)

        # Assert
        self.assertEqual(actual_fi_id, expected_fi_id)
        self.assertEqual(actual_table, expected_table)


    def test_find_last_date(self):

        # Act
        result = str(find_last_date()).split('-')

        # Arrange
        year, month, day = [int(i) for i in result]
        month_with_last_day_31 = [1, 3, 5, 7, 8, 10, 12]
        month_with_last_day_30 = [4, 6, 9, 11]

        # Assert
        if month in month_with_last_day_30:
            self.assertEqual(day, 30)
        elif month in month_with_last_day_31:
            self.assertEqual(day, 31)
            # Checking for leap year
        elif (year % 400 == 0) or ((year % 4 == 0) and (year % 100 != 0)):
            self.assertEqual(day, 29)
        else:
            self.assertEqual(day, 28)


    def test_export_file_happy_path_good_fi_id_good_table_fico_prospects(self):
        # Arrange
        fi_id = self.good_fi_id
        table = self.prospects_table
        name = self.prospects_export
        replace_kwargs = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][0]["Export_Name"],
            query_name=self.query_map[table][0]["Query"],
            table=table,
            vendor=self.query_map[table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[table][0]["Bucket"].format(**replace_kwargs)
        prefix = self.query_map[table][0]["Prefix"].format(**replace_kwargs)
        test_folder = os.path.join(bucket)
        self.create_folder(test_folder)

        file_name = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=name,
            bucket=bucket,
            prefix=prefix.format(
                fi_id=fi_id,
                export_name=name,
                vendor=self.vendor_fico,
                date=self.date_utc
            ),
            cwd=self.cwd
        )
        # if the file currently exists before the test is run,
        # remove it so we know we have a good file to test against
        if (os.path.isfile(file_name)):
            os.remove(file_name)

        # {pkk}, {fi_id}, {fico_id}, {kmpid}, {email}, {file_name}
        table_kwargs = dict(
            fi_id=fi_id,
            pkk="1234-5678",
            ckk="8765-4321",
            fico_id="12346578",
            kmpid="blergh",
            email="blergh@blergh.com",
            file_name="blergh.txt"
        )
        self.populate_fico_prospects_table(table_kwargs)

        # Act
        actual_success_string = export_file(fi_id, table)

        # Assert
        self.assertEqual(actual_success_string, self.expected_success_string_prospects.format(fi_id=fi_id, export_name=name))
        self.assertTrue(os.path.isfile(file_name), msg="file name: {}".format(file_name))

    def test_export_file_happy_path_good_fi_id_good_table_fico_customer(self):
        # Arrange
        fi_id = self.good_fi_id
        table = self.customer_table

        replace_kwargs1 = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][0]["Export_Name"],
            query_name=self.query_map[table][0]["Query"],
            table=table,
            vendor=self.query_map[table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )
        replace_kwargs2 = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][1]["Export_Name"],
            query_name=self.query_map[table][1]["Query"],
            table=table,
            vendor=self.query_map[table][1]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[table][0]["Bucket"].format(**replace_kwargs1)
        prefix1 = self.query_map[table][0]["Prefix"].format(**replace_kwargs1)
        prefix2 = self.query_map[table][1]["Prefix"].format(**replace_kwargs2)
        name1 = self.fico_customer_export
        name2 = self.mapping_export

        file_name1 = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=name1,
            bucket=bucket,
            prefix=prefix1.format(
                fi_id=fi_id,
                export_name=name1,
                vendor=self.vendor_fico,
                date=self.date_utc
            ),
            cwd=self.cwd
        )
        file_name2 = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=name2,
            bucket=bucket,
            prefix=prefix2.format(
                fi_id=fi_id,
                export_name=name2,
                vendor=self.vendor_fico,
                date=self.date_utc
            ),
            cwd=self.cwd
        )

        test_folder = os.path.join(bucket)
        self.create_folder(test_folder)

        # {fi_id}, {ckk}, {fico_id}, {kmpid}, {email}, {file_name}
        table_kwargs = dict(
            fi_id=fi_id,
            ckk="1234-5678",
            pkk="8765-4321",
            fico_id="12346578",
            kmpid="blergh",
            email="blergh@blergh.com",
            file_name="blergh.txt"
        )
        self.populate_fico_customers_table(table_kwargs)
        self.populate_fico_prospects_customer_mapping_table(table_kwargs)

        # if the file currently exists before the test is run,
        # remove it so we know we have a good file to test against
        if (os.path.isfile(file_name1)):
            os.remove(file_name1)
        if (os.path.isfile(file_name2)):
            os.remove(file_name2)

        # Act
        actual_success_string = export_file(fi_id, table)

        # Assert
        self.assertEqual(actual_success_string, self.expected_success_string_customer.format(fi_id=fi_id, export_name=name1, export_name2=name2))
        self.assertTrue(os.path.isfile(file_name1), msg="file name: {}".format(file_name1))
        self.assertTrue(os.path.isfile(file_name2), msg="file name: {}".format(file_name2))

    def test_export_file_unhappy_path_bad_fi_id_bad_table(self):
        # Arrange
        fi_id = self.bad_fi_id
        table = self.bad_table

        expected_fail_string = "export_file_key {table} not found!".format(table=table)

        # Act
        actual_fail_string = export_file(fi_id, table)

        # Assert
        self.assertTrue(expected_fail_string in actual_fail_string, msg="Actual fail string is: '{fs}'".format(fs=actual_fail_string))

    def test_export_file_unhappy_path_bad_fi_id_good_table_fico_prospects(self):
        # Arrange
        fi_id = self.bad_fi_id
        export_name = self.prospects_export
        table = self.prospects_table

        replace_kwargs = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][0]["Export_Name"],
            query_name=self.query_map[table][0]["Query"],
            table=table,
            vendor=self.query_map[table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[table][0]["Bucket"].format(**replace_kwargs)
        prefix = self.query_map[table][0]["Prefix"].format(**replace_kwargs)
        test_folder = os.path.join(bucket)
        self.create_folder(test_folder)

        file_name = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=export_name,
            bucket=bucket,
            prefix=prefix,
            cwd=self.cwd
        )

        if (os.path.isfile(file_name)):
            # if the file currently exists before the test is run,
            # remove it so we know we have a good file to test against
            os.remove(file_name)

        # Act
        actual_fail_string = export_file(fi_id, table)

        # Assert
        self.assertFalse(os.path.isfile(file_name))
        self.assertRaises(ProgrammingError)  # meaning the database or table doesn't exist

    def test_export_file_unhappy_path_good_fi_id_good_table_fico_prospects_empty_table(self):
        # Arrange
        fi_id = self.one_time_use_fi_id  # randomly created string to avoid a real db with actual data
        table = self.prospects_table
        name = self.mapping_export

        with self.map_stack_connection.cursor() as cur:
            # delete dummy record
            sql = f"delete from fi_{fi_id}.fico_sourced_prospects;"
            cur.execute(sql)
            self.map_stack_connection.commit()

        replace_kwargs = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][0]["Export_Name"],
            query_name=self.query_map[table][0]["Query"],
            table=table,
            vendor=self.query_map[table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[table][0]["Bucket"].format(**replace_kwargs)
        prefix = self.query_map[table][0]["Prefix"].format(**replace_kwargs)
        # test_folder = os.path.join(bucket)
        self.create_folder(bucket)

        file_name = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=name,
            bucket=bucket,
            prefix=prefix.format(
                fi_id=fi_id,
                export_name=name,
                vendor=self.vendor_fico,
                date=datetime.datetime.utcnow().strftime('%Y-%m-%d')
            ),
            cwd=self.cwd
        )

        # Act
        actual_fail_string = export_file(fi_id, table)

        # Assert
        self.assertTrue("No results to export!" in actual_fail_string, msg="Actual fail string is: '{fs}'".format(fs=actual_fail_string))
        self.assertFalse(os.path.isfile(file_name))

    def test_export_file_happy_path_good_fi_id_good_table_livetech_mailing_list(self):
        # Arrange
        fi_id = self.good_fi_id
        table = self.live_tech_mailing_table
        name = self.mailing_export

        replace_kwargs = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][0]["Export_Name"],
            query_name=self.query_map[table][0]["Query"],
            table=table,
            vendor=self.query_map[table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[table][0]["Bucket"].format(**replace_kwargs)
        prefix = self.query_map[table][0]["Prefix"].format(**replace_kwargs)
        test_folder = os.path.join(bucket)
        self.create_folder(test_folder)

        expected_success_string = self.expected_success_string_generic.format(fi_id=fi_id, export_name=name)
        file_name = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=name,
            bucket=bucket,
            prefix=prefix.format(
                fi_id=fi_id,
                export_name=name,
                vendor=self.vendor_livetech,
                date=datetime.datetime.utcnow().strftime('%Y-%m-%d')
            ),
            cwd=self.cwd
        )

        if (os.path.isfile(file_name)):
            # if the file currently exists before the test is run,
            # remove it so we know we have a good file to test against
            os.remove(file_name)

        # {list_id}, {ckk}, {list_name}, {fi_id}
        table_kwargs = dict(
            fi_id=fi_id,
            ckk="1234-5678",
            list_id="blergh",
            list_name="blergh"
        )
        self.populate_livetech_mailing_list_table(table_kwargs)

        # Act
        actual_success_string = export_file(fi_id, table)

        # Assert
        self.assertTrue(expected_success_string in actual_success_string, msg="actual_success_string = '{}'".format(actual_success_string))
        self.assertTrue(os.path.isfile(file_name))
        # assert that we've grabbed the right file and exported the right data
        with open(file_name, "r") as check_file:
            csv_reader = csv.reader(check_file, delimiter="\x1f")
            for row in csv_reader:
                assert(row[0] == table_kwargs["ckk"])
                assert(row[1] == table_kwargs["list_name"])
                assert(row[2] == table_kwargs["fi_id"])

    def test_export_file_happy_path_good_fi_id_good_table_livetech_customers(self):
        # Arrange
        fi_id = self.good_fi_id
        table = self.livetech_customer_table
        name = self.livetech_customer_export

        replace_kwargs = dict(
            fi_id=fi_id,
            export_name=self.query_map[table][0]["Export_Name"],
            query_name=self.query_map[table][0]["Query"],
            table=table,
            vendor=self.query_map[table][0]["Vendor"],
            date=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
            env=os.environ["ENV"],
            region=os.environ["REGION"]
        )

        bucket = self.query_map[table][0]["Bucket"].format(**replace_kwargs)
        prefix = self.query_map[table][0]["Prefix"].format(**replace_kwargs)
        test_folder = os.path.join(bucket)
        self.create_folder(test_folder)

        expected_success_string = self.expected_success_string_generic.format(fi_id=fi_id, export_name=name)
        file_name = self.generic_file_name.format(
            fi_id=fi_id,
            export_name=name,
            bucket=bucket,
            prefix=prefix.format(
                fi_id=fi_id,
                export_name=name,
                vendor=self.vendor_livetech,
                date=datetime.datetime.utcnow().strftime('%Y-%m-%d')
            ),
            cwd=self.cwd
        )

        if (os.path.isfile(file_name)):
            # if the file currently exists before the test is run,
            # remove it so we know we have a good file to test against
            os.remove(file_name)

        # {fi_id}, {ckk}, {fico_id}, {kmpid}, {email}, {file_name}
        table_kwargs = dict(
            fi_id=fi_id,
            ckk="1234-5678",
            fico_id="1234-5678",
            kmpid="1234-5678",
            email="blergh@blergh.com"
        )
        self.populate_livetech_customers_table(table_kwargs)

        # Act
        actual_success_string = export_file(fi_id, table)

        # Assert
        self.assertTrue(expected_success_string in actual_success_string, msg="actual_success_string = '{}'".format(actual_success_string))
        self.assertTrue(os.path.isfile(file_name))
        # assert that we've grabbed the right file and exported the right data
        with open(file_name, "r") as check_file:
            csv_reader = csv.reader(check_file, delimiter="\x1f")
            for row in csv_reader:
                # https://confluence.bancvue.com/display/DEV/Customer+List+File
                assert(row[0] == table_kwargs["ckk"])
                assert(row[2] == table_kwargs["kmpid"])
                assert(row[1] == table_kwargs["fi_id"])
                assert(row[17] == table_kwargs["email"])



if __name__ == "__main__":
    unittest.main()
