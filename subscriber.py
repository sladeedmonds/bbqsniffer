import os
import sqlite3
from sqlite3 import Error
import datetime
import paho.mqtt.subscribe as subscribe
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SENSOR = os.getenv("SENSOR")
FOODPROBE = os.getenv("FOODPROBE")
SMOKERPROBE = os.getenv("SMOKERPROBE")
HOSTNAME = os.getenv("HOSTNAME")
DBNAME = os.getenv("DBNAME")


'''
Create a database and the rtl_433 table if it doesn't exist.
Create a callback function that prints a message published to the mqtt broker.
Subscribe to a topic using a callback.

sensor_data is a tuple and needs to be changed.
sensor_data_list is this conversion as a list.
sensor_data_list[1] then converts the temp reading to a float.
'''


def create_database_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def create_database_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as Error:
        print(Error)


def main():
    # database = r"/Users/sladeedmonds/Documents/bbq.db"
    database = DBNAME
    sql_create_food_probe_table = """ CREATE TABLE IF NOT EXISTS food_probe (
                                        id integer primary key,
                                        topic text,
                                        food_temp integer,
                                        timestamp text
                                    ); """

    sql_create_smoker_probe_table = """ CREATE TABLE IF NOT EXISTS smoker_probe (
                                        id integer primary key,
                                        topic text,
                                        smoker_temp integer,
                                        timestamp text
                                    ); """

    # create a database connection
    conn = create_database_connection(database)

    # create tables
    if conn is not None:
        # create rtl_433 table
        create_database_table(conn, sql_create_food_probe_table)
        create_database_table(conn, sql_create_smoker_probe_table)

    else:
        print("Error! cannot create the database connection.")


def insert_rtl_data(client, userdata, message):
    try:
        sqliteConnection = sqlite3.connect(DBNAME)
        cursor = sqliteConnection.cursor()

        sqlite_insert_rtl_with_food = """ INSERT INTO food_probe
                        (topic, food_temp, timestamp)
                        VALUES (?, ?, ?); """
        sqlite_insert_rtl_with_smoker = """ INSERT INTO smoker_probe
                        (topic, smoker_temp, timestamp)
                        VALUES (?, ?, ?); """

        sensor_data = (message.topic, message.payload)
        sensor_data_list = list(sensor_data)

        if any(SENSOR in i for i in sensor_data_list):
            if sensor_data_list[0] == FOODPROBE:
                sensor_data_list[1] = float(sensor_data_list[1])
                sensor_data_list[1] = sensor_data_list[1] * 9/5 + 32
                sensor_data_list[1] = round(sensor_data_list[1])
                dt = datetime.datetime.now()
                sensor_data_list.append(str(dt.replace(microsecond=0)))
                cursor.execute(sqlite_insert_rtl_with_food, sensor_data_list)
                sqliteConnection.commit()
                cursor.close()
            elif sensor_data_list[0] == SMOKERPROBE:
                sensor_data_list[1] = float(sensor_data_list[1])
                sensor_data_list[1] = sensor_data_list[1] * 9/5 + 32
                sensor_data_list[1] = round(sensor_data_list[1])
                dt = datetime.datetime.now()
                sensor_data_list.append(str(dt.replace(microsecond=0)))
                cursor.execute(sqlite_insert_rtl_with_smoker, sensor_data_list)
                sqliteConnection.commit()
                cursor.close()
    except sqlite3.Error as error:
        print("Failed to insert data.", error)


def on_message_print(client, userdata, message):
    # print("%s %s" % (message.topic, message.payload))
    print(message.topic, message.payload)


def on_message_log(client, userdata, message):
    f = open('rtl_433.txt', 'a+')
    f.write("%s %s \n" % (message.topic, message.payload))


if __name__ == '__main__':
    main()

while True:
    subscribe.callback(insert_rtl_data, "rtl_433/3bcbaceb4fe8/#",
                       auth={'username': USERNAME, 'password': PASSWORD}, hostname=HOSTNAME)
