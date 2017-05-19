import os

import jaydebeapi
import re
import json
from src.config import ROOT_DIR


from src.models.users.errors import UserAlreadyRegisteredError


class DataBase:

    driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    driverPath = os.path.join(ROOT_DIR, "common/sqljdbc_6.0/enu/sqljdbc4.jar")
    # uri = "jdbc:sqlserver://pcds-sqlr.database.windows.net:1433;database=pcds_sqlr"
    uri = os.environ.get("URI")
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    # user = "pcds-sqlr@pcds-sqlr"
    # pwd = "Pure2017"

    def __init__(self, user=None, pwd=None):
        self.user = user
        self.pwd = pwd

    def get_connection(self):
        conn = jaydebeapi.connect(DataBase.driverClass, DataBase.uri,
                                  {'user': self.user, 'password': self.pwd},
                                  DataBase.driverPath)

        return conn

    @staticmethod
    def get_connection_default():
        print(os.environ.get("PASSWORD"))
        print(DataBase.user)
        print(DataBase.password)
        print("Current Path: ", os.path.abspath(__file__))
        conn = jaydebeapi.connect(DataBase.driverClass, DataBase.uri,
                                  {'user': DataBase.user, 'password': DataBase.password},
                                  DataBase.driverPath)

        return conn

    @staticmethod
    def find_one_default(table, fields):
        """
        This method is mainly for user look up
        :param table:
        :param fields:
        :return:
        """

        cursor = DataBase.get_connection_default().cursor()
        if len(fields) != 0:
            statement = "SELECT TOP 1 * FROM {table} WHERE {condition} for json auto".format(
                table=table,
                condition=" and ".join(key + "='" + val + "'" for key, val in fields.items())
            )
        else:
            statement = "SELECT TOP 1 * FROM {table} for json auto".format(table=table)

        cursor.execute(statement)
        json_str = cursor.fetchone()
        if json_str is None:
            return None
        else:
            return {k.lower(): v for k, v in json.loads(json_str[0])[0].items()}

    @staticmethod
    def find_one_not_reviewed(user_id):
        """
        This method is mainly for getting a record that has not been reviewed for the user
        :param user_id:
        :return:
        """
        cursor = DataBase.get_connection_default().cursor()

        statement = "SELECT TOP 1 d.* FROM DealerMapping d " \
                    "LEFT JOIN (SELECT * FROM UserReviewed WHERE UserId={user_id}) u " \
                    "ON d.RecordId=u.RecordId WHERE u.UserId IS NULL for json auto".format(
            user_id=user_id
        )

        cursor.execute(statement)
        json_str = cursor.fetchone()
        if json_str is None:
            return None
        else:
            return json.loads(json_str[0])[0]

    @staticmethod
    def add_one_default(table, fields):
        """
        This method is mainly for user register
        :param table:
        :param fields:
        :return:
        """
        try:
            cursor = DataBase.get_connection_default().cursor()
            if len(fields) != 0:
                column_names, column_values = zip(*fields.items())
                statement = "INSERT INTO " + table + "(" + \
                    ", ".join(column_names) + ")" + \
                    "VALUES ('" + "', '".join(column_values) + "')"
                print(statement)
                cursor.execute(statement)

        except:
            raise UserAlreadyRegisteredError("This email has already been registered!")


