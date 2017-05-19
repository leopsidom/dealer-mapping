import os

import jaydebeapi
import re
import json
from src.config import ROOT_DIR
import os
import psycopg2

from src.models.users.errors import UserAlreadyRegisteredError


class DataBase:

    database_url = os.environ.get("DATABASE_URL")

    def __init__(self, user=None, pwd=None):
        self.user = user
        self.pwd = pwd

    def get_connection(self):
        conn = psycopg2.connect(DataBase.database_url)
        conn.autocommit = True

        return conn

    @staticmethod
    def get_connection_default():

        conn = psycopg2.connect(DataBase.database_url)
        conn.autocommit = True
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
            statement = "SELECT row_to_json({table}) FROM {table} WHERE {condition} limit 1".format(
                table=table,
                condition=" and ".join(key + "='" + val + "'" for key, val in fields.items())
            )
        else:
            statement = "SELECT row_to_json({table}) FROM {table} limit 1".format(table=table)

        print(statement)
        cursor.execute(statement)
        result_dict = cursor.fetchone()
        print(result_dict)
        if result_dict is None:
            return None
        else:
            return {k.lower(): v for k, v in result_dict[0].items()}

    @staticmethod
    def find_one_not_reviewed(user_id):
        """
        This method is mainly for getting a record that has not been reviewed for the user
        :param user_id:
        :return:
        """
        cursor = DataBase.get_connection_default().cursor()

        statement = "SELECT row_to_json(d) FROM DealerMapping d " \
                    "LEFT JOIN (SELECT * FROM UserReviewed WHERE UserId={user_id}) u " \
                    "ON d.RecordId=u.RecordId WHERE u.UserId IS NULL limit 1".format(
            user_id=user_id
        )

        print(statement)
        cursor.execute(statement)
        result_dict = cursor.fetchone()
        if result_dict is None:
            return None
        else:
            return result_dict[0]

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
                statement = "INSERT INTO {table} ({column_names}) VALUES ('{column_values}')".format(
                    table=table, column_names=", ".join(column_names), column_values="', '".join(column_values)
                )
                print(statement)
                cursor.execute(statement)

        except:
            raise UserAlreadyRegisteredError("This email has already been registered!")


