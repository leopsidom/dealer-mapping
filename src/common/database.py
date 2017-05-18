import os
import pymssql
import json


from src.models.users.errors import UserAlreadyRegisteredError


class DataBase:

    server = os.environ.get("SERVER")
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    database = os.environ.get("DATABASE")

    def __init__(self, user=None, pwd=None):
        self.user = user
        self.pwd = pwd

    def get_connection(self):
        conn = pymssql.connect(server=DataBase.server, port=1433, user=self.user,
                               password=self.pwd, database=DataBase.database)

        return conn

    @staticmethod
    def get_connection_default():
        print(os.environ.get("PASSWORD"))
        print(DataBase.user)
        print(DataBase.password)
        conn = pymssql.connect(server=DataBase.server, port=1433, user=DataBase.user,
                               password=DataBase.password, database=DataBase.database)

        return conn

    @staticmethod
    def find_one_default(table, fields):
        """
        This method is mainly for user look up
        :param table:
        :param fields:
        :return:
        """

        conn = DataBase.get_connection_default()
        cursor = conn.cursor()
        if len(fields) != 0:
            statement = "SELECT TOP 1 * FROM {table} WHERE {condition} for json auto".format(
                table=table,
                condition=" and ".join(key + "='" + val + "'" for key, val in fields.items())
            )
        else:
            statement = "SELECT TOP 1 * FROM {table} for json auto".format(table=table)

        cursor.execute(statement)
        conn.commit()
        json_str = cursor.fetchone()
        conn.close()
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
        conn = DataBase.get_connection_default()
        cursor = conn.cursor()

        statement = "SELECT TOP 1 d.* FROM DealerMapping d " \
                    "LEFT JOIN (SELECT * FROM UserReviewed WHERE UserId={user_id}) u " \
                    "ON d.RecordId=u.RecordId WHERE u.UserId IS NULL for json auto".format(
            user_id=user_id
        )

        cursor.execute(statement)
        conn.commit()
        json_str = cursor.fetchone()
        conn.close()
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
            conn = DataBase.get_connection_default()
            cursor = conn.cursor()
            if len(fields) != 0:
                column_names, column_values = zip(*fields.items())
                statement = "INSERT INTO " + table + "(" + \
                    ", ".join(column_names) + ")" + \
                    "VALUES ('" + "', '".join(column_values) + "')"
                print(statement)
                cursor.execute(statement)
                conn.commit()
                conn.close()

        except:
            raise UserAlreadyRegisteredError("This email has already been registered!")


