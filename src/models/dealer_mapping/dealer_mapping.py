import datetime
from flask import session
from src.common.database import DataBase
from src.models.users.user import User


class DealerMapping:

    order_columns = ["DealerName", "State", "City", "Lat", "Long", "StreetNo", "StreetName", "Zip", "DealerDomain",
                     "DealerId"]
    display_columns = ["Dealer Name", "State", "City", "Latitude", "Longitude", "Street Number", "Street Name",
                       "Zip Code", "Dealer Domain", "Dealer ID"]

    def __init__(self, name="dealer mapping", toreview="DealerMapping", reviewed="DealerMappingReviewed",
                 user_reviewed="UserReviewed", votes_per_record=1):
        self.name = name
        self.toreview = toreview
        self.reviewed = reviewed
        self.user_reviewed = user_reviewed
        self.votes_per_record = votes_per_record

    def get_one_record_not_reviewed(self):

        display_dict = dict(zip(DealerMapping.order_columns, DealerMapping.display_columns))

        values = DataBase.find_one_not_reviewed(
            user_id=User.find_by_email(session['email']).userid
        )
        if values is None:
            return None
        record_id = values['RecordId']
        mc_columns, pc_columns = {}, {}
        mc_dealerid, pc_dealerid = None, None

        for column, value in values.items():
            if column.startswith("MC"):
                mc_columns[column[3:]] = value
            if column.startswith("PC"):
                pc_columns[column[3:]] = value
            if column == "MC_DealerId":
                mc_dealerid = value
            if column == "PC_DealerId":
                pc_dealerid = value

        mc_pc_columns_dict = {key: (mc_columns.get(key, ''), pc_columns.get(key, ''))
                              for key in set().union(mc_columns, pc_columns)}
        mc_pc_columns = [(display_dict[col], mc_pc_columns_dict[col])
                         for col in DealerMapping.order_columns if col in mc_pc_columns_dict]

        return {'mc_pc_columns': mc_pc_columns, 'mc_dealerid': mc_dealerid,
                'pc_dealerid': pc_dealerid, 'record_id': record_id}

    def save_to_db(self, user_id, record_id, match,
                   time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')):
        cursor = DataBase.get_connection_default().cursor()
        cursor.execute("SELECT ViewCounts FROM {table} where RecordId={record_id}".format(
            table=self.toreview, record_id=record_id
        ))
        reviewed_times = cursor.fetchone()[0]

        if reviewed_times < self.votes_per_record - 1:

            cursor.execute("UPDATE {table} SET ViewCounts={counts} where RecordId={record_id}".format(
                table=self.toreview, counts=reviewed_times+1, record_id=record_id
            ))
        else:

            cursor.execute("INSERT INTO {reviewed} SELECT * FROM {toreview} "
                           "WHERE RecordId={record_id}".format(
                reviewed=self.reviewed, toreview=self.toreview, record_id=record_id
            ))

            cursor.execute("DELETE FROM {toreview} WHERE RecordId={record_id}".format(
                toreview=self.toreview, record_id=record_id
            ))

        cursor.execute("INSERT INTO {user_reviewed} (UserId, RecordId, Match, Time) Values"
                       "({user_id}, {record_id}, {match}, '{time}')".format(
            user_reviewed=self.user_reviewed, user_id=user_id, record_id=record_id, match=match, time=time
        ))

    def get_statistics(self, user_id):

        cursor = DataBase.get_connection_default().cursor()

        cursor.execute("select count(*) from {toreview}".format(toreview=self.toreview))
        records_to_review = cursor.fetchone()[0]

        cursor.execute("select count(*) from {user_reviewed} where UserId={user_id}".format(
            user_reviewed=self.user_reviewed, user_id=user_id
        ))
        user_reviewed = cursor.fetchone()[0]

        cursor.execute("SELECT * FROM ("
                       "SELECT UserId, Counts, Rank() OVER (ORDER BY Counts DESC) AS rank "
                       "FROM ("
                       "SELECT UserId, count(*) AS Counts "
                       "FROM {user_reviewed} "
                       "GROUP BY UserId) i) j "
                       "WHERE UserId={user_id}".format(
            user_reviewed=self.user_reviewed, user_id=user_id
        ))
        rank = cursor.fetchone()

        return {'records_to_review': records_to_review,
                'user_reviewed': user_reviewed,
                'rank': -1 if rank is None else rank[0]}





