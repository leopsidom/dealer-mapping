import json
import uuid
from src.common.database import DataBase
import src.models.users.errors as UserErrors
from src.common.utils import Utils
import src.models.users.constants as UserConstants

__author__ = 'jslvtr'


class User(object):
    def __init__(self, email, password, name="", userid=None):
        self.email = email
        self.password = password
        self.name = name
        self.userid = uuid.uuid4().hex if userid is None else userid

    def __repr__(self):
        return "<User {}>".format(self.email)

    @classmethod
    def find_by_email(cls, email):

        return cls(**DataBase.find_one_default(UserConstants.COLLECTION, {"email": email}))

    @staticmethod
    def is_login_valid(email, password):
        """
        This method verifies that an e-mail/password combo (as sent by the site forms) is valid or not.
        Checks that the e-mail exists, and that the password associated to that e-mail is correct.
        :param email: The user's email
        :param password: A sha512 hashed password
        :return: True if valid, False otherwise
        """
        user_data = DataBase.find_one_default("users", {"email": email})  # Password in sha512 -> pbkdf2_sha512
        print(user_data)
        if user_data is None:
            # Tell the user that their e-mail doesn't exist
            raise UserErrors.UserNotExistsError("Your user does not exist.")
        if not Utils.check_hashed_password(password, user_data['password']):
            # Tell the user that their password is wrong
            raise UserErrors.IncorrectPasswordError("Your password was wrong.")

        return True

    @staticmethod
    def register_user(email, password, name = ""):
        """
        This method registers a user using e-mail and password.
        The password already comes hashed as sha-512.
        :param email: user's e-mail (might be invalid)
        :param password: sha512-hashed password
        :return: True if registered successfully, or False otherwise (exceptions can also be raised)
        """
        user_data = DataBase.find_one_default(UserConstants.COLLECTION, {"email": email})

        if user_data is not None:
            raise UserErrors.UserAlreadyRegisteredError("The e-mail you used to register already exists.")
        if not Utils.email_is_valid(email):
            raise UserErrors.InvalidEmailError("The e-mail does not have the right format.")

        User(email, Utils.hash_password(password), name).save_to_db()

        return True

    def save_to_db(self):
        DataBase.add_one_default(UserConstants.COLLECTION, self.json())

    def json(self):
        return {
            "email": self.email,
            "password": self.password,
            "Name": self.name
        }

    def get_alerts(self):
        pass