import os

DEBUG = False
ADMINS = frozenset([os.environ.get("EMAIL")])
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))