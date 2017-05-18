from src.app import app

__author__ = 'xiaoyun'

app.run(debug=app.config['DEBUG'], port=4990)