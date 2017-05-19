from flask import Flask
from flask import render_template
import os, requests


from src.common.database import DataBase

app = Flask(__name__)
app.secret_key = "123"


@app.before_first_request
def set_proxy():
    os.environ['http_proxy'] = os.environ.get('FIXIE_URL', '')
    os.environ['https_proxy'] = os.environ.get('FIXIE_URL', '')


@app.route('/')
def home():
    print("Http proxy: ", os.environ['http_proxy'])
    cur = DataBase().get_connection_default().cursor()

    cur.execute("select table_name from information_schema.tables")
    tables = cur.fetchall()

    return render_template("base.html", tables=tables)


from src.models.users.views import user_blueprint
from src.models.dealer_mapping.views import project_blueprint
app.register_blueprint(user_blueprint, url_prefix="/users")
app.register_blueprint(project_blueprint, url_prefix="/dealer_mapping")


if __name__ == "__main__":
    app.run()
