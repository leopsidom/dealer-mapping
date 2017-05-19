from flask import Blueprint, render_template, request, session, redirect
from flask import url_for

from src.common.database import DataBase
from src.models.dealer_mapping.dealer_mapping import DealerMapping
from src.models.users.user import User
import src.models.users.constants as UserConstants

project_blueprint = Blueprint('dealer_mapping', __name__)


@project_blueprint.route('/', methods=['GET'])
def list_of_projects():
    if not session.get('email'):
        return redirect(url_for("users.login_user"))
    user_id = User.find_by_email(session['email']).userid
    stats = DealerMapping().get_statistics(user_id)
    stats.update({'email': session['email']})
    return render_template('dealer_mapping/dealer_mapping_home.html', stats=stats)


@project_blueprint.route('/label', methods=['GET', 'POST'])
def pair_of_dealers():
    dm = DealerMapping()
    if request.method == 'GET':
        record = dm.get_one_record_not_reviewed()
        if record is None:
            return "You have completed the list. Thanks for the reviewing!"
        return render_template("dealer_mapping/mc_pc_pair.html",
                               mc_pc_columns=record['mc_pc_columns'],
                               mc_dealerid=record['mc_dealerid'],
                               pc_dealerid=record['pc_dealerid'],
                               record_id=record['record_id'])

    user_id = User.find_by_email(session['email']).userid
    record_id = request.form['record_id']
    match = request.form['options']
    print(match)
    if match == "Match":
        match = 1
    elif match == "NotMatch":
        match = 0
    elif match == "TBD":
        match = -1
    else:
        return "You must choose an option!"

    dm.save_to_db(user_id=user_id, record_id=record_id, match=match)
    return redirect(url_for('.pair_of_dealers'))






