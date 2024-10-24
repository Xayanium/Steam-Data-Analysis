# coding: utf-8
# @Author: Xayanium
import json

from flask import Flask, render_template, request, redirect, session
from utils.query import QueryData
from utils.get_data import get_table_data, get_search_data, get_analysis_data
from itertools import islice

# HbaseTableConn = None
# pool= hbase_connection()
query = QueryData()


app = Flask(__name__)
app.secret_key = 'hndxwcnm'

@app.route('/')
def index():
    return redirect('/login')


@app.route('/home')
def home():
    username = session['username']
    return render_template(
        'index.html',
        username=username,
        max_types=get_analysis_data('max_types'),
        max_prices=get_analysis_data('max_prices'),
        max_discount=get_analysis_data('max_discounts')
    )


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        form = dict(request.form)
        users = query.query_mysql('select * from user', [], 'select')

        def filter_user(item):
            return form['username'] in item and form['password'] in item

        login_success = list(filter(filter_user, users))
        if not len(login_success):
            return 'Invalid username or password'
        else:
            session['username'] = form['username']
            session['password'] = form['password']
            return redirect('/home')
    else:
        return render_template('pages-login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        form = dict(request.form)
        if form['username'] and form['password'] and form['confirm']:
            if form['password'] != form['confirm']:
                return 'Passwords do not match'
            else:
                users = query.query_mysql('select * from user', [], 'select')

                def filter_user(item):
                    return form['username'] in item and form['password'] in item

                filter_list = list(filter(filter_user, users))
                if len(filter_list):
                    return 'Username already exists'
                else:
                    query.query_mysql(
                        'insert into user (username, password) values (%s, %s)',
                        (form['username'], form['password']),
                        'insert'
                    )
        else:
            return 'Username or password is empty'
        return redirect('/login')

    else:
        return render_template(
            'pages-register.html',
        )


@app.route('/table', methods=['GET', 'POST'])
def table_view():
    username = session['username']
    # with pool.connection() as connection:
    #     table_data= get_table_data(connection.table("steam_data"))
    table_data = get_table_data(query)

    return render_template(
        'table-data.html',
        table_data=table_data,
        username=username
    )


@app.route('/search', methods=['GET', 'POST'])
def search_view():
    username = session['username']
    if request.method == 'POST':
        form = dict(request.form)
        # with pool.connection() as connection:
        #     data = get_search_data(connection.table("steam_data"), form['search-input'])
        data = get_search_data(query, form['search-input'])
        return render_template(
            'search-games.html',
            username=username,
            data=data
        )
    else:
        # with pool.connection() as connection:
        #     data = get_search_data(None)
        data = get_search_data(query, None)
        return render_template(
            'search-games.html',
            username=username,
            data=data
        )


@app.route('/price')
def price_view():
    username = session['username']
    return render_template(
        'price-analysis.html',
        username=username,
        prices=json.dumps(get_analysis_data('prices'))
    )


@app.route('/type')
def type_view():
    username = session['username']
    types: dict = get_analysis_data('types')
    types_part = dict(islice(types.items(), 20))
    return render_template(
        'type-analysis.html',
        username=username,
        types_part=json.dumps(types_part),
        types=json.dumps(types)
    )


@app.route('/review')
def review_view():
    username = session['username']
    return render_template(
        'review-analysis.html',
        username=username,
        reviews=get_analysis_data('reviews')
    )


@app.route('/year', methods=['GET', 'POST'])
def year_view():
    username = session['username']
    return render_template(
        'year-analysis.html',
        username=username,
        years=get_analysis_data('years')
    )

#
@app.route('/logout', methods=['GET', 'POST'])
def logout():
    session.clear()
    return redirect('/login')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

