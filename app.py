# coding: utf-8
# @Author: Xayanium

from flask import Flask, render_template, request, redirect, session
from utils.query import query_mysql

app = Flask(__name__)
app.secret_key = 'hndxwcnm'


@app.route('/')
def index():
    return redirect('/login')

@app.route('/home')
def home():
    return render_template('index.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        form = dict(request.form)
        users = query_mysql('select * from user', [], 'select')

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
                users = query_mysql('select * from user', [], 'select')

                def filter_user(item):
                    return form['username'] in item and form['password'] in item

                filter_list = list(filter(filter_user, users))
                if len(filter_list):
                    return 'Username already exists'
                else:
                    query_mysql(
                        'insert into user (username, password) values (%s, %s)',
                        (form['username'], form['password']),
                        'insert'
                    )
        else:
            return 'Username or password is empty'
        return redirect('/login')

    else:
        return render_template('pages-register.html')


if __name__ == '__main__':
    app.run(debug=True)

