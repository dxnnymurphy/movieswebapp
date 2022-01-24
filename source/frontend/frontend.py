from source.backend.backend import *
from flask import Flask, render_template

#This file contains the front end aspect of the app. The functions render the html template, displaying data
# according to the query so the user can apply filters on the data

@app.route('/')
def show_10():
    return render_template('show_10.html', Films = Films.query.limit(10).all())

@app.route('/adult')
def adult():
    return render_template('adult.html', Films = Films.query.filter_by(adult = True).limit(10).all())

@app.route('/non_adult')
def non_adult():
    return render_template('non_adult.html', Films = Films.query.filter_by(adult = False).limit(10).all())

@app.route('/lessthananhour')
def less_than_an_hour():
    return render_template('lessthananhour.html', Films = Films.query.filter_by(runtime = '< 1 hr').limit(10).all())

@app.route('/onetotwohours')
def one_to_two_hours():
    return render_template('onetotwohours.html', Films = Films.query.filter_by(runtime = '1-2 hrs').limit(10).all())

@app.route('/overtwohours')
def over_two_hours():
    return render_template('overtwohours.html', Films = Films.query.filter_by(runtime = '> 2 hrs').limit(10).all())

@app.route('/american')
def american():
    return render_template('american.html', Films = Films.query.filter_by(american = True).limit(10).all())

@app.route('/non-american')
def non_american():
    return render_template('notamerican.html', Films = Films.query.filter_by(american = False).limit(10).all())

@app.route('/netflix')
def netflix():
    return render_template('netflix.html', Films = Films.query.filter_by(streaming_service = 'Netflix').limit(10).all())

@app.route('/amazonprime')
def amazon_prime():
    return render_template('amazon_prime.html', Films = Films.query.filter_by(streaming_service = 'Amazon Prime').limit(10).all())

@app.route('/disneyplus')
def disney_plus():
    return render_template('disney_plus.html', Films = Films.query.filter_by(streaming_service = 'Disney Plus').limit(10).all())