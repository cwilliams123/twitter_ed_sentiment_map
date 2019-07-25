import csv
import sqlite3
import pandas as pd
import json

from flask import Flask, request, g, render_template
app = Flask(__name__)


DATABASE = 'all_data'

app.config.from_object(__name__) #check what this does

def connect_to_database():
    return sqlite3.connect(app.config['DATABASE'])

def get_db():
    db = getattr(g, 'db', None) #???
    if db is None:
        db = g.db = connect_to_database()
    return db

@app.teardown_appcontext #???
def close_connection(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()
    print('torndown')

def execute_query(query, args=()):
	df = pd.read_sql(query, get_db())
	return df

@app.route('/')
def introduction():
	df = execute_query("""SELECT * FROM cc_counts LIMIT 15""")
	chart_data = df.to_dict(orient='records')
	chart_data = json.dumps(chart_data, indent=2)
	data = {'chart_data': chart_data}
	return render_template("app.html", data=data)

# @app.route("/viewdb")
# def viewdb():
#     result = execute_query("""SELECT * FROM cc_counts LIMIT 15""")
#     str_rows = [','.join(map(str, row)) for row in result]
#     cur.close()
#     header = 'time,count\n'
#     return header + '\n'.join(str_rows)
    

if __name__ == '__main__':
   app.run()