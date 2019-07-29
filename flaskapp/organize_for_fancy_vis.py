import pandas as pd 
import sqlite3 as db
import numpy as np
import us

def get_abbr(state):
	return str(us.states.lookup(state).abbr)

database = "all_data"
type_data = "ccr"
table_from = type_data
table_to = "ccr_reorganized"
limit = 500

con = db.connect(database)
c = con.cursor()
old_df = pd.read_sql("SELECT * from " + table_from, con)

start_time = "2012"
end_time = "2020"

states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado",
	"Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois",
	"Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
	"Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
	"Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
	"North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
	"Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
	"Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming", 
	'District of Columbia']

date_range = pd.date_range(start=start_time, end=end_time, freq="MS").to_period("M")

big_time_state_df = pd.DataFrame( 
	columns = ['date', 'state', 'sentiment', 'frequency'])


old_df = old_df[old_df['deleted'] == 0]


old_df['created_at'] = pd.to_datetime(old_df['created_at']) #make this time downloaded?
old_df['created_at'] = old_df['created_at'].dt.to_period("M")

for state in states:
	time_state_df = pd.DataFrame(
	index = date_range, 
	columns = ['date', 'state', 'sentiment', 'frequency'])
	time_state_df['date'] = date_range
	time_state_df['state'] = state
	time_state_df['frequency'].values[:] = 0
	groupby = old_df[old_df['state'] == state].groupby(['created_at'])['sentiment_textblob']
	time_state_df['sentiment'] = groupby.mean()
	time_state_df['frequency'] = groupby.size()
	time_state_df.loc[time_state_df['frequency'].isna(), 'frequency'] = 0
	big_time_state_df = big_time_state_df.append(time_state_df, ignore_index = True)

big_time_state_df.insert(0, 'year', big_time_state_df['date'].dt.year.apply(str))
big_time_state_df.insert(1, 'month', big_time_state_df['date'].dt.month.apply(str))
big_time_state_df = big_time_state_df.drop(['date'], axis = 1)
big_time_state_df['state'] = big_time_state_df['state'].apply(get_abbr)

c.execute("DROP TABLE IF EXISTS %s" % table_to)
big_time_state_df.to_sql(table_to, con, index= False)

con.close()