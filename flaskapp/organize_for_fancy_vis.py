import pandas as pd 
import sqlite3 as db
import numpy as np
import us

def get_abbr(state):
	return str(us.states.lookup(state).abbr)

database = "all_data"
#type of the data is always cc (#commoncore) or ccr ("college and career readiness")
type_data = "cc"
table_from = type_data
table_to = type_data + "_reorganized"

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

#dataframe where sentiment's aggregated by date for each state
big_time_state_df = pd.DataFrame( 
	columns = ['date', 'state', 'frequency', 'sentiment'])

#make sure we're using full tweets (not deleted ones)
old_df = old_df[old_df['deleted'] == 0]



old_df['created_at'] = pd.to_datetime(old_df['created_at'])

#and then of the full tweets, make sure there aren't duplicates; since Prof. Rice's data doesn't have 
#indices we just check time created and text of the tweet to see if they're the same
old_df.drop_duplicates(subset=['created_at', 'tweet'], keep='first', inplace=True)


#"rounds" all the dates up to month-year for easier aggregation
old_df['created_at'] = old_df['created_at'].dt.to_period("M")

for state in states:
	#make a lil df for each state and add it to the big df
	time_state_df = pd.DataFrame(
	index = date_range, 
	columns = ['date', 'state', 'frequency', 'sentiment'])
	time_state_df['date'] = date_range
	time_state_df['state'] = state
	groupby = old_df[old_df['state'] == state].groupby(['created_at'])['sentiment_textblob']
	time_state_df['sentiment'] = groupby.mean()
	time_state_df['frequency'] = groupby.size()
	time_state_df.loc[time_state_df['frequency'].isna(), 'frequency'] = 0
	big_time_state_df = big_time_state_df.append(time_state_df, ignore_index = True)

#separate the date into a month col and a year col
big_time_state_df.insert(0, 'year', big_time_state_df['date'].dt.year.apply(str))
big_time_state_df.insert(1, 'month', big_time_state_df['date'].dt.month.apply(str))
big_time_state_df = big_time_state_df.drop(['date'], axis = 1)
big_time_state_df['state'] = big_time_state_df['state'].apply(get_abbr)

#save to db
c.execute("DROP TABLE IF EXISTS %s" % table_to)
big_time_state_df.to_sql(table_to, con, index= False)
con.close()

#save to csv
big_time_state_df.to_csv(type_data + "_data_for_vis.csv", index = False)
