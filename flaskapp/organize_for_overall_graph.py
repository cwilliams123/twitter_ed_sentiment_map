import pandas as pd 
import sqlite3 as db
import numpy as np
import us


database = "all_data"
#type of the data is always cc (#commoncore) or ccr ("college and career readiness")
type_data = "ccr"
table_from = type_data
table_to = type_data + "_time_aggregated"

con = db.connect(database)
c = con.cursor()
old_df = pd.read_sql("SELECT * from " + table_from, con)

start_time = "2012"
end_time = "2020"

#make the cols for the time aggregated sentiment df
time_ag_df = pd.DataFrame(
	index = pd.date_range(start=start_time, end=end_time, freq = "M").to_period("M"), 
	columns = ['sentiment'])

#make sure we're using full tweets (not deleted ones)
old_df = old_df[old_df['deleted'] == 0]

old_df['created_at'] = pd.to_datetime(old_df['created_at']).dt.to_period('M')

#and then of the full tweets, make sure there aren't duplicates; since Prof. Rice's data doesn't have 
#indices we just check time created and text of the tweet to see if they're the same
old_df.drop_duplicates(subset=['created_at', 'tweet'], keep='first', inplace=True)

time_ag_df['sentiment'] = old_df.groupby(['created_at'])['sentiment_textblob'].mean()

c.execute("DROP TABLE IF EXISTS %s" % table_to)
time_ag_df.to_sql(table_to, con, index= False)
con.close()

time_ag_df.to_csv(type_data + "_overall_month.csv")
