import sqlite3
import requests as re
import pandas as pd
import time
import json
import sys, getopt

DB = "data.db"


def create_connection():
	global DB
	conn = None

	try:
		conn = sqlite3.connect(DB)
		return conn
	except Error as e:
		print(e)
 
	return conn


def create_populate_db(conn):
	c = conn.cursor()
	c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='cities' ''')
	
	if c.fetchone()[0] != 1:
		table_sql = """ 
		CREATE TABLE cities (
		id integer PRIMARY KEY,
		name text NOT NULL,
		lat text,
		lon text
		); """

		c.execute(table_sql)
		conn.commit()

		populate_data = [
			[1, 'New York', '40.730610', '-73.935242'], 
			[2, 'Moscow', '55.751244', '37.618423'], 
			[3, 'London', '51.509865', '-0.118092'], 
			[4, 'Tashkent', '41.311081', '69.240562'], 
			[5, 'Beijing', '39.916668', '116.383331']
		]
		
		c.executemany('INSERT INTO cities values(?, ?, ?, ?);', populate_data);
		conn.commit()
		
	table_sql = """ 
	CREATE TABLE IF NOT EXISTS weather (
	city_id integer,
	summary text,
	windSpeed text,
	temperature text,
	uvIndex text,
	visibility text,
	_time text,

	FOREIGN KEY(city_id) REFERENCES cities(id)
	); """

	c.execute(table_sql)
	conn.commit()



def get_weather(conn):
	url = 'https://api.darksky.net/forecast/b2673d7ec1034d82a08ce7e90f0926f5/{},{},{}'
	
	# get cities in dataframe
	df = pd.read_sql_query("SELECT * FROM cities", conn)

	# getting current time and looping for every 60 in last 10 mins 
	ts = int(time.time())
	last_mins = [ts - i * 60 for i in range(1, 11)]

	for index, row in df.iterrows():
		city_id = row['id']
		for i in last_mins:
			full_url = url.format(row['lat'], row['lon'], i)
			response = re.get(full_url)
			data = json.loads(response.text)
			data = data['currently']
			sql = 'insert into weather values(?, ?, ?, ?, ?, ?, ?)'

			c = conn.cursor()
			c.execute(
				sql, [
					city_id,
					data['summary'], 
					data['windSpeed'], 
					data['temperature'], 
					data['uvIndex'], 
					data['visibility'],
					data['time']
				]
			)
			conn.commit()
	return True


def show_weather(city_id, conn):

	weather_df = pd.read_sql_query("SELECT * FROM weather where city_id={} order by _time desc limit 10".format(city_id), conn)

	print('Min temperature is: ', weather_df['temperature'].astype('float64').min())
	print('Max temperature is: ', weather_df['temperature'].astype('float64').max())
	print('Avg temperature is: ', weather_df['temperature'].astype('float64').mean())


def export_csv(fname, conn):
	df = pd.read_sql_query("SELECT * FROM weather order by _time desc", conn)
	df.to_csv (fname, index=False)
	print('Weather data exported to ', fname)

	df = pd.read_sql_query("SELECT * FROM cities", conn)
	df.to_csv('cities_' + fname, index=False)
	print('Cities data exported to ', 'cities_' + fname)


def main(argv):
	conn = create_connection()

	if conn:
		 create_populate_db(conn)

	city_id = None
	fname = None

	try:
		opts, args = getopt.getopt(argv,"hi:o:",["city_id=","fname="])
	except getopt.GetoptError:
		print('Usage: weather.py --city_id <id> --fname <file>')
		sys.exit(2)
	
	for opt, arg in opts:
		if opt == '--city_id':
			city_id = int(arg)
		elif opt == '--fname':
			fname = arg

	if city_id and fname:
		print('City ID is  ', city_id)
		print('Output file is ', fname)
		show_weather(city_id)
		export_csv(fname, conn)
	elif city_id:
		print('City ID is  ', city_id)
		show_weather(city_id, conn)
	elif fname:
		print('Output file is ', fname)
		export_csv(fname, conn)
	else:
		print('Updating data')
		get_weather(conn)
		print('Data updated')


if __name__ == '__main__':
	main(sys.argv[1:])