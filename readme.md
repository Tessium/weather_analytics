Initialization:
	Run python weather.py in order to create/initialize tables. Further runs will update tables.
	P.s. It takes time due to the fact that, we have 5 cities and it updated for every 1 minute of the last 10 minutes. So that, 10 * 5 = 50 requests in total.

Usage: 
	python weather.py
		Will initialize or update database

	python weather.py --city_id <id>
		Shows weather info for the last 10 minutes of given city

	python weather.py --fname <filename>
		Exports contents of db to <filename> csv file. (Both cities and weather data)

