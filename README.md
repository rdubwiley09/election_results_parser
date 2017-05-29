# election_results_parser
Load:

description: Takes the zipped tab delimited files and uploads them to a database of your choosing
  
The files can be found here: http://miboecfr.nictusa.com/cgi-bin/cfr/precinct_srch.cgi?elect_year_type=2016GEN&county_code=00&Submit=Search

Replace the year with your even year of choice. The Michigan SOS does not have the same format for off-year elections.
Place these files in the data folder within the load directory.

To point at a database, create a python file within the load directory called "server_config.py."
This folder requires three variables: user, ip, pwd.

When these are entered, run run_scripts.py and the necessary load transformations will load the necessary tables to your database

API:

description: Runs a hug api to serve election data

Create a server_config file like you did above for the load directory with the same variables.
This is needed to grab the data from the database.

To run the api locally, run "hug -f api.py" within the api directory.
This will initiate an api running on port 8000.

If you go to localhost:8000, you will find all the routes with the given hug documentation
