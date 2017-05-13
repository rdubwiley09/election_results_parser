import hug
from server_config import user,ip, pwd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import requests

@hug.get(examples="year=2016")
def offices(year: hug.types.text):
    """Returns all the offices for a given year"""
    engine = create_engine(
            'postgresql://%s:%s@%s/%s' %(user,pwd,ip,user),
            client_encoding='utf8',echo=False)
    conn = engine.connect()
    Base = declarative_base()
    query = "SELECT * FROM offices WHERE election_year = '%s'" %(str(year))
    df = pd.read_sql(query, conn)
    return df.reset_index().to_json(orient="records")

@hug.get(examples="year=2016&firstName=Ryan&lastName=Wiley")
def results(year: hug.types.text, firstName: hug.types.text, lastName: hug.types.text):
    """Returns the results for a given candidate for a given year"""
    engine = create_engine(
            'postgresql://%s:%s@%s/%s' %(user,pwd,ip,user),
            client_encoding='utf8',echo=False)
    conn = engine.connect()
    Base = declarative_base()
    query = "SELECT * FROM names WHERE election_year = '%s' AND candidate_first_name = '%s' AND candidate_last_name = '%s'" %(str(year),firstName.upper(),lastName.upper())
    df = pd.read_sql(query, conn)
    candidateId = df['candidate_id'].tolist()[0]
    resultQuery = "SELECT * FROM votes WHERE candidate_id = '%s';" %(str(candidateId))
    result = pd.read_sql(resultQuery,conn)
    officeId, districtId = result['office_code'].tolist()[0], result['district_code'].tolist()[0]
    totalQuery = "Select office_code, district_code, county_code, city_code, ward_number, precinct_number, SUM(precinct_votes) AS total_votes FROM votes WHERE office_code = '%s' AND district_code = '%s'  AND election_year = '%s' GROUP BY 1,2,3,4,5,6" %(str(officeId),str(districtId),str(year))
    totalTable = pd.read_sql(totalQuery,conn)
    output = pd.merge(result,totalTable, on = ['office_code', 'district_code', 'county_code', 'city_code', 'ward_number', 'precinct_number'], how="inner")
    output['candidate_percentage'] = 100*output['precinct_votes']/output['total_votes']
    return output.reset_index().to_json(orient="records")

@hug.get(examples="year=2016&city=Southfield")
def city(year: hug.types.text, city: hug.types.text):
    """Returns the results for a given candidate for a given year"""
    engine = create_engine(
            'postgresql://%s:%s@%s/%s' %(user,pwd,ip,user),
            client_encoding='utf8',echo=False)
    conn = engine.connect()
    Base = declarative_base()
    query = "SELECT * FROM cities WHERE election_year = '%s' AND city_description = '%s CITY' OR city_description = '%s TOWNSHIP'" %(str(year),city.upper(),city.upper())
    df = pd.read_sql(query, conn)
    cityId = df['city_code'].tolist()[0]
    resultQuery = "SELECT * FROM votes WHERE city_code = '%s';" %(str(cityId))
    result = pd.read_sql(resultQuery,conn)
    return result.reset_index().to_json(orient="records")
