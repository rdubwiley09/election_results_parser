import zipfile as zp
import pandas as pd

def read_zip(zip_loc,filename):
    with zp.ZipFile(zip_loc) as myzip:
        with myzip.open(filename) as myfile:
            return pd.read_csv(myfile, sep="\t", header=None)

def lookup_headers(name):
    if name == 'votes':
        return [
                'election_year',
                'election_type',
                'office_code',
                'district_code',
                'status_code',
                'candidate_id',
                'county_code',
                'city_code',
                'ward_number',
                'precinct_number',
                'precinct_label',
                'precinct_votes'
        ]
    elif name == 'cities':
        return [
                'election_year',
                'election_type',
                'county_code',
                'city_code',
                'city_description'
        ]
    elif name == 'offices':
        return [
                'election_year',
                'election_type',
                'office_code',
                'district_code',
                'status_code',
                'office_description'
        ]
    elif name == 'counties':
        return [
                'county_code',
                'county_name'
        ]
    elif name == 'names':
        return [
                'election_year',
                'election_type',
                'office_code',
                'district_code',
                'status_code',
                'candidate_id',
                'candidate_last_name',
                'candidate_first_name',
                'candidate_party_name'
        ]
    else:
        return "ERROR"

def assign_header(df, name):
    headers = lookup_headers(name)
    if headers != "ERROR":
        df.columns = headers
    else:
        print("%s is not a correct name" %(name))
    return df

def build_dataframe_from_file(zip_loc,filename,name):
    df = read_zip(zip_loc,filename)
    return assign_header(df,name)
