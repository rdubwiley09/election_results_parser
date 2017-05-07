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
                'precinct_votes',
                'error_column'
        ]
    elif name == 'cities':
        return [
                'election_year',
                'election_type',
                'county_code',
                'city_code',
                'city_description',
                'error_column'
        ]
    elif name == 'offices':
        return [
                'election_year',
                'election_type',
                'office_code',
                'district_code',
                'status_code',
                'office_description',
                'error_column'
        ]
    elif name == 'counties':
        return [
                'county_code',
                'county_name',
                'error_column'
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
                'error_column',
                'candidate_party_name',
                'error_column_2'
        ]
    else:
        return "ERROR"

def assign_header(df, name):
    headers = lookup_headers(name)
    if headers != "ERROR":
        df.columns = headers
        if name == 'names':
            del df['error_column']
            del df['error_column_2']
            return df
        else:
            del df['error_column']
            return df
    else:
        print("%s is not a correct name" %(name))
        return df

def lookup_filename(year,name):
    if name == 'votes':
        return "%svote.txt" %(str(year))
    elif name == 'cities':
        return "%scity.txt" %(str(year))
    elif name == 'offices':
        return "%soffc.txt" %(str(year))
    elif name == 'counties':
        return "county.txt"
    elif name == 'names':
        return "%sname.txt" %(str(year))
    else:
        return "ERROR"

def build_dataframe(year,name):
    zip_loc = 'data/%sGEN.zip' %(str(year))
    filename = lookup_filename(year,name)
    df = read_zip(zip_loc,filename)
    return assign_header(df,name)
