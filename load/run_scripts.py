from load.python_scripts.read_data import build_dataframe
from load.python_scripts.load_data import load_all_tables, remove_duplicates

def generate_tables(year):
    votes = build_dataframe(year, 'votes')
    cities = build_dataframe(year, 'cities')
    offices = build_dataframe(year, 'offices')
    names = build_dataframe(year, 'names')
    counties = build_dataframe(year, 'counties')
    tables = {
        'votes': votes,
        'cities': cities,
        'offices': offices,
        'names': names,
        'counties': counties
    }
    return tables

def load_tables(tableArray):
    for i,year in enumerate(tableArray):
        print("Loading table %s of %s" %(str(i+1), str(len(tableArray))))
        if i == 0:
            #Replace existing tables
            load_all_tables(year,True)
        else:
            #Append existing tables
            load_all_tables(year,False)
    for key in tableArray[0].keys():
        if key != 'votes':
            remove_duplicates(key)

if __name__ == "__main__":
    table_2016 = generate_tables('2016')
    table_2014 = generate_tables('2014')
    table_2012 = generate_tables('2012')
    table_2010 = generate_tables('2010')
    table_2008 = generate_tables('2008')
    tables = [table_2016,
              table_2014,
              table_2012,
              table_2010,
              table_2008]
    load_tables(tables)
