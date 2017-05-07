from python_scripts.read_data import *
import pprint

if __name__ == "__main__":
    votes = read_zip('data/2016GEN.zip', '2016vote.txt')
    cities = read_zip('data/2016GEN.zip', '2016city.txt')
    offices = read_zip('data/2016GEN.zip', '2016offc.txt')
    name = read_zip('data/2016GEN.zip', '2016name.txt')
    print(list(votes))
    print(list(cities))
    print(list(offices))
    print(list(name))
