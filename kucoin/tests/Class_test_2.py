file_string = 'C:/Users/jocox/Dropbox/Teaching/Spring 2022/home_runs.csv'

import csv

with open(file_string, "r") as input_file:
    reader = csv.reader(input_file)
    for row in reader:
        print(row[0], row[1], row[2])
