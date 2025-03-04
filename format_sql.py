import csv
import sqlparse

input_file = '/Users/zww/workspace/codes/github/rust-practice/tspider-parser/error-sql-dedup.csv'
output_file = '/Users/zww/workspace/codes/github/rust-practice/tspider-parser/formatted-error-sql-dedup.csv'

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    for row in reader:
        formatted_sql = sqlparse.format(row[0], reindent=True, keyword_case='upper')
        writer.writerow([formatted_sql])
