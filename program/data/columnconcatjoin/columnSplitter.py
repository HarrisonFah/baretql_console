'''
Scans a txt file with numerous tables separtated by newlines with columns separated by commas. 
Splits the specified columns for each table by a delimiter and stores this as the left table.
The table itself is stored as the right table.
'''

#The names of the folders for each table, the column to be split, the delimiter used for splitting, and the number of columns to be split into.
#Ordered by appearance of table in the text file.
split_info = [
["snl_hosts", 1, "/", 2],
["minnesota_politicians", 0, ", ", 2],
["chinese_cities", 0, " , ", 2],
["tennis_winners", 0, " ", 3],
["tour_dates", 0, " – ", 2],
["drug_names", 0, " (", 2],
["swimming", 0, " ", 2]
]

input_file = "wikiTableData2.txt"

def main():
	table_index = 0
	first_line = True
	with open (input_file, "r", encoding="UTF-8") as f:
		for line in f:
			if line == "\n":
				table_index += 1
				left.close()
				right.close()
				first_line = True
				continue
			if first_line:
				print("Reading/Writing " + split_info[table_index][0])
				with open(split_info[table_index][0] + "/metadata.txt", "w", encoding="UTF-8") as metadata:
					metadata.write("name: " + split_info[table_index][0] + "\n")
					metadata.write("column split: " + str(split_info[table_index][1]) + "\n")
					metadata.write("delimiter: '" + split_info[table_index][2] + "'\n")
					metadata.write("number of splits: " + str(split_info[table_index][3]) + "\n")
				left = open(split_info[table_index][0] + "/left.csv", "w", encoding="UTF-8")
				right = open(split_info[table_index][0] + "/right.csv", "w", encoding="UTF-8")
				first_line = False
			right.write(line)
			column = line.strip("\n").split('", ')[split_info[table_index][1]]
			split = column.split(split_info[table_index][2], split_info[table_index][3]-1)
			for col_index in range(len(split)-1):
				left.write('"' + split[col_index].strip('"') + '", ')
			left.write('"' + split[-1].strip('"') + '"\n')

main()



