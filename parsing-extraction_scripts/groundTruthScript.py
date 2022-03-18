import os
import csv

'''
Given a root directory, this script scans through all sub-folders to find the 'right.csv' files and then formats them together such that they can be used in BareTQL
'''
FILE_NAME = "right.csv"
OUTPUT_NAME = "..\\program\\data\\AFJ_DirtyTables.txt"

output_array = []
num_tables = 0
total_rows = 0
total_columns = 0

def writeOutput():
	if num_tables > 0:
		try:
			with open(OUTPUT_NAME, "w", encoding="UTF-8") as output:
				header = "{numTables} {avgRows:.3f} {avgColumns:.3f}\n\n".format(numTables=num_tables, avgRows=(total_rows/num_tables), avgColumns=(total_columns/num_tables))
				output.write(header)
				for table in output_array:
					for row in table:
						firstEntry = True
						for entry in row:
							if firstEntry:
								output.write("\"" + entry + "\"")
								firstEntry = False
							else:
								output.write(",\"" + entry + "\"")
						output.write("\n")
					output.write("\n")
			print("Successfully wrote to file.")
		except Exception as e:
			print("Error: Could not write to " + OUTPUT_NAME + ".")
			print(e)
	else:
		print("Error: No tables to write.")

def readFile(file, printLine):
	global num_tables
	global total_columns
	global total_rows
	num_tables += 1
	reader = csv.reader(file)
	table = []
	first_row = True
	for row in reader:
		if printLine:
			print(row)
		if first_row:
			total_columns += len(row) - 1
			first_row = False
		else:
			total_rows += 1
			table.append(row)
	output_array.append(table)

def findFile(folder):
	fileName = folder + "\\" + FILE_NAME
	if 'fruit' in fileName:
		printLine = True
	else:
		printLine = False
	try:
		f = open(fileName, "r", encoding="UTF-8")
		readFile(f, printLine)
	except Exception as e:
		#print(e)
		pass

def main():
	rootDir = input("Enter name of root directory (in relation to this file):")
	cur_path = os.path.dirname(__file__)
	new_path = os.path.relpath(rootDir, cur_path)
	subFolders = [x[0] for x in os.walk(new_path)]
	for sub in subFolders:
		findFile(sub)
	writeOutput()

main()