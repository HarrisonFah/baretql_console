"""
This python script extracts the wikipedia tables based on the descriptions
 in 'listOfNotes.md'. 
"""

import sys
import re
import tables

def main():

    file = open('txtFiles/db.txt', 'w', encoding='UTF-8')

    DEBUG = any([arg.lower() == '-debug' for arg in sys.argv[1:]])
    lines = next((int(arg) for arg in sys.argv[1:] if arg.isnumeric()), 1) * 10 ** 6
    if any(arg.lower() == 'inf' for arg in sys.argv[1:]):
        lines = float('inf')

    title = None
    titleFlag = False
    tableFlag = False
    wikiTable = tables.WikiTable(file,DEBUG)
    count = 0

    # Pipe input from file (either cat or through terminal unzip)
    for line in sys.stdin:
        count += 1
        if count % 1000000 == 0:
            print(count // 1000000, "million lines read")
        if count == lines:
            break
        if re.match('\s*<title>', line):
            titleFlag = False
            tableFlag = False

        if re.match('\s*<title>List of', line):
            title = line.strip()
            titleFlag = True
        if titleFlag and re.search('wikitable', line):
            tableFlag = True
        
        if titleFlag and tableFlag:
            file.write(re.sub('<(\/)?title>', "", title).encode('utf-8', 'surrogateescape').decode('utf-8', 'replace') + '\n')
            wikiTable.readTable()
            tableFlag = False

    file.close()
    return

if __name__ == "__main__":
    main()