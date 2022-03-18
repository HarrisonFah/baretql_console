import pickle
import math
import json
from R import R
'''
Function used in order help with splitting a string on commas
but not if the comma is in between quotation marks or apostrophes.
This function concatenates the split up portion of a string if it 
had quotation marks or apostrophes and a comma.
@params entries A list that was a string split on commas
@params newEntry The concatenated entry 
@params ind The index of the string that has been potentially 
    split up
@params separator Indicates if it was either an apostrophe or a
    quotation mark used to encapsulate the entry
@return Returns the index from where to continue the parsing 
    of the user inputed entries
'''
def findCorrespondingPair(entries, newEntry, ind, separator):
    entry = entries[ind]
    if entries[ind][-1] != separator:
        for j in range(ind+1, len(entries)):
            entry += "," + entries[j]
            try:
                if entries[j][-1] == separator:
                    break
            except IndexError:
                continue
            
    newEntry.append(entry.strip(separator))
    try:
        return j
    except NameError:
        return ind
'''
Function used in order to extract row entries from an inputed row.
@params entries A row input that was split upon ","
@return Returns a tuple of the inputed row without quotations marks
    and where each entry is separated by a ","
'''
def extractEntries(entries):
    newEntry = []
    i = 0
    while i < len(entries):
        entries[i] = entries[i].strip(" ")
        if len(entries[i]) == 0:
            newEntry.append(entries[i])

        elif entries[i][0] == '"':
            if entries[i][-1] != '"':
                i = findCorrespondingPair(entries, newEntry, i, '"')
            else:
                newEntry.append(entries[i][1:-1])
        elif entries[i][0] == "'":
            if entries[i][-1] != "'":
                i = findCorrespondingPair(entries, newEntry, i, "'")
            else:
                newEntry.append(entries[i][1:-1])            
        else:
            newEntry.append(entries[i].strip(' "\''))
        i += 1
    return tuple(newEntry)


def buildIndex(filename):
    invertedIndex = {}
    #tables_list = []
    thresholds = [10, 25, 50, 75, 90, 100]

    # https://stackoverflow.com/a/6475407
    # accessed on 2018-07-04
    try:
        listFileName = filename.rsplit('.', 1)[0] + ".file"
        listFile = open(listFileName, 'w', encoding="UTF-8")

        with open(filename, "r", encoding="UTF-8") as infile:
            print("\nParsing file '%s'." %(filename))
            print("This may take a while if the file is large.")
            table = '' 
            header = False
            tableCount = False
            i = 0
            for line in infile:
                # Handle file description
                if not header:
                    header = True
                    line = line.strip('\n').split()
                    try:
                        numTables = int(line[0])
                        tableCount = True
                        avgRowLen = float(line[1])
                        avgColLen = float(line[2])

                    except (ValueError, IndexError):
                        if tableCount == False:
                            print("\nNo table count description at the top of the text file.")
                    continue

                if line == '\n':
                    if table == '':
                        continue
                    table = table.strip('\n')
                    #print("NEW\n" + table)
                    table_list = []
                    for entry in table.split("\n"):
                        entry = extractEntries(entry.split(","))
                        key = entry[0]
                        if key in invertedIndex:
                            invertedIndex[key].append(i)
                        else:
                            invertedIndex[key] = [i]
                        table_list.append(tuple(entry))
                    listFile.write(str(table_list) + "\n")
                    #tables_list.append(tuple(table_list))
                    i += 1
                    if tableCount:
                        score = math.floor((i / numTables) * 100)
                        if score in thresholds:
                            print("%d%% of the tables parsed..." %score)
                            thresholds.remove(score)
                    else:
                        if (i % 100000 == 0) and i != 0:
                            print("%d tables parsed..." %i)

                    table = ''
                    
                else:
                    table += line
        listFile.close()
    except FileNotFoundError:
        print("\nFile '%s' does not exist." %filename)
        return False

    print("\nParsing complete.")

    # https://stackoverflow.com/a/11027069
    # accessed on 2018-07-31    
    idxFileName = filename.rsplit('.', 1)[0] + ".idx"
    with open(idxFileName, 'w', encoding="UTF-8") as idxFile:
        for key in invertedIndex:
            entryString = str(key) + ":" + str(invertedIndex[key]) + "\n"
            idxFile.write(entryString)
    
    #NEEDED?
    #numTables = len(r_table_list)
    return True

def readIndex(filename):
    # https://stackoverflow.com/a/11027069
    # accessed on 2018-07-31
    try:
        idxFileName = filename.rsplit('.', 1)[0] + ".idx"
        with open(idxFileName, 'r', encoding="UTF-8") as idxFile:
            invertedIndex = {} 
            for line in idxFile:
                entry = line.strip("\n").rsplit(":", 1)
                key = entry[0]
                indxs = json.loads(entry[1])
                invertedIndex[key] = indxs
            print("Finished reading inverted index.")

        listFileName = filename.rsplit('.', 1)[0] + ".file"
        with open(listFileName, 'r', encoding="UTF-8") as listFile:
            r_table_list = []
            for line in listFile:
                table_list = eval(line.strip("\n"))
                table = R(*table_list)
                r_table_list.append(table)

        if type(invertedIndex) == dict:
            return invertedIndex, r_table_list
        else:
            print("Type is not dictionary")
            return False, None
    except FileNotFoundError:
        return FileNotFoundError

def testIndexing():
    import time
    start = time.time()
    filename = "data/WDCTables.txt"
    #buildIndex(filename)
    idx, table_list = readIndex(filename)
    #print(idx)
    #print(table_list)
    print(time.time() - start)

if __name__ == "__main__":
    testIndexing()