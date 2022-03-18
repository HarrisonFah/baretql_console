import sys
import os
import re
import time
import math
import configparser
import re
import traceback
import baretql
from R import R

num_tables = 0

def print_title():
    print("  ____              _______ ____  _      ")
    print(" |  _ \\            |__   __/ __ \\| |     ")
    print(" | |_) | __ _ _ __ ___| | | |  | | |     ")
    print(" |  _ < / _` | '__/ _ \\ | | |  | | |     ")
    print(" | |_) | (_| | | |  __/ | | |__| | |____ ")
    print(" |____/ \\__,_|_|  \\___|_|  \\___\\_\\______|")
    print("\n")

def main():
    '''
    Main function that begins program.
    '''
    print_title()

    # Extract global parameters. If set_vars returns false then end the
    # program.
    if not baretql.set_vars():
        return
    # Extract file from command line
    if (len(sys.argv) == 1):
        print("Enter name of table file (with extension):")
        fileName = input(">").strip()
        while (not baretql.parse_file(fileName)):
            print("File name or file contents invalid, enter name of table file (with extension):")
            fileName = input(">")
    else:
        filename = sys.argv[1]
        # Parse file entered in command line. If parse_file returns false then
        # end the program.
        if baretql.parse_file(filename) == False:
            return

    # Welcome print statements
    print(
        "Default parameters are set in 'config.ini' and can be modified\n"
        + "using the 'set' command. Enter 'set' for more info on the set\n"
        + "command.")
    print('Type "help" if you need help.\n')
    print(">", end="", flush=True)

    # Determine whether input is through terminal or pipe
    if os.isatty(0):
        pipe = False
    else:
        pipe = True

    # Iterate over each line of user input
    for line in sys.stdin:
        line = line.strip(" \n")
        lower = line.lower()

        if pipe:
            print(line)

        # Begin timer
        start_time = time.time()

        # If blank line, exit program
        if line == '':
            print(">", end="", flush=True)
            continue

        # Set command 
        elif lower[:4] == "set " or lower == "set":
            baretql.set_parameters(lower)

        # Help command
        elif lower == "help":
            print_help_message()

        # Print variables command
        elif lower == "params":
            baretql.print_parameters()

        elif lower == "quit":
            return

        # TODO Maybe change to 'elif line != ''', remove above 'if'? 
        else: 
            # Assignment operator in input
            if line.find("=") != -1:
                line = line.split("=")
                table_name = line[0].strip()
                dot_op = baretql.handle_dot_ops(line[1].strip())
                search_bool, search_result = baretql.handle_search(line[1].strip())
                # Assign the table to the result of the dot operation if 
                # a table was returned
                if type(dot_op) != bool:
                    baretql.user_inp_tables[table_name] = dot_op
                elif search_bool:
                    if search_result is not None:
                        baretql.user_inp_tables[table_name] = search_result
                    else:
                        print("Cannot assign " + table_name + " to 0 or >1 tables.")
                elif len(line[1].strip(" ")) >= 4 and line[1].strip(" ")[0:4] == "join":
                    handle_join_command(line[1].strip(" "), table_name=table_name)
                # dot_op returns false if there were no dot operation
                # succesfully performed.
                elif not dot_op and not search_bool and line[1].lower().strip(" ")[0:4] == "open":
                    # If no dot operation, the user is potentially
                    # initializing a user inputed table
                    formats = ["open\(\"(.*)\",\s*\"(.*)\"\)", "open\(\"(.*)\"\)"]
                    correct_format = False
                    for re_format in formats:
                        result = re.search(re_format, line[1].lower().strip(" "))
                        if result is not None:
                            correct_format = True
                            try:
                                #if the user used open() to access a table file
                                filename = result.group(1)
                                try:
                                    header_char = result.group(2)
                                    if header_char.lower().strip(" ") == "y":
                                        header = True
                                    else:
                                        header = False
                                except:
                                    header = False
                                table = baretql.import_table(filename, header)
                                if table is not None:
                                    baretql.user_inp_tables[table_name] = R(table)
                                    print(baretql.user_inp_tables[table_name])
                                else:
                                    print("Error: File does not exist.")
                            except:
                                pass
                            break
                    if not correct_format:
                        print("Open command must be formatted as:")
                        print("\t-*table name* = open(\"*file name*\")")
                        print("\t-*table name* = open('*file name*')")
                else:
                    #if the user entered using stdin instead of file
                    table_name, table = baretql.extract_table(line)
                    # table_name = None if improper table intialization
                    if table_name != None:
                        # Determine if all keys are not empty
                        bad_key = False
                        for row in table:
                            key = row[0]
                            if key == '':
                                print("Table cannot contain a row with"
                                      + "an empty key.")
                                bad_key = True
                                break
                        if not bad_key:
                            baretql.user_inp_tables[table_name] = R(table)
                            print(baretql.user_inp_tables[table_name])
            else:
                dot_op = baretql.handle_dot_ops(line)
                search_bool, result = baretql.handle_search(line)
                # If no dot operation was performed then the user input
                # was invalid
                if not dot_op and not search_bool:
                    if len(lower) >= 6 and lower[0:6] == "import":
                        #create 'import' command for reading another database file into this database
                        if "\"" in lower:
                            result = re.search("import\(\"(.*)\"\)", lower)
                        elif "\'" in line:
                            result = re.search("import\('(.*)'\)", lower)
                        try:
                            filename = result.group(1).strip(" ")
                            baretql.parse_file(filename)
                        except:
                            print("Import command must be formatted as: ")
                            print("\t -import(\"*filename*\")")
                            print("\t -import(\'*filename*\')")
                    elif len(lower) >= 6 and lower[0:6] == "export":
                        #create 'export' command for writing this database to a new file
                        if "\"" in lower:
                            result = re.search("export\(\"(.*)\"\)", line)
                        elif "\'" in line:
                            result = re.search("export\('(.*)'\)", line)
                        try:
                            filename = result.group(1).strip(" ")
                            baretql.export_tables(filename, R.table_list)
                            print("Succesfully exported to " + filename)
                        except:
                            print("Export command must be formatted as: ")
                            print("\t -export(\"*filename*\")")
                            print("\t -export(\'*filename*\')")
                    elif len(lower) >= 8 and lower[0:8] == "joinable":
                        handle_joinable_command(line)
                    elif len(lower) >= 4 and lower[0:4] == "join":
                        handle_join_command(line)
                    else:
                        print("Unknown command or unknown table.")    
        
        # If verbose parameter set to true, display time it took for operation
        if baretql.verbose == "on":
            tot_time = time.time() - start_time
            print("Operation Time: %.3f" %tot_time)
        # End of operation print statements
        print("------------------------------\n")
        print(">", end="", flush=True)

def handle_join_command(line, table_name=None):
    '''
        Parses and performs a join command.

        Arguments:
            line: the line containing the join command
            table_name: the name of the table to assign the result to if specified
    '''
    #The possible formats of the join command
    formats = [
        "join\((.*),\s*\"(.*)\",\s*(.*),\s*\"(.*)\",\s*\"(.*)\"\)",
        "join\((.*),\s*\"(.*)\",\s*(.*),\s*\"(.*)\",\s*\'(.*)\'\)",
        "join\((.*),\s*\"(.*)\",\s*(.*),\s*\"(.*)\"\)"
    ]
    correct_format = False
    #check if the given line matches any of the formats
    for re_format in formats:
        result = re.search(re_format, line)
        if result is not None:
            try:
                #parse the parameters of the command
                try:
                    srcTable = baretql.user_inp_tables[result.group(1).strip(" ")]
                except:
                    print("Error: Src table does not exist.")
                    return
                srcColumns = result.group(2).strip(" ")
                if len(srcColumns) != srcTable.get_num_cols():
                    print("Error: Src column priority must contain a character for each column in the table.")
                    return
                try:
                    tgtTable = baretql.user_inp_tables[result.group(3).strip(" ")]
                except:
                    print("Error: Tgt table does not exist.")
                    return
                tgtColumns = result.group(4).strip(" ")
                if len(tgtColumns) != tgtTable.get_num_cols():
                    print("Error: Tgt column priority must contain a character for each column in the table.")
                    return

                srcTable, tgtTable, columnPairs = process_grouping(srcTable, srcColumns, tgtTable, tgtColumns)

                #import transformations from the file specified
                try:
                    transformationFile = result.group(5).strip(" ")
                    transformations = {}
                    for src_col in columnPairs:
                        tgt_col = columnPairs[src_col]
                        current_transformations, is_swapped = baretql.import_transformations(transformationFile, src_col, tgt_col)
                        if current_transformations is None:
                            return
                        transformations[src_col] = (current_transformations, is_swapped)
                except:
                    transformations = None

                #perform join
                joinedTables = baretql.joinTablesOnTransform(srcTable, tgtTable, columnPairs, transformations=transformations)
                print(joinedTables)
                if table_name != None:
                    baretql.user_inp_tables[table_name] = joinedTables
                correct_format = True
                break
            except Exception as e:
                print("Join command must be formatted as: ")
                print ("\t -join(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\")")
                print ("\t -join(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\", \"transformation file\")")                         
    if not correct_format:
        print("Join command must be formatted as: ")
        print ("\t -join(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\")")
        print ("\t -join(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\", \"transformation file\")")

def handle_joinable_command(line):
    exportFile = None
    if "." in line:
        #if the user is exporting to a file, separate entry into joinable command and export command
        joinPart = line.split(".",1)[0]
        endPart = line.split(".",1)[1]
        if "export" in endPart:
            endResult = re.search("export\(\"(.*)\"\)", endPart)
            try:
                exportFile = endResult.group(1).strip(" ")
            except:
                print("Error: export command formatted incorrectly.")
    else:
        joinPart = line
    try:
        joinResult = re.search("joinable\((.*),\s*\"(.*)\",\s*(.*),\s*\"(.*)\"\)", joinPart)
        if joinResult is not None:
            try:
                try:
                    srcTable = baretql.user_inp_tables[joinResult.group(1).strip(" ")]
                except:
                    print("Error: Src table does not exist.")
                    return
                srcColumns = joinResult.group(2).strip(" ")
                if len(srcColumns) != srcTable.get_num_cols():
                    print("Error: Src column priority must contain a character for each column in the table.")
                    return
                try:
                    tgtTable = baretql.user_inp_tables[joinResult.group(3).strip(" ")]
                except:
                    print("Error: Tgt table does not exist.")
                    return
                tgtColumns = joinResult.group(4).strip(" ")
                if len(tgtColumns) != tgtTable.get_num_cols():
                    print("Error: Tgt column priority must contain a character for each column in the table.")
                    return

                srcTable, tgtTable, columnPairs = process_grouping(srcTable, srcColumns, tgtTable, tgtColumns)

                #find the transformations for each column pair
                for src_col in columnPairs:
                    tgt_col = columnPairs[src_col]
                    tables = baretql.create_cm_table(srcTable, tgtTable, {src_col:tgt_col})
                    print_output = baretql.column_matcher(tables, exportFile=exportFile)
                    for line in print_output:
                        print(line, end = '')
            except Exception as e:
                print("Joinable command must be formatted as: ")
                print ("\t -joinable(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\")")
                print ("\t -joinable(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\").export(\"*filename*\")") 
        else:
            print("Joinable command must be formatted as: ")
            print ("\t -joinable(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\")")
            print ("\t -joinable(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\").export(\"*filename*\")")
    except ZeroDivisionError:
        print("Error: Tables do not join on given columns")
    except Exception as e:
        print("Joinable command must be formatted as: ")
        print ("\t -joinable(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\")")
        print ("\t -joinable(*srcTable*, \"*srcColumns*\", *tgtTable*, \"*tgtColumns*\").export(\"*filename*\")")

def process_grouping(srcTable, srcColumns, tgtTable, tgtColumns):
    '''
    Concatenates together columns to form new src and tgt tables as well as pairs column indexes by groups.

    Arguments:
        srcTable: The object of the src table
        srcColumns: A string of the src table alphanumeric column markers
        tgtTable: The object of the tgt table
        tgtColumns: A string of the tgt table alphanumeric column markers

    Returns:
        srcTable: The new src table with the columns concatenated together by markings
        tgtTable: the new tgt table with the columns concatenated together by markings
        columnPairs: A dictionary of src column:tgt column indexe pairings
    '''
    
    columnGroups = {}

    #group together columns by alphanumeric character
    srcColumnGroups = {}
    srcOptionalColumns = []
    for i in range(len(srcColumns)):
        currentVal = srcColumns[i]
        if currentVal.isalnum():
            if currentVal in srcColumnGroups:
                srcColumnGroups[currentVal].append(i)
            else:
                srcColumnGroups[currentVal] = [i]
        elif currentVal == '-':
            srcOptionalColumns.append(i)
        else:
            print("Error: Column priority characters must be alphanumeric or '-'")
            return
    #for each group, concatenate together columns
    for group in srcColumnGroups:
        if len(srcColumnGroups[group]) > 1:
            srcColumn = min(srcColumnGroups[group])
            srcTable = baretql.concatenate_columns(srcTable, srcColumnGroups[group])
        else:
            srcColumn = srcColumnGroups[group][0]
        columnGroups[group] = [srcColumn, None]

    #group together columns by alphanumeric character
    tgtColumnGroups = {}
    tgtOptionalColumns = []
    for i in range(len(tgtColumns)):
        currentVal = tgtColumns[i]
        if currentVal.isalnum():
            if currentVal in tgtColumnGroups:
                tgtColumnGroups[currentVal].append(i)
            else:
                tgtColumnGroups[currentVal] = [i]
        elif currentVal == '-':
            tgtOptionalColumns.append(i)
        else:
            print("Error: Column priority characters must be alphanumeric or '-'")
            return
    #for each group, concatenate together columns
    for group in tgtColumnGroups:
        if len(tgtColumnGroups[group]) > 1:
            tgtColumn = min(tgtColumnGroups[group])
            tgtTable = baretql.concatenate_columns(tgtTable, tgtColumnGroups[group])
        else:
            tgtColumn = tgtColumnGroups[group][0]
        try:
            columnGroups[group][1] = tgtColumn
        except:
            print("Error: Column groups must match between tables.")
            return

    #pair together src and tgt columns
    columnPairs = {}
    for group in columnGroups:
        if columnGroups[group][1] == None:
            print("Error: Column groups must match between tables.")
            return
        columnPairs[columnGroups[group][0]] = columnGroups[group][1]

    return srcTable, tgtTable, columnPairs

def print_help_message():
    '''
    Function that displays a help message.
    '''
    print(
        "To initialize a relational table, type \n"
        + "'*table name* = ((*r1c1*, *r1c2*, ..., *r1cn*), "
        + "(*r2c1*, *r2c2*, ..., *r2cn*), ..., "
        + "(*rnc1*, *rnc2*, ..., *rncn*))'\n"
        + "e.g. R = ((John, 21), (Mary, 22), (Jeff, 20))\n"
        + "If you need to write a comma in a row entry, "
        + "encapsulate the row entry in\n"
        + "either quotation marks or apostrophes e.g.\n"
        + "R = ((\"Doe, John\", 21), ('Jane, Mary', 22), (Jeff, 20))\n\n"
        + "After creating a table you can:\n"
        + "    - find related tables by typing by entering '*tablename*.related'\n"
        + "    - find a particular related table by doing '*tablename*.related.i'\n"
        + "      where i is index between 1 and the number of related tables\n"
        + "    - extend the rows of a table by entering '*tablename*.xr'\n"
        + "    - extend the columns of a table by entering '*tablename*.xc'\n"
        + "    - sort the table on any column by entering '*tablename*.sort(i)'\n"
        + "      or '*tablename*.sort(i, bool)' where i is an integer between 1\n"
        + "      and the number of columns in *tablename* and bool is an optional\n"
        + "      boolean indicating if the table should be sorted in descending\n"
        + "      order. Table is sorted in ascending order by default\n"
        + "    - Swap two columns in a table by entering '*tablename*'.swap(i, j)\n"
        + "      where i and j are integers between 1 and the number of columns\n"
        + "      in *tablename*\n"
        + "    - insert a row into the table by entering\n"
        + "      '*tablename*.insert(*r1c1*, *r1c2*, ..., *r1cn*)'\n"
        + "    - delete a row from the table by entering\n"
        + "      '*tablename*.insert(*r1c1*, *r1c2*, ..., *r1cn*)' provided\n"
        + "      this row is already in *tablename*\n"
        + "    - delete a row from the table by entering\n"
        + "      '*tablename*.delrow(index)' where index is an integer between\n"
        + "      1 and the number of rows in *tablename*\n"
        + "    - delete a column from the table by entering\n"
        + "      '*tablename*.delcol(index)' where index is an integer between\n"
        + "      1 and the number of columns in *tablename*\n\n"
        + "You are able to combine multiple related, related.i, xr and xc\n"
        + "operations into one. e.g. R.related.2.xr.xc\n\n"
        + "You can also use the following commands:\n"
        + "    - keyword_search('*search terms*', *number of results*)\n"
        + "    - keyword_search('*search terms*')\n"
        + "    - import('*filename*')\n"
        + "    - export('*filename*')\n\n"
        + "Enter 'set' for information on setting the parameters of the\n"
        + "program. Enter 'params' to find out what the value of each\n"
        + "parameter is currently.\n"
        + "Type 'quit' to exit the console.")

main()
