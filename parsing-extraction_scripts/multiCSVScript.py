import json
import sys
import string
import tarfile
import re
import os
import csv
from dateutil.parser import parse

punct_chars = string.punctuation + "– –"
ROOT_DIR = "..\\..\\AutomaticFuzzyJoin\\data"
FILE_NAME = "right.csv"

'''
Main function for parsing a 'WDC'.tar.gz file. This file's structure is
a tar file that consists of multiple files where each file contains
one table in json format.
'''
def main():
    cur_path = os.path.dirname(__file__)
    new_path = os.path.relpath(ROOT_DIR, cur_path)
    subFolders = [x[0] for x in os.walk(new_path)]

    out = open("AFJ_RightTables.txt", "w+", encoding="UTF-8")

    total_table_count = 0
    total_column_count = 0
    total_row_count = 0

    tables_removed_step_1 = 0
    columns_removed_step_1 = 0

    tables_removed_step_2 = 0
    key_cols_removed_step_2 = 0

    rows_removed_step_3 = 0

    columns_removed_step_4 = 0

    tables_removed_step_5 = 0

    
    # Initialize vars that'll report statistics at end of program
    table_count = 0
    row_size = []
    col_size = []

    # Iterate over each table
    for sub in subFolders:
        fileName = sub + "\\" + FILE_NAME
        try:
            f = open(fileName, "r", encoding="UTF-8")
        except Exception as e:
            #print(e)
            continue

        reader = csv.reader(f)

        # Extract the table data, number of rows and number of columns
        num_rows = 0
        num_cols = 0

        columns = []
        first_row = True
        for row in reader:
            if first_row:
                num_cols = len(row)
                first_row = False
                for i in range(len(row)):
                    columns.append([])
            else:
                num_rows += 1
                for i in range(len(row)):
                    columns[i].append(row[i])

        total_table_count += 1
        total_column_count += num_cols
        total_row_count += num_rows

        # Variable that will contain columns that are valid.
        # A column is valid if it is not empty, does not have an average
        # length longer than 60 characters, is not the same entry repeating
        # in every row and lastly is not completely composed of punctuation
        # characters. As well, the key column (1st column) cannot be the row
        # index for that row e.g. (Row 1 entry is 1, Row 2 entryis 2, ..., 
        # Row n entry is n)
        valid_cols = columns[:]

        # Remove invalid columns as described above
        for col in columns:
            if not is_valid_col(col):
                valid_cols.remove(col)

        columns_removed_step_1 += (num_cols - len(valid_cols))

        # If there are no more remaining columns, go to next table.
        if len(valid_cols) == 0:
            tables_removed_step_1 += 1
            continue                 

        # Make sure key column is valid      
        key_cols_removed_step_2 = validate_key_col(valid_cols, key_cols_removed_step_2)
        
        # If there are no more remaining columns, go to next table.
        if len(valid_cols) == 0:
            tables_removed_step_2 += 1
            continue 
        
        # Iterate over each row
        i = 0
        while i < num_rows:
            # If key column entry is empty, remove this row from the table.
            if valid_cols[0][i] == '':
                if i == 0:
                    for j in range(len(valid_cols)):
                        valid_cols[j] = valid_cols[j][1:]
                elif i == (num_rows - 1):
                    for j in range(len(valid_cols)):
                        valid_cols[j] = valid_cols[j][:-1]
                else:
                    for j in range(len(valid_cols)):              
                        valid_cols[j] = valid_cols[j][:i] + valid_cols[j][i+1:]
                rows_removed_step_3 += 1
                num_rows -= 1
            else:
                i += 1
        
        # Iterate over each column
        num_cols = len(valid_cols)
        i = 0
        while i < num_cols:
            # If column is empty or is all punctuation, remove column.
            if is_empty_col(valid_cols[i]) or is_punctuation_col(valid_cols[i]):
                valid_cols.remove(valid_cols[i])
                columns_removed_step_4 += 1
                num_cols -= 1
            else:
                i += 1
        
        # If the table has no more columns or has fewer than 3 rows, then
        # do not add this table to data file.
        if len(valid_cols) > 0 and num_rows > 3:
            out.write("\n")
            table_count += 1
            row_size.append(num_rows)
            col_size.append(len(valid_cols))
        else:
            tables_removed_step_5 += 1
            continue
        
        # Write table to file
        separator = '"'
        for i in range(num_rows):
            if valid_cols[0][i] == '':
                continue
            for j in range(len(valid_cols)):
                if valid_cols[j][i] == "None" or valid_cols[j][i] == "none":
                    valid_cols[j][i] = ''                
                elif valid_cols[j][i] != '':
                    if valid_cols[j][i][0] == '"':
                        valid_cols[j][i] == valid_cols[j][i][1:]
                    if valid_cols[j][i][-1] == '"':
                        valid_cols[j][i] == valid_cols[j][i][:-1]
                    valid_cols[j][i] = valid_cols[j][i].replace('"', "'")
                if j == 0:
                    out.write(separator + valid_cols[j][i] + separator)                       
                else:
                    out.write(', ' + separator + valid_cols[j][i] + separator)       
            out.write("\n")

        f.close()

    # Print out statistics          
    print(table_count)  
    # print(compute_average(row_size))
    # print(compute_average(col_size))

    print('Total table count: ', total_table_count)
    print('Total column count: ', total_column_count)
    print('Total row count: ', total_row_count)

    print('Tables removed step 1', tables_removed_step_1)
    print('Columns removed step 1', columns_removed_step_1)
    print('Tables removed step 2', tables_removed_step_2)
    print('Key columns removed step 2', key_cols_removed_step_2)
    print('Rows removed step 3', rows_removed_step_3)
    print('Columns removed step 4', columns_removed_step_4)
    print('Tables removed step 5', tables_removed_step_5)

    out.close()

'''
Function that determines whether a column is valid. A column is not valid if
it is empty, has an average length longer than 60 characters, all its 
entries are punctuation or if the same entry is repeated every time.

Arguments:
    column: Column to be determined if it is valid
Returns:
    True if column is valid and False otherwise.
'''
def is_valid_col(column):
    if (not is_empty_col(column) and 
        is_appropriate_size(column) and 
        not is_punctuation_col(column) and 
        not is_same_col(column)):
        return True
    else:
        return False

'''
Function used to determine whether a column is empty.

Arguments:
    column: Column to be determined if it is empty
Returns:
    True if column is empty and False otherwise.
'''
def is_empty_col(column):
    threshold = 0.8
    count = 0
    length = len(column)
    for i in range(length):
        if column[i] == "":
            count += 1
    if length == 0 or (count / length) > threshold:
        return True
    else:
        return False

'''
Function used to determine whether a column is appropriate size.

Arguments:
    column: Column to be determined if it is appropriate size
Returns:
    True if column is appropriate size and False otherwise.
'''
def is_appropriate_size(column):
    length = len(column)
    size = 0
    for i in range(length):
        size += len(column[i])
    if length > 0 and size / length > 60:
        return False
    else:
        return True

'''
Function used to determine whether a column has the same entry for
all its entries.

Arguments:
    column: Column to be determined if it has the same entries for all rows
Returns:
    True if column is has the same entries for all rows and False otherwise.
'''
def is_same_col(column):
    entries = set(column)
    if len(entries) == 1:
        return True
    else:
        return False

'''
Function used to determine whether every entry in a column is punctuation

Arguments:
    column: Column to be determined if it is all punctuation
Returns:
    True if column is all punctuation and False otherwise.
'''
def is_punctuation_col(column):
    is_punctuation_col = True
    for entry in column:
        if not is_punctuation(entry):
            is_punctuation_col = False
            break
    return is_punctuation_col

'''
Function used to determine whether a string is all punctuation.

Arguments:
    entry: String to be determined if it is all punctuation
Returns:
    True if entry is all punctuation and False otherwise.
'''
def is_punctuation(entry):
    is_punctuation = True
    for c in entry:
        if c not in punct_chars:
            is_punctuation = False
            break
    return is_punctuation

'''
Function that validates the key column. A key column is valid
if it is not the row index for the row, is textual, all entries
are unique and there at least 3 entries that do not have any
punctuation characters (excluding ,.-)

Arguments:
    columns: Columns in the data table
'''
def validate_key_col(valid_cols, key_cols_removed_step_2):
    if len(valid_cols) > 0:
        key_col = valid_cols[0]
        # Determine whether key column is valid
        if is_increment(key_col):
            key_cols_removed_step_2 += 1
            valid_cols.remove(key_col)
        switch_key_col(valid_cols)
    return key_cols_removed_step_2

'''
Function that determines whether the key column is incremental.

Arguments:
    key_col: Key column

Returns:
    True if the key column is incremental and False otherwise.
'''
def is_increment(key_col):
    increment = True
    try:
        start = int(key_col[0].strip(punct_chars))
        for i in range(1, len(key_col)):
            if (not is_punctuation(key_col[i]) and 
                key_col[i].strip(punct_chars) != str(start + i)):
                increment = False
                break
        return increment
    except ValueError:
        return False 

'''
Function that switches the key column for a table if the current key
column is not textual, unique or does not have more than 3 rows that
have no punctuation other than ,.-.
Deletes the table if there are no suitable key columns.

Arguments:
    cols: The columns of a data table
'''
def switch_key_col(cols):
    if len(cols) > 0:
        key_col = cols[0] 
        tag = tag_unit_for_column(key_col)
        # Determine whther current key column is valid
        if ((tag == "numeric") or 
            (tag == "date") or  
            (tag == "text" and is_punct_int_col(key_col)) or 
            not unique_key(key_col) or 
            not min_punct_col(key_col)):
            # If the current key column is not valid, iterate over each
            # other column and determine whether that column is valid.
            # If there is another column that would be a valid key column,
            # shift the columns to the right and put the new key column
            # as the first column. Otherwise clear the table.
            for i in range(1, len(cols)):
                tag = tag_unit_for_column(cols[i])
                if (tag == "text" and 
                    not is_punct_int_col(cols[i]) 
                    and unique_key(cols[i]) 
                    and min_punct_col(cols[i])):
                    # Make a copy of the columns
                    valid_cols = cols[:]
                    # Insert new key column at the beginning
                    valid_cols[0] = cols[i]
                    # Shift key column to the right by one
                    valid_cols[1] = key_col
                    # Remove the other columns
                    valid_cols = valid_cols[:2]
                    # Add the other columns (except for the old key column)
                    # into valid_cols
                    valid_cols.extend(cols[1:i] + cols[i+1:])
                    # Insert these new columns into the memory slot cols
                    cols.clear()
                    cols.extend(valid_cols)
                    return
            cols.clear()

'''
Function that takes in a column and returns the type of column (date, numeric or textual).
A column is considered date or numeric if it contains a single one of those types, textual otherwise.

Arguments: 
    col: column to be checked

Returns:
    A lowercase string of the column tag
'''
def tag_unit_for_column(col):
    for entry in col:
        try:
            parse(entry, fuzzy=fuzzy)
            return "date"
        except:
            try:
                float(entry)
                return "numeric"
            except:
                pass
    return "textual"

'''
Function that finds out if the column has at least the minimum number of 
entries that don't have punctuation characters (other than ,.-)

Arguments:
    column: Column to be checked

Returns:
    True if the column has the minimum amount of entries without punctuation
    and False otherwise.
'''
def min_punct_col(column):
    count = 0
    valid = False
    restrict_chars = punct_chars.replace("-", '').replace(",", '').replace(".", '').replace(" ", '').replace("'", '').replace("\"", '')
    for i in range(len(column)):
        entry = column[i]
        if not re.search("[" + restrict_chars + "]", entry) :
            count += 1
            if count > 2:
                valid = True
                break
    return valid   

'''
Function that determines whether at least 3 entries in the column are not a
combination of punctuation and integers

Arguments:
    column: Column to be checked

Returns:
    True if the column does not have 3 entries that are not a combination
    of punctuation and integers and False otherwise.
'''
def is_punct_int_col(column):
    is_punct_int_col = True
    count = 0
    for entry in column:
        if not is_punct_int(entry):
            count += 1
            if count > 2:
                is_punct_int_col = False
                break
    return is_punct_int_col

'''
Function that determines whether a string is a combination of punctuation and
integers.

Arguments:
    entry: String to be checked

Returns:
    True if the string is a combination of punctuation and integers and False
    otherwise.
'''
def is_punct_int(entry):
    is_punct_int = True
    punct_int_chars = punct_chars + "0123456789"
    for c in entry:
        if c not in punct_int_chars:
            is_punct_int = False
            break
    return is_punct_int
            
'''
Function that determines if the column has all unique entries (no duplicates)

Arguments:
    col: Column to be checked

Returns:
    True if the column has unique entries and False otherwise.
'''
def unique_key(col):
    key_entries_length = len(col)
    key_set_length = len(set(col))
    if (key_set_length / key_entries_length) == 1:
        return True
    else:
        return False

'''
Function that computes the average of a list of numbers.

Arguments:
    numbers: A list of numbers

Returns:
    The average of the numbers.
'''
def compute_average(numbers):
    return sum(numbers) / len(numbers) 

main()
