import operator
import math
import ApproxMatch
import ColumnGrouping
from Column import Column

class R:
    '''
    Class that represents a relational table.
    '''
    table_list = None
    key_table_index = None
    keywords_index = None
    exact_match_index = None
    ngrams_index = None

    def __init__(self, rows):
        self.rows = list(rows)
        self.num_rows = len(rows)
        self.related_tables = None
        self.avg_col_char_size = {}
        self.agg_word_size = 0
        self.col_lengths = {}
        self.count_num_cols()   
        self.has_changed = True  
        self.related_tables_dict = {}       
    
    def __str__(self):
        # The output is each row on a separate line, then the 
        # number of rows and columns on their own line
        return_string = ""
        for row in self.rows:
            return_string += str(row) + "\n"
        return_string += "Number of rows: " + str(len(self.rows)) + "\n"
        return_string += "Number of cols: " + str(self.get_num_cols())
        return return_string

    def get_num_cols(self):
        '''
        Function the returns the number of columns in this table.
        '''
        return self.num_cols

    def get_avg_col_char_size(self, column):
        '''
        Returns the average number of characters per entry in the column relating to the given index.
        '''
        return self.avg_col_char_size[column]

    def get_agg_word_size(self):
        '''
        Returns the average number of characters per entry in the column relating to the given index.
        '''
        return self.agg_word_size

    def get_col_size(self, column):
        '''
        Returns the size of the given column (number of rows). 
        '''
        return self.col_lengths[column]
        
    def count_num_cols(self):
        '''
        Function that counts the number of columns in this table (if some
        columns have different lengths then it takes the most common
        number of columns). Also counts the avg entry size in characters 
        for each column.
        '''
        # For loop used to count the different lengths of rows and
        # how frequent they are
        col_lengths = {}
        agg_col_char_size = []
        for row in self.rows:
            size = len(row)
            if size in col_lengths:
                col_lengths[size] += 1
            else:
                col_lengths[size] = 1
            for i in range(len(row)):
                self.agg_word_size += len(row[i].split(" "))
                if i > len(agg_col_char_size) - 1:
                    agg_col_char_size.append([len(row[i]),1])
                else:
                    agg_col_char_size[i] = [agg_col_char_size[i][0] + len(row[i]), agg_col_char_size[i][1] + 1]
                    

        for column in range(len(agg_col_char_size)):
            self.avg_col_char_size[column] = int(agg_col_char_size[column][0]/agg_col_char_size[column][1])
            self.col_lengths[column] = agg_col_char_size[column][1]

        if len(col_lengths) > 0:
            # Sorts the column lengths in descending order based on frequency
            # and picks the most frequent column length as the table column
            # length.
            self.num_cols = sorted(
                col_lengths.items(), key=operator.itemgetter(1), 
                reverse=True)[0][0] 
        else:
            self.num_cols = 0

    def count_num_rows(self):
        '''
        Function that counts the number of rows in this table.
        '''
        self.num_rows = len(self.rows)

    def get_num_rows(self):
        '''
        Function that returns the number of rows in this table.
        '''
        return self.num_rows

    def get_rows(self):
        '''
        Returns a copy of the rows in this relational table.
        '''
        return tuple(self.rows[:])

    def insert(self, row):
        '''
        Function that inserts a row into a table.

        Arguments:
            row: A row to be added to this table.

        Returns:
            True if the row was successfully added and false otherwise.
        '''
        size = len(row)
        if size > self.num_cols:
            # Can't add a row with more columns than the number of 
            # columns in the table
            print("Row given has %d columns but requires %d columns." 
                  % (size, self.num_cols))
            return False
        else:
            # Insert row
            self.rows.append(row)

            # Update table information
            self.has_changed = True
            self.count_num_rows()
            self.count_num_cols()
            return True
        
    def delete(self, row):
        '''
        Function that deletes a row from a table.

        Arguments:
            row: A row to be removed to this table.

        Returns:
            True if the row was successfully removed and false otherwise.
        '''
        if row in self.rows:
            # Delete row
            self.rows.remove(row)

            # Update table information
            self.has_changed = True
            self.count_num_rows()
            self.count_num_cols()
            return True
        else:
            # Can't delete a row that's not in the table.
            print("Row %s not in table." %str(row))
            return False

    def del_row(self, ind):
        '''
        Function that deletes a row from a table.
        
        Arguments:
            ind: An index (from 1 to number of rows) of the row to be 
                removed from the table.

        Returns:
            True if the row was successfully removed and false otherwise.
        '''
        # Delete row from table
        if ind == 1:
            self.rows = self.rows[1:]
        elif ind == self.num_rows:
            self.rows = self.rows[:-1]
        elif ind > 1 and ind < self.num_rows:
            self.rows = self.rows[:ind - 1] + self.rows[ind:]
        else:
            print("Index out of range 1 to %d." %self.num_rows)
            return False

        # Update table information
        self.has_changed = True
        self.count_num_rows()
        self.count_num_cols()
        return True

    def del_col(self, ind):
        '''
        Function that deletes a column from a table.

        Arguments:
            ind: An index (from 1 to number of columns) of the column to be
                removed from the table.

        Returns: 
            True if the column was successfully removed and 
            false otherwise.
        '''
        if ind == 1:
            # Case where deleting key column
            # Thus make sure there is another column that does not have an
            # empty entry
            for i in range(1, self.num_cols):
                an_empty = False
                for j in range(self.num_rows):
                    entry = self.rows[j][i]
                    if entry == '':
                        an_empty = True
                        break
                if not an_empty:
                    self.swap(1, i + 1)
                    ind = i + 1
                    break

            # Delete column only if it's the last column or if there is
            # another column that can be the key column.
            if self.num_cols != 1 and an_empty:
                # Can't delete key column as there is no suitable replacement
                print(
                    "Cannot delete the key column as there is no other column\n"
                    + "that does not have a null value in one of its rows.")
                return False
        
        # Delete column
        if ind == self.num_cols:
            for i in range(self.num_rows):
                self.rows[i] = self.rows[i][:-1]
        elif ind > 1 and ind < self.num_cols:
            for i in range(self.num_rows):
                self.rows[i] = self.rows[i][:ind - 1] + self.rows[i][ind:]
        else:
            print("Index out of range 1 to %d." %self.num_cols)
            return False

        # Update table information
        self.has_changed = True
        self.count_num_cols()
        self.count_num_rows()
        return True

    def delcell(self, row_index, col_index):
        '''
        Function that deletes the entry from the cell at
        [row_index][col_index].

        Arguments:
            row_index: Row index of the cell to be deleted
            col_index: Column index of the cell to be deleted

        Returns:
            True if the cell was successfully removed and 
            false otherwise.
        '''
        # Ensure indexes are appropriate
        if row_index < 1 or row_index > self.num_rows:
            print("Row index is not in between 1 and %d" %self.num_rows)
            return False
        elif col_index == 1:
            print("Cannot delete an entry from the key column")
            return False
        elif col_index < 2 or col_index > self.num_cols:
            print("Column index is not in between 2 and %d" %self.num_cols)
            return False

        row_index -= 1
        col_index -= 1

        # Remove entry from cell
        self.rows[row_index] = list(self.rows[row_index])
        self.rows[row_index][col_index] = ""
        self.rows[row_index] = tuple(self.rows[row_index])

        # Update table information
        self.has_changed = True
        self.count_num_cols()
        self.count_num_rows()
        return True

    def update_cell(self, row_index, col_index, value):
        '''
        Function that updates the entry of the cell at
        [row_index][col_index].

        Arguments:
            row_index: Row index of the cell to be updated
            col_index: Column index of the cell to be updated
            value: Desired entry for the cell

        Returns:
            True if the cell was successfully updated and 
            false otherwise.
        '''
        # Ensure the indexes are appropriate
        if row_index < 1 or row_index > self.num_rows:
            print("Row index is not in between 1 and %d" %self.num_rows)
            return False
        elif col_index == 1:
            print("Cannot delete an entry from the key column")
            return False
        elif col_index < 1 or col_index > self.num_cols:
            print("Column index is not in between 1 and %d" %self.num_cols)
            return False

        row_index -= 1
        col_index -= 1

        # Update the cell
        self.rows[row_index] = list(self.rows[row_index])
        try:
            self.rows[row_index][col_index] = value
        except IndexError:
            # Catch block if adding a cell to a row with less columns than
            # the other rows in the table.
            row_length = len(self.rows[row_index])
            while row_length < col_index:
                self.rows[row_index].append('')
                row_length += 1
            self.rows[row_index].append(value)
        self.rows[row_index] = tuple(self.rows[row_index])

        # Update table information
        self.has_changed = True
        self.count_num_cols()
        self.count_num_rows()
        return True

    def find_related_tables(self, relational_threshold, edit_dist_threshold, marked_columns=None):
        '''
        Function used to created a dictionary 'self.related_tables_dict' 
        where the key is a table that is related to 'self' and the value 
        is a list composed of three entries: order of the column mapping, 
        value of the related score and dictionary containing the row_mappings.

        Arguments:
            relational_threshold: Determines the minimum decimal amount of rows 
                from this relational table that need to be in the related table
            edit_dist_threshold: Threshold that determines the allowed amount 
                of deviation for two strings to be considered similar.
            marked_columns: a list of column indices indicating which columns should be used to map (if none then all are used)
        '''
        # Check if table has changed or if the relational_threshold has 
        # changed. If it has not, we can return the related_tables_dict 
        # from the previous iteration.
        if self.has_changed or self.previous_threshold != relational_threshold:    
            self.previous_threshold = relational_threshold       

            short = {}
            longs = []
            # Extract 'short' keys from self. A key is considered short
            # if it is impossible to perform an edit operation upon the 
            # key to match it with another string given the current 
            # edit distance threshold.
            # i.e. if len(example_key) * edit_dist_threshold < 1, this
            # means there needs to be less than 1 operation (0 operations)
            # on example_key and another string for them to be 
            # considered the same.
            for row in self.rows:
                if (len(row[0]) * edit_dist_threshold) < 1:
                    short[row] = []
                else:
                    longs.append(row)

            # For each short key, extract the table indices of the tables 
            # in the data file that have this key in one of their rows
            for row in short.keys():
                key = row[0].lower()
                try:
                    # Find all data tables that have the short key
                    pot_tables_ind = self.key_table_index[key] 
                    for ind in pot_tables_ind:
                        table = self.table_list[ind]
                        # Determine whether they contain not only the
                        # key but the full row
                        if table.contains(row, edit_dist_threshold):
                            short[row].append(table)
                except KeyError:
                    continue
            
            # We are able to improve speed and efficiency by only using keys
            # that cannot be modified to other keys. This is because we can
            # quickly reduce the set of tables that can be related to the
            # subset of tables that have any of these short keys in their 
            # table. 
            # This will work if the number of short rows is greater than the
            # number of tables in the query table multiplied by the relational
            # threshold and then adding 1 to the total.
            # In other words, the number of long rows required for us not to 
            # have a subset of all possible tables is as follows below:
            num_short = len(short)
            num_long_req = math.ceil(self.num_rows
                                    * relational_threshold 
                                    - num_short)

            if num_long_req > 0:
                # There are not enough short rows so we extract the fewest 
                # amount of long rows as possible (num_long_req)
                for i in range(num_long_req):
                    row = longs[i]
                    row_length = len(row[0])
                    short[row] = []
                    query_key = row[0].lower()
                    # Maximum number of addition or subtraction operations
                    # that can be made
                    edit_dist_max = int(row_length * edit_dist_threshold)
                    # Need to iterate over each possible key from the data
                    # file since there is a possibility the long key can
                    # be modified into one or more of these keys.
                    for key in self.key_table_index.keys():
                        key_length = len(key)
                        # Check to see if it is impossible for the long 
                        # key to be modified to the variable key
                        if ((key_length > (row_length + edit_dist_max)) or 
                            (key_length < (row_length - edit_dist_max))):
                            continue
                        if ApproxMatch.is_approx_match(
                                query_key, key, 
                                edit_dist_threshold):
                            # Find all data tables that have the key
                            pot_tables_ind = self.key_table_index[key]
                            for ind in pot_tables_ind:
                                table = self.table_list[ind]
                                # Determine whether they contain not only the
                                # key but the full row
                                if (table.contains(row, edit_dist_threshold) and 
                                    table not in short[row]):
                                    short[row].append(table)
                        
            # Count the number of keys each table contains
            related_tables_count = {}
            # https://stackoverflow.com/questions/27733685/iterating-over-dict-values
            for row in short.keys():
                for table in short[row]:
                    if table not in related_tables_count:
                        related_tables_count[table] = 1
                    else:
                        related_tables_count[table] += 1
            
            # Determine how many short rows are required for a table to
            # potentially be related.
            num_short_req = num_short - (self.num_rows * relational_threshold)
            related_tables = []  
            for table in related_tables_count.keys():
                if related_tables_count[table] >= num_short_req:
                    related_tables.append(table)

            # For each potentially related table, determine whether it is
            # actually related to self.
            self.related_tables_dict.clear()
            for table in related_tables:
                # Remove all tables from consideration that do cannot be 
                # related as they do not have enough rows or do not 
                # have enough columns.
                if (table.get_num_rows() < (self.num_rows * relational_threshold) or
                    table.get_num_cols() < self.num_cols):
                    continue             
                (col_mapping, value), row_mappings = self.compare(
                                                table, edit_dist_threshold, marked_columns)
                value = value / self.num_rows
                if value >= relational_threshold:
                    self.related_tables_dict[table] = [col_mapping, 
                                                       value, 
                                                       row_mappings]

            self.has_changed = False

    def get_related_by_index(self, index):
        '''
        Function used to find a particular related table to this relational
        table.

        Arguments:
            index: The index of the table that would like to be retrieved
        
        Returns:
            If the index was between 1 and the number of related tables this 
            functions returns the desired table and its related score. 
            Otherwise return None for both the desired table and its score
        '''
        # Determine if index is valid
        if self.related_tables == None:
            print(".related has not been called on this table yet.")
        elif self.related_tables == []:
            print(".related returned no results")
        elif index > len(self.related_tables):
            print(
                "Index given is too large. Number of tables related is %d." 
                % (len(self.related_tables)))
        elif index <= 0:
            print("Index given is invalid. Indexes start at 1.")
        else:
            return (self.related_tables[index - 1][0], 
                    self.related_tables[index - 1][1])
        return None, None

    def related(self, relational_threshold, edit_dist_threshold, marked_columns=None):
        '''
        Function used to find all tables that are related to this relational 
        table given a certain threshold.

        Arguments:
            relational_threshold: Determines the minimum decimal amount of 
                rows from this relational table that need to be in the 
                related table
            edit_dist_threshold: Threshold that determines the allowed 
                amount of deviation for two strings to be considered similar.
            marked_columns: a list of column indices indicating which columns should be used to map (if none then all are used)

        Returns:
            The related tables to self if there are any.
        '''
        self.related_to = {}
        # Find related tables
        self.find_related_tables(relational_threshold, edit_dist_threshold, marked_columns)

        # Store the score of each related table
        for table in self.related_tables_dict.keys():
            value = self.related_tables_dict[table][1]
            self.related_to[table] = value

        # https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value
        # taken on 2018-05-02
        # Sort the order of the related tables based on their relatedness 
        # score with self
        self.related_tables = sorted(self.related_to.items(), 
                                     key=operator.itemgetter(1), 
                                     reverse=True)
        related_tables = []
        for table, value in self.related_tables:
            related_tables.append(table)
        return related_tables

    def compare(self, other_r, edit_dist_threshold, marked_columns=None):
        '''
        Function used to compare this relational table to another 
        relational table.

        Arguments:
            other_r: The other relational table
            edit_dist_threshold Threshold that determines the allowed amount 
                of deviation for two strings to be considered similar.
            marked_columns: a list of column indices indicating which columns should be used to map (if none then all are used)

        Returns:
            The mapping from this relational table to other_r (col_mapping), 
            how many rows matched (value) and a mapping of the rows in this 
            table to the rows they matched to in the other relational table.
        '''
        other_r_rows = other_r.get_rows()
        other_length = other_r.get_num_rows()
        col_mapping_scores = {}
        row_mappings = {}
        if self.num_rows > 0 and other_length > 0:
            # FOR loop that iterates over each row in this relational table
            for i in range(self.num_rows):
                # FOR loop that iterates over each row in the other 
                # relational table
                for j in range(other_length):   
                    # Determine if the primary key (first column) are the 
                    # same. If not we know these rows don't match  
                    if not (ApproxMatch.is_approx_match(
                            self.rows[i][0].lower(), 
                            other_r_rows[j][0].lower(), 
                            edit_dist_threshold)):
                        continue                    
                    # refers_to is a list to keep track of the mapping from 
                    # our table to another table. The first index in the list 
                    # corresponds to the first column in this table and the 
                    # numeric entries in this list refer to the column index 
                    # in the other table that is similar.                  
                    refers_to = []
                    # ASSUMPTION First column is a primary key and must be 
                    # identical to the other relational table so we set the 
                    # first list to contain the index 0 and only 0
                    refers_to.append([0])

                    # FOR loop that iterates over each column in the row 
                    # from this relational table
                    for k in range(1, len(self.rows[i])): 
                        #CHANGE TO ONLY ITERATE OVER GIVEN COLUMNS
                        if marked_columns is not None and k not in marked_columns:
                            continue
                        refers_to.append([])
                        entry = self.rows[i][k].lower()
                        # Empty string matches everything
                        if entry == '':
                            for l in range(1, len(other_r_rows[j])):
                                refers_to[-1].append(l)
                        else:
                            # FOR loop that iterates over each column in the 
                            # row from the other relational table
                            for l in range(1, len(other_r_rows[j])):
                                # Check to see if the entries from row i 
                                # column k in this relational table and row j
                                # column l in the other relational table are 
                                # similar
                                if (ApproxMatch.is_approx_match(
                                        entry, 
                                        other_r_rows[j][l].lower(), 
                                        edit_dist_threshold)):
                                    refers_to[-1].append(l)
                    
                    # After iterating over each column for a specific row, 
                    # determine if there is a row match.If there is a 
                    # column that does not have a mappping to a column in
                    # the other table, there is NOT a row match.
                    row_match = True
                    for k in range(1, len(refers_to)):
                        if refers_to[k] == []:
                            row_match = False
                    # If there is a row match, determine the mapping
                    if row_match:
                        row_mappings[self.rows[i]] = other_r_rows[j]
                        ids = ['']
                        # FOR loop that iterates over each column in the 
                        # row from this relational table
                        for k in range(len(refers_to)):
                            # If only 1 mapping possible
                            if len(refers_to[k]) == 1:
                                for l in range(len(ids)):
                                    ids[l] += str(refers_to[k][0]) + ","
                            else:
                                new_ids = []
                                # FOR loop that iterates over each possible 
                                # column from the other relational table
                                for l in range(len(refers_to[k])):
                                    org_ids = ids[:]
                                    for m in range(len(org_ids)):
                                        org_ids[m] += (str(refers_to[k][l]) 
                                                       + "," )
                                    new_ids += org_ids
                                ids = new_ids
                        # Iterate over each id and add it or increase the 
                        # count in the dictionary
                        for string_id in ids:
                            if string_id in col_mapping_scores:
                                col_mapping_scores[string_id] += 1
                            else:
                                col_mapping_scores[string_id] = 1
                        break
            # Check to see if there were matches. If there was, return the 
            # most common mapping.
            if len(col_mapping_scores) > 0:
                best_col_mapping_and_score = sorted(
                        col_mapping_scores.items(), 
                        key=operator.itemgetter(1), 
                        reverse=True)[0]
                return best_col_mapping_and_score, row_mappings
            else:
                return (0, 0), {}
        else:
            return (0, 0), {}
        
    def contains(self, row, edit_dist_threshold):
        '''
        Function used to determine if a row is contained inside this table.

        Arguments:
            row: Query row
            edit_dist_threshold: Threshold that determines the allowed 
                amount of deviation for two strings to be considered similar.

        Returns:
            A boolean indicating if the row is contained inside this table
        '''
        # Row must have same number of columns
        if len(row) > self.num_cols:
            return False
        # FOR loop that iterates over each row in this relational tab;e
        for i in range(self.num_rows):   
            # Determine if the primary key (first column) are the same
            # If not we know these rows don't match  
            if not (ApproxMatch.is_approx_match(
                    self.rows[i][0].lower(), 
                    row[0].lower(), 
                    edit_dist_threshold)):
                continue                    
            # refers_to is a list to keep track of the mapping from our table
            # to another table. The first index in the list corresponds to 
            # the first column in the parameter row and the numeric entries 
            # in this list refer to the column in this table that are 
            # identical.                  
            refers_to = []
            # ASSUMPTION First column is a primary key and must be identical 
            # to the other relational table so we set the first list to 
            # contain the index 0 and only 0
            refers_to.append([0])
    
            # FOR loop that iterates over each column in the row passed as 
            # parameter
            for k in range(1, len(row)):
                refers_to.append([])                             
                entry = row[k].lower()       
                # Empty string matches everything         
                if entry == '':
                    for l in range(1, len(self.rows[i])):
                        refers_to[k].append(l)
                else:
                    # FOR loop that iterates over each column in the rows 
                    # from this relational table
                    for l in range(1, len(self.rows[i])):
                        # Check to see if the entries from row i column l in 
                        # this relational table and column k of the parameter 
                        # row are similar
                        if (ApproxMatch.is_approx_match(
                                entry, 
                                self.rows[i][l].lower(), 
                                edit_dist_threshold)):
                            refers_to[k].append(l)               
            
            # After iterating over each column, 
            # determine if there is a row match. If there is a 
            # column that does not have a mappping to a column in
            # the other table, there is NOT a row match. If all columns
            # have a mapping that means there is a row match.
            row_match = True
            for k in range(1, len(row)):
                if refers_to[k] == []:
                    row_match = False
            # If there is a row match, return True
            if row_match:
                return True
        return False

    def xr(self, relational_threshold, edit_dist_threshold, num_tables):
        '''
        Function used to extend this relational table to include additional 
        rows from the tables that are similar.

        Arguments:
            relational_threshold: Determines the minimum decimal amount of 
                rows from this relational table that need to be in the data
                table for the data table to be considered related
            edit_dist_threshold: Threshold that determines the allowed amount
                of deviation for two strings to be considered similar.
            num_tables: Number of tables in the data file. This is used to
                scale the existing entries in the table so they will not
                be below any row that is appended through .xr

        Returns:
            A table that includes the additional rows taken from other 
            similar tables.
        '''
        related_rows = {}
        self.find_related_tables(relational_threshold, edit_dist_threshold)

        # Scale all rows already in the table so cannot be supassed by a row 
        # that is appended through xr
        for row in self.get_rows():
            related_rows[row] = num_tables + 1

        # Iterate over each related table
        for table in self.related_tables_dict.keys():
            col_mapping = self.related_tables_dict[table][0]
            value = self.related_tables_dict[table][1]
            row_mappings = self.related_tables_dict[table][2]

            col_mapping = col_mapping.split(",")[:-1]
            # Iterate over each row from the related table
            for table_row in table.get_rows():
                # Create new row with key and columns that match the query
                # table
                row = []
                for ind in col_mapping:
                    if len(table_row) > int(ind):
                        row.append(table_row[int(ind)])
                    else:
                        row.append("")
                row = tuple(row)
                # Add the related score of the row to related_rows
                if row in related_rows:
                    related_rows[row] += value
                else:
                    related_rows[row] = value

        # https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value
        # taken on 2018-05-02
        # Sort the related_rows based on their score in descending order
        xr_order = sorted(
                    related_rows.items(), 
                    key=operator.itemgetter(1), 
                    reverse=True)

        xr_table = []
        # Make sure there are no rows with empty keys that are going to be 
        # appended to query table
        for row_tup in xr_order:
            key = row_tup[0][0]
            if key != '':
                xr_table.append(row_tup[0])

        return R(xr_table)

    def xc(self, relational_threshold, grouping_threshold, empty_threshold, 
           operation, multiplier, edit_dist_threshold):
        '''
        Function used to extend this relational table to include additional 
        columns from the tables that are similar.

        Arguments:
            relational_threshold: Determines the minimum decimal amount of 
                rows from this relational table that need to be in the 
                related table
            grouping_threshold: Determines the minimum percentage amount of
                rows from 2 columns that are the same for these two columns 
                to be grouped together
            empty_threshold: Maximum percentage amount of empty entries in a
                column for it to be added to the table
            operation: The way the grouped columns are supposed to be
                combined into a single column (either sum rule or product 
                rule)
            multiplier: The multiplier parameter represents the smoothing 
                parameter for the product rule operation. Is irrelevant for 
                the sum rule.
            edit_dist_threshold: Threshold that determines the allowed 
                amount of deviation for two strings to be considered similar.

        Returns:
            A table that includes the additional columns taken
            from other similar tables.
        '''
        # Find related tables
        self.find_related_tables(relational_threshold, edit_dist_threshold)
        
        cols = []
        # Iterate over each related table
        for table in self.related_tables_dict.keys():
            col_mapping = self.related_tables_dict[table][0]
            value = self.related_tables_dict[table][1]
            row_mappings = self.related_tables_dict[table][2]

            col_mapping = col_mapping.split(",")[:-1]
            for i in range(len(col_mapping)):
                col_mapping[i] = int(col_mapping[i])
            # All possible column indexes
            col_indexes = list(range(table.get_num_cols()))

            # https://codereview.stackexchange.com/questions/24520/finding-missing-items-in-an-int-list
            # taken on 2018-05-11
            # Find all column indexes that are not being mapped to a column
            # in the query table
            missing_cols_ind = list(set(col_indexes) - set(col_mapping))
            # Iterate over each missing column
            for ind in missing_cols_ind:
                missing_col = []
                # Create a column that only has the entries for the keys in 
                # the query table. The key to the rows that are not found 
                # in this related table are given an empty string as entry
                for row in self.rows:
                    if row in row_mappings and len(row_mappings[row]) > ind:
                        missing_col.append(row_mappings[row][ind])
                    else:
                        missing_col.append("")
                column = Column(value, tuple(missing_col))
                cols.append(column)
        
        # Used ColumnGrouping module to group and extract the appropriate 
        # columns
        groups = ColumnGrouping.group_columns(cols, grouping_threshold)  
        cols = ColumnGrouping.extract_grouped_cols(
                                groups, operation, multiplier)

        # Make sure all columns have a smaller percentage of empties than the 
        # empty threshold.
        max_empty_entries = self.num_rows * empty_threshold
        for col in cols:
            empty = False
            count = 0
            for entry in col:
                if entry == '':
                    count += 1
                    if count > max_empty_entries:
                        empty = True
                        break
            if empty:
                cols.remove(col)

        # Create new extended table
        num_value_cols = len(cols)
        xc_table = list(self.rows[:])
        for i in range(self.num_rows):
            xc_table[i] = list(xc_table[i])
            # Append the additional column entries to each row
            for j in range(num_value_cols):
                xc_table[i].append(cols[j][i])
            xc_table[i] = tuple(xc_table[i])
            
        return R(xc_table)

    def fill(self, relational_threshold, operation, 
             multiplier, edit_dist_threshold):
        '''
        Function that tries to fill all empty cells inside of self.

        Arguments:
            relational_threshold: Determines the minimum decimal amount of 
                rows from this relational table that need to be in the 
                related table
            operation: The way that the scores for different values for the
                same cell are too be calculated (options are sum, product
                and probabilistic)
            multiplier: The multiplier parameter represents the smoothing 
                parameter for the product rule operation. Is irrelevant for 
                the sum rule and probabilistic version.
            edit_dist_threshold: Threshold that determines the allowed 
                amount of deviation for two strings to be considered similar.

        Returns:
            A table that may include new entries where previously there were
            empty values.        
        '''
        # Find all null values
        nulls = []
        for i in range(self.num_rows):
            for j in range(1, self.num_cols):
                entry = self.rows[i][j]
                if entry == '':
                    nulls.append((i,j))

        # Extract copy of self.rows
        rows = list(self.rows)
        # Initialize variable if probablistic operation
        if operation == "probabilistic":
            cell_value_pairs = {}

        # Iterate over each null value
        for null in nulls:
            values = {}          
            fill_table = R(rows)
            
            # Modify fill_table so that it does not have the row with an empty
            # value as well as other value columns that are not needed.
            null_row_ind, null_col_ind = null
            for i in range(self.num_cols - 1, 0, -1):
                if i != null_col_ind:
                    fill_table.del_col(i + 1)
            fill_table.del_row(null_row_ind + 1)
            # Reinsert the key of the row with an empty value in order to have
            # a mapping between the key and a row that can potentially have
            # a value for the empty string
            key = self.rows[null_row_ind][0]
            tup_key = (key,)
            fill_table.insert(tup_key)
            try:
                # Find tables that have the key with the empty value in its
                # row
                query_key = key.lower()
                key_length = len(key)
                if (key_length * edit_dist_threshold) > 1:
                    # Case where the key is a 'long' string so it could match
                    # with multiple keys. Keep track of all tables that have
                    # a key that matches with our key.
                    pot_tables_ind = []
                    edit_dist_max = int(key_length * edit_dist_threshold)
                    for data_key in self.key_table_index.keys():
                        data_key_length = len(data_key)
                        if ((data_key_length > (key_length + edit_dist_max)) or
                            (data_key_length < (key_length - edit_dist_max))):
                            continue
                        if (ApproxMatch.is_approx_match(
                                query_key,
                                data_key, 
                                edit_dist_threshold)):
                            pot_tables_ind += self.key_table_index[data_key]
                    pot_tables_ind = list(set(pot_tables_ind))
                else:
                    # Case where the key is a 'short' string so it could match
                    # with only itself.
                    pot_tables_ind = self.key_table_index[query_key] 
                
                # Count var is used to maintain count of the number of 
                # iterations in order to scale all entries equally in the 
                # product operation.
                if operation == "product":
                    count = 0   

                for ind in pot_tables_ind:
                    # For each table that has a key that matches with the
                    # key that has a missing value, calculate it's related
                    # score.
                    table = self.table_list[ind]
                    (col_mapping, value), row_mappings = fill_table.compare(
                                                        table,
                                                        edit_dist_threshold)
                    col_mapping = col_mapping.split(",")[:-1]
                    for i in range(len(col_mapping)):
                        col_mapping[i] = int(col_mapping[i])
                    # Adding 1 since the key row (has no value columns) will 
                    # not have the same column mappings as the other rows 
                    # if the other rows have more than one column
                    if len(col_mapping) > 1:
                        value += 1

                    # Determine whether the table has a related score greater
                    # than the related threshold
                    if (value > 1 and 
                        value >= relational_threshold 
                                 * fill_table.get_num_rows()):
                        entry = row_mappings[tup_key][col_mapping[1]]
                        value = value / fill_table.get_num_rows()
                        # Sum Version
                        if operation == "sum":
                            if entry in values:
                                values[entry] += value
                            else:
                                values[entry] = value
                        # Product Version
                        elif operation == "product":
                            if entry in values:
                                values[entry] *= value
                            else:
                                values[entry] = value * (multiplier ** count)
                            count += 1
                            for key in values.keys():
                                if key != entry:
                                    values[key] *= multiplier
                        # Probabilistic Version
                        elif operation == "probabilistic":
                            if entry in values:
                                values[entry].append(value)
                            else:
                                values[entry] = [value]
                        else:
                            print("INVALID OPERATION TYPE")
                            return False

                if operation == "probabilistic":
                    # If operation is probabilistic, keep track of the scores 
                    # for each different possible value.    
                    cell_value_pairs[null] = values
                else:
                    # If operation is sum or product, find the value with the
                    # highest score and insert it into the empty value slot.
                    try:
                        best_entry = sorted(values.items(), 
                                            key=operator.itemgetter(1), 
                                            reverse=True)[0][0]
                    except IndexError:
                        best_entry = ''
                    rows[null_row_ind] = list(rows[null_row_ind])
                    rows[null_row_ind][null_col_ind] = best_entry
                    rows[null_row_ind] = tuple(rows[null_row_ind])
            except KeyError:
                continue

        # If operation is probabilistic, find the assignment that accrues
        # the highest score.
        if operation == "probabilistic":
            # https://docs.python.org/3/library/itertools.html#itertools.permutations
            # accessed on 2018-08-08
            # Find all possible assignments
            assignments = [[]]
            for cell in cell_value_pairs:
                # If there were no values for this cell, set this cell to the
                # empty string
                if cell_value_pairs[cell] == {}:
                    cell_value_pairs[cell] = {'':[0]}
                assignments = [
                    x + [y] 
                    for x in assignments 
                    for y in list(cell_value_pairs[cell].keys())
                ]

            # Calculate the score for each assignment
            best_score = 0
            best_assignment = None
            cells = list(cell_value_pairs.keys())
            # For each assignment, the score is calculated as follows:
            # For each cell c and value v:
            # For each table t that assigns a value for cell c:
            # If t assigns v to c, score += table_score
            # Else, score += (1 - table_score)
            for assignment in assignments:
                score = 0
                # Iterate over each cell/value
                for i in range(len(assignment)):
                    cell = cells[i]
                    value = assignment[i]
                    # Iterate over each possible value
                    for possible_value in list(cell_value_pairs[cell].keys()):
                        if value == possible_value:
                            # Iterate over each table that assigns value
                            # to this cell
                            for probability_score in cell_value_pairs[cell][possible_value]:
                                score += probability_score
                        else:
                            # Iterate over each table that assigns this other
                            # value possible_value to cell c
                            for probability_score in cell_value_pairs[cell][possible_value]:
                                score += (1 - probability_score)
                # Update best score
                if score > best_score:
                    best_score = score
                    best_assignment = assignment

                #print("Assignment:", assignment, "\tScore:", score)

            # If there was an assignment, fill all the previously null values 
            # with their value
            if best_assignment != None:
                for i in range(len(cells)):
                    cell = cells[i]
                    rows[cell[0]] = list(rows[cell[0]])
                    rows[cell[0]][cell[1]] = best_assignment[i]
                    rows[cell[0]] = tuple(rows[cell[0]])
                    
        rows = tuple(rows)
        return R(rows)

    def sort(self, index, desc):
        '''
        Function used to sort the table on a column. 

            index: Index (from 1 to number of columns) of the column
                to be sorted on.
            desc: Boolean indicating if it should be sorted in 
                descending order
        
        Returns:
            A boolean indicating if the sorting was succesful
        '''
        # Dictionary where the key is the row and the value is the entry 
        # from the column that will be sorted upon.
        col = {}
        # Make sure index is appropriate
        if index >= 1 and index <= self.num_cols:
            for i in range(self.num_rows):
                col[self.rows[i]] = self.rows[i][index - 1]
                
            # Sort the table by the values of column i
            sorted_order = sorted(col.items(), 
                                  key=operator.itemgetter(1), 
                                  reverse=desc)
            self.rows = []
            for entry in sorted_order:
                self.rows.append(entry[0])          

            return True
                
        else:
            print("Index given is not in between 1 and %d" %self.num_cols)
            return False

    def swap(self, indx1, indx2):
        '''
        Function used to swap two columns in the table.

        Arguments:
            indx1: Index (from 1 to number of columns) of the first
                column to be swapped
            indx2: Index (from 1 to number of columns) of the second
                column to be swapped
        ''' 
        # If both indexes are the same no swap needs to happen
        if indx1 == indx2:
            return 
        # Make sure indx1 is appropriate
        elif indx1 < 1 or indx1 > self.num_cols:
            print("Index 1 is not in between 1 and %d" %self.num_cols)
            return False
        # Make sure indx1 is appropriate
        elif indx2 < 1 or indx2 > self.num_cols:
            print("Index 2 is not in between 1 and %d" %self.num_cols)
            return False
        
        # If one of the indexes is the key column, make sure there will be no
        # empty keys after the swap.
        if indx1 == 1:
            empties = []
            for i in range(self.num_rows):
                entry = self.rows[i][indx2 - 1]
                if entry == '':
                    empties.append(i + 1)
            num_empties = len(empties)

            if num_empties > 0:
                print(
                    "Cannot swap these two columns as the key column (first" 
                    + " column)\ncannot have empty keys and in Column %d, "
                    % (indx2)
                    + "row(s):")
                for ind in empties:
                    print("Row %d" %ind)
                if num_empties == 1:
                    print("is empty.")
                else:
                    print("are empty.")
                return False               
        elif indx2 == 1:
            empties = []

            for i in range(self.num_rows):
                entry = self.rows[i][indx1 - 1]
                if entry == '':
                    empties.append(i + 1)
            num_empties = len(empties)
            if num_empties > 0:
                print(
                    "Cannot swap these two columns as the key column (first" 
                    + " column)\ncannot have empty keys and in Column %d, "
                    % (indx1)
                    + "row(s):")
                for ind in empties:
                    print("Row %d" %ind)
                if num_empties == 1:
                    print("is empty.")
                else:
                    print("are empty.")
                return False
        
        # Swap the 2 columns
        for i in range(len(self.rows)):
            row = list(self.rows[i])
            swap = row[indx1 - 1]
            row[indx1 - 1] = row[indx2 - 1] 
            row[indx2 - 1] = swap
            self.rows[i] = tuple(row)

        return True
