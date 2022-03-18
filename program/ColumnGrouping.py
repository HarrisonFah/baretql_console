import operator
from Column import Column

def extract_all_cols(rows):
    '''
    Function that extracts all columns from a given set of rows.
    Assumes that all rows have same number of columns

    Arguments:
        rows: List of rows containing columns

    Returns:
        List of columns
    '''
    num_cols = len(rows[0])
    cols = []
    for i in range(num_cols):
        cols.append([])
        for row in rows:
            cols[i].append(row[i])
    return cols

def extract_value_cols(rows):
    '''
    Function that extracts all value columns (all columns except
    for the key column which is assumed to be the first) from a 
    given set of rows. Assumes that all rows have same number of columns.

    Arguments:
        rows: List of rows containing columns
    
    Returns:
        List of columns
    '''
    num_cols = len(rows[0])
    cols = []
    for i in range(1, num_cols):
        cols.append([])
        for row in rows:
            cols[i - 1].append(row[i])
        cols[i - 1] = Column(1, tuple(cols[i-1]))
    return cols

def extract_specific_cols(rows, inds):
    '''
    Function that extracts specific columns from a given set 
    of rows. Assumes that all rows have same number of columns.
    
    Arguments:
        rows: List of rows containing columns
        inds: List of the indexes of the columns to be extracted
    
    Returns:
        List of columns
    '''
    num_cols = len(rows[0])
    cols = []
    for i in range(len(inds)):
        cols.append([])
        for row in rows:
            cols[i].append(row[inds[i]])
    return cols

def group_columns(cols, grouping_threshold):
    '''
    Function that groups similar columns together. Two columns are
    similar if they have the same type and if they have identical
    entries above a certain threshold.

    Arguments:
        cols: Columns to be grouped
        grouping_threshold: The column(C) threshold for two columns in a 
            relational table to be considered in the same group.

    Returns:
        A list where each element in the list
        is a list of columns that are similar.
    '''
    num_cols = len(cols)
    if num_cols > 0:
        matches = {}
        for i in range(num_cols):
            # Initialize a group for cols[i] if none has been made yet
            if i not in matches:
                matches[i] = [i]
            # Iterate over the next columns
            for j in range(i + 1, num_cols):
                # If the columns have the same tag, calculate their
                # match score. Otherwise, score = 0
                if cols[i].get_tag() == cols[j].get_tag():
                    match = cols[i].compare_col(cols[j])
                else:
                    match = 0
                # If the columns match, then combine the two groups
                if match >= grouping_threshold:
                    if j not in matches[i]:
                        matches[i].append(j)
                    if j not in matches:
                        matches[j] = matches[i]
                    else:
                        if i not in matches[j]:
                            matches[j] += matches[i]
                            matches[j] = list(set(matches[j]))
                        matches[i] = matches[j]
        # Change the indices to the columns themselves and create a list
        # of all the groups
        groups = []
        ind_groups = []
        for ind_group in matches.values():
            if ind_group not in ind_groups:
                ind_groups.append(ind_group)
                group = []
                for ind in ind_group:
                    group.append(cols[ind])
                groups.append(group)
    else:
        groups = []

    return groups

def extract_grouped_cols(groups, operation, multiplier):
    '''
    Function that uses the sum rule or the product rule to extract the 
    representative column from each group.

    Arguments:
        groups: A list of elements where each element is a list of columns
            that are similar
        operation: The appropriate method to extract the representative column
        multiplier: The multiplier represents the smoothing multiplier for the 
            product rule. It is a nonzero multiplier that scales a result down 
            when it is not apparent in all the columns. Is not used in the sum 
            rule.
    
    Return: 
        The representative columns for each group.
    '''
    # Variable that contains the representative columns
    columns = []
    for group in groups:
        consolidated_col = []
        # Create a dictionary for each row entry that keeps track of the 
        # different possible value for that entry and their scores
        for row in range(group[0].get_num_entries()):
            consolidated_col.append({})
        # Iterate over each column and use the related score from the table 
        # the column was taken from for the entry it suggests
        for i in range(len(group)):
            column = group[i]
            rows = column.get_entries()
            # Sum Rule
            if operation == "sum":
                for j in range(len(rows)):
                    row = rows[j]
                    # Don't consider empty string
                    if row == "":
                        continue
                    # Take the sum of the scores
                    if row in consolidated_col[j]:
                        consolidated_col[j][row] += column.get_value()
                    else:
                        consolidated_col[j][row] = column.get_value()
            # Product Rule
            elif operation == "product":
                for j in range(len(rows)):
                    row = rows[j]
                    # Scale down all entry that are not key
                    for key, value in consolidated_col[j].items():
                        if key != row:
                            consolidated_col[j][key] *= multiplier
                    # Don't consider empty string
                    if row == "":
                        continue
                    # Take the product of the entries (if the entry was not
                    # there than use the multiplier)
                    if row in consolidated_col[j]:
                        consolidated_col[j][row] *= column.get_value()
                    else:
                        consolidated_col[j][row] = column.get_value() * (multiplier ** i)
            else:
                print("INVALID OPERATION TYPE")
                return False

        # If the column is all empty then do not add it's representative 
        # column
        all_none = True
        for i in range(group[0].get_num_entries()):
            if len(consolidated_col[i]) == 0:
                consolidated_col[i][""] = 1
            else:
                all_none = False
                break
        if all_none:
            continue
        
        # Create the column where each entry is the highest scoring entry
        # for that row
        column = []
        for row_entries in consolidated_col:
            if row_entries != {}:
                row_entry = sorted(row_entries.items(), 
                            key=operator.itemgetter(1), 
                            reverse=True)[0][0]
            else:
                row_entry = ''
            column.append(row_entry)
        columns.append(column)
    
    return columns

def test_group():
    '''
    Test function
    '''
    rows = [("John", "Male", "21", "UofA"),
            ("Mary", "Female", "22", "UofA"),
            ("Jeff", "Male", "20", "UofC"),
            ("Sarah", "Female", "24", "McGill")]
    '''
    print(extract_all_cols(rows))
    print("----------------------------")
    print(extract_value_cols(rows))
    print("----------------------------")
    print(extract_specific_cols(rows, [1,3]))
    '''
    cols = [
        ('21', '22', '20'), ('None', 'Female', 'Male'), 
        ('UofA', 'UofA', 'UofA'), ('Male', 'None', 'None'),
        ('UofA', 'UofA', 'UofA'), ('Male', 'None', 'Male'), 
        ('UofA', 'None', 'UofA')
    ]
    cols = group_columns(cols)
    print(cols)

if __name__ == "__main__":
    test_group()