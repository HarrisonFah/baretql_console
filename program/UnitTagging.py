import itertools
import operator
try:
    # Try to import date util module but if they don't have it then set 
    # date_check to False to avoid Import Error
    from dateutil.parser import parse
    date_check = True
except ImportError:
    print(
        "WARNING:\n"
        + "You do not have the 'python-dateutil' module downloaded.\n"
        + "The 'python-dateutil' module is a recommended module because\n"
        + "it allows BareTQL to tag columns as 'date'. This can\n"
        + "have an effect when calling .xc on a query table.\n")  
    date_check = False

def tag_columns(columns):
    '''
    Function that tags a set of columns as either "numeric", "text", "date"
    or "None".

    Arguments:
        columns: Columns to be tagged

    Returns: 
        A dictionary where they keys consist of the columns given as an 
        argument and the value is the tag
    '''
    columns_with_tags = {}
    for column in columns:
        unit_tag = tag_unit_for_column(column)
        columns_with_tags[column] = unit_tag
    return columns_with_tags

def tag_unit_for_column(column):
    '''
    Function that tags a particular column as either "numeric", 
    "text", "date" or "None".

    Arguments:
        column: Column to be tagged

    Returns:
        The tag given to this column
    '''
    value_tag_list = []

    # Tag every entry in the column as "numeric", "text", "date" or "None"
    for value in column:
        if value != "":
            value_tag = tag_unit_for_value(value)
            value_tag_list.append(value_tag)

    # Take most common tag or if there were no tags, give the tag "None"
    if len(value_tag_list) > 0:
        finalTag = most_common(value_tag_list)
    else:
        finalTag = "None"
    return finalTag

def tag_unit_for_value(value):
    '''
    Function that tags a particular value as either "numeric", "text",
    "date" or "None"

    Arguments:
        value: Value to be tagged

    Return:
        Returns the tag
    '''
    global date_check
    # Check if the value is date
    word_count = len(
        (value.replace("/"," ").replace("-"," ").replace(".","").replace(",","")).split(" "))
    if (word_count > 1) and date_check:
        date_result = is_date(value)
        if (date_result == True):
            return "date"

    # Check if the value is numeric
    float_result = is_float(value)
    if float_result:
        return "numeric"

    # If not numeric and not date, then the value is considered text
    return "text"

def is_date(string):
    '''
    Function that determines whether a given string is of type date.
    '''
    try:
        parse(string)
        return True
    except ValueError:
        return False

# https://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float
# taken on 2018-05-15
def is_float(string):
    '''
    Function that determines whether a given string is of type float.
    '''
    try:
        float(string)
        return True
    except ValueError:
        return False

# https://stackoverflow.com/questions/1518522/find-the-most-common-element-in-a-list
# taken on 2018-05-15
def most_common(L):
    '''
    Function that determines the most common occurence of a number in a list.
    
    Arguments:
        L: List of elements
    
    Returns: 
        Most common element
    '''
    # get an iterable of (item, iterable) pairs
    sl = sorted((x, i) for i, x in enumerate(L))
    # print 'sl:', sl
    groups = itertools.groupby(sl, key=operator.itemgetter(0))
    # auxiliary function to get "quality" for an item
    def _auxfun(g):
        item, iterable = g
        count = 0
        min_index = len(L)
        for _, where in iterable:
            count += 1
            min_index = min(min_index, where)
        # print 'item %r, count %r, minind %r' % (item, count, min_index)
        return count, -min_index
    # pick the highest-count/earliest item
    return max(groups, key=_auxfun)[0]

def test_tagging():
    '''
    Test function
    '''
    cols = [
        ('21', '22', '20'), ('None', 'Female', 'Male'), 
        ('UofA', 'UofA', 'UofA'), ('Male', 'None', 'None'), 
        ('UofA', 'UofA', 'UofA'), ('Male', 'None', 'Male'), 
        ('UofA', 'None', 'UofA')
    ]    
    #cols = tag_columns(cols)
    col1 = ("None", "1941")
    col2 = ("1941", "None")
    col3 = ("02/10/2018", "05/06/2018")
    col4 = ("February 23, 2018", "October 6, 2018")
    print(tag_unit_for_column(col1))
    print(tag_unit_for_column(col2))
    print(tag_unit_for_column(col3))
    print(tag_unit_for_column(col4))  
    #print(cols)

if __name__ == "__main__":
    test_tagging()