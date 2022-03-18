import math
import sys
import ApproxMatch
import baretql
from R import R

def get_column(rows, columnIndexes):
    '''
    Generates a list of column entries given rows and column indexes

    Arguments:
        rows: A list of rows from a table
        columnIndexes: A list of column indexes, if more than one column index in the list then column entries are concatenated together

    Returns:
        A list of column entries
    '''
    colData = []
    for row in rows:
        currentCell = ""
        for index in columnIndexes:
            try:
                #add cell if it exists
                cell = row[index].lower()
                if currentCell == "":
                    currentCell += cell
                else:
                    #concatenate together column entires using obscure unicode character
                    currentCell += '\u254d' + cell
            except:
                pass
        colData.append(currentCell)
    return colData

def get_qgrams(q,s):
    '''
    Finds the set of ngrams for a given string and length

    Arguments:
        q: the length of the ngram
        s: the given string

    Returns:
        A set of ngrams for the string
    '''
    res = set()
    assert q > 0
    if q > len(s):
        return res
    if q == len(s):
        res.add(s)
        return res

    end = len(s) - q + 1

    for i in range(end):
        res.add(s[i:i+q])

    return res

def jaccard_similarity(table_list, qry_table, columnGroups, exactMatchIndex=None, ngramIndex=None, ngramSize=None, b=1.2, k=0.75):
    '''
    Used to test the results of the jaccard similarity on single column tables
    This is not implemented properly for multi-column tables. Do not use
    '''
    
    matching_tables = {} #Stores all the tables that join on the query table

    #Get the full column lists for each column group
    qry_rows = qry_table.get_rows()
    qry_columns = []
    qry_ngrams = set()
    for group in columnGroups:
        #for each index in the mandatory columns, get all the row entries for that column
        qry_columns.append((get_column(qry_rows, columnGroups[group]), group))
    for column, qryColIndex in qry_columns:
        for entry in column:
        	ngrams = get_qgrams(ngramSize, entry)
	        for ngram in ngrams:
	            qry_ngrams.add(ngram)
    index = ngramIndex

    table_ngrams = {}
    for term in index:
        for table in index[term]:
            if table in table_ngrams:
                table_ngrams[table].add(term)
            else:
                table_ngrams[table] = set()
                table_ngrams[table].add(term)
    
    for table in table_ngrams:
        matching_tables[table] = len(qry_ngrams.intersection(table_ngrams[table])) / len(qry_ngrams.union(table_ngrams[table]))

    sortedTables = sorted(matching_tables.items(), key = lambda i: i[1], reverse=True)
    return [(table_list[item[0]], item[1]) for item in sortedTables]

def get_matching_tables(table_list, qry_table, columnGroups, exactMatchIndex=None, ngramIndex=None, ngramSize=None, b=1.2, k=0.75):
    '''
    Finds tables that join the given table on the selected columns using a modified BM25 approach.
    
    Arguments:
        table_list: List that stores all the tables in baretql
        qry_table: The table that is being joined on
        columnGroups: A dictionary of groups and the column indices that belong in it
        exactMatchIndex: The index used for joining on exact matches
        ngramIndex: The index used for joining on n-gram matches
        ngramSize: The size of n-grams used for n-gram matching
        b: parameter in BM25
        k: parameter in BM25

    Returns: A list of tuples of matching tables and column pairs sorted by score
    '''

    matching_tables = {} #Stores all the tables that join on the query table

    #Get the full column lists for each column group
    qry_rows = qry_table.get_rows()
    qry_columns = []
    for group in columnGroups:
        #for each index in the mandatory columns, get all the row entries for that column
        qry_columns.append((get_column(qry_rows, columnGroups[group]), group))

    #the average number of characters or ngrams in a column
    avg_doc_length = baretql.get_avg_document_length(ngramIndex!=None)

    #calculates the total number of rows in the dataset
    total_num_rows = 0
    for table in R.table_list:
        total_num_rows += table.get_num_rows()

    column_lengths = {} #used to store the number of rows in each column so that they do not have to be recalculated each time

    for column, qryColIndex in qry_columns:
        #generate the weighting for the terms in the current column (n-gram or exact match)

        current_column_scores = {} #stores the scores for each column for this qry_column

        #stores all the entries/ngrams in the current query column and where they occur in the query column
        qry_terms = [] #store tuples of terms and their weightings
        qry_occurrences = {} #stores the row where the term occurs
        entry_index = 0
        for entry in column:
            #if using exact matching
            if ngramIndex is None:
                qry_terms.append((entry,1)) #add the current entry and a term weight of 1
                if entry not in qry_occurrences:
                    qry_occurrences[entry] = [entry_index]
                else:
                    qry_occurrences[entry].append(entry_index)
            #if using ngram matching
            else:
                #calculate the weighting for each row by finding the total number of ngrams
                entry_size = 0
                for field in entry.split('\u254d'):
                    entry_size += max(1, len(field) - ngramSize + 1)
                for field in entry.split('\u254d'):
                    #if the current word is smaller than the ngram size, use the whole term
                    if len(field) < ngramSize:
                        qry_terms.append((field,1/entry_size)) #add the current ngram and a term weight of 1/row_length
                        if field not in qry_occurrences:
                            qry_occurrences[field] = [entry_index]
                        elif entry_index not in qry_occurrences[field]:
                            qry_occurrences[field].append(entry_index)
                    #find the ngrams and treat each one as a term
                    else:
                        ngrams = get_qgrams(ngramSize, field)
                        for ngram in ngrams:
                            qry_terms.append((ngram,1/entry_size)) #add the current ngram and a term weight of 1/row_length
                            if ngram not in qry_occurrences:
                                qry_occurrences[ngram] = [entry_index]
                            elif entry_index not in qry_occurrences[ngram]:
                                qry_occurrences[ngram].append(entry_index)
            entry_index += 1

        #for each term, find columns that contain it and calculate their BM25 score
        term_index = 0
        for term, term_weight in qry_terms:
            term_row_count = 1 #counts the number of columns in the collection that contain the current term

            #for each term, check the index for tables where the term is found and create a occurrence dictionary for each table
            table_column_occurences = {}
            if ngramIndex is None:
                index = exactMatchIndex
            else:
                index = ngramIndex
            if term in index:
                for table in index[term]:
                    #for each table, get all row,column pairs that contain the ngram
                    for rowColumn in index[term][table]:
                        term_row_count += 1
                        if table not in table_column_occurences:
                            table_column_occurences[table] = {rowColumn[1]:1}
                        elif rowColumn[1] not in table_column_occurences[table]:
                            table_column_occurences[table][rowColumn[1]] = 1
                        else:
                            table_column_occurences[table][rowColumn[1]] += 1

            #calculate the query column IDF value and the data column IDF value
            qry_term_count = len(qry_occurrences[term])
            qry_idf = math.log((len(column) - qry_term_count + 0.5)/(qry_term_count + 0.5) + 1)
            data_idf = math.log((total_num_rows - term_row_count + 0.5)/(term_row_count + 0.5) + 1)

            #use the term frequencies to calculate the score for each column
            for table in table_column_occurences:
                for data_column in table_column_occurences[table]:
                    #for each column, get the number of rows and document length from the table or the dictionary
                    if table not in column_lengths:
                        data_table = baretql.get_table_by_index(table)
                        num_rows = data_table.get_col_size(data_column)
                        if ngramIndex == None or data_table.get_avg_col_char_size(data_column) < ngramSize:
                            document_length = num_rows
                        else:
                            avg_col_size = data_table.get_avg_col_char_size(data_column)
                            document_length = num_rows*(avg_col_size - ngramSize + 1)
                        column_lengths[table] = {data_column:document_length}
                    elif data_column not in column_lengths[table]:
                        data_table = baretql.get_table_by_index(table)
                        num_rows = data_table.get_col_size(data_column)
                        if ngramIndex == None or data_table.get_avg_col_char_size(data_column) < ngramSize:
                            document_length = num_rows
                        else:
                            avg_col_size = data_table.get_avg_col_char_size(data_column)
                            document_length = num_rows*(avg_col_size - ngramSize + 1)
                        column_lengths[table][data_column] = document_length
                    else:
                        document_length = column_lengths[table][data_column]

                    tf = table_column_occurences[table][data_column] #use the raw count as term freuency
                    term_score_numerator = tf*(k+1)
                    term_score_denominator = tf + k * (1 - b + b*(document_length/avg_doc_length))
                    term_score = term_weight*qry_idf*data_idf*term_score_numerator/term_score_denominator

                    #update the total score for each column
                    if table not in current_column_scores:
                        current_column_scores[table] = {data_column:term_score}
                    elif data_column not in current_column_scores[table]:
                        current_column_scores[table][data_column] = term_score
                    else:
                        current_column_scores[table][data_column] += term_score
            term_index += 1

        #retrieve the max bm25 score for each table by looking at each column
        max_scores = {}
        for table in current_column_scores:
            current_max, max_index = -1, -1
            for data_column in current_column_scores[table]:
                if current_column_scores[table][data_column] > current_max:
                    current_max, max_index = current_column_scores[table][data_column], data_column
            if current_max > -1:
                max_scores[table] = (current_max, max_index)

        #if this is the first query column then set the score to equal the current column score, otherwise use multiplicative scoring
        for table in max_scores:
            if table not in matching_tables:
                matching_tables[table] = [max_scores[table][0], {qryColIndex:max_scores[table][1]}]
            else:
                matching_tables[table][0] *= max_scores[table][0]
                matching_tables[table][1][qryColIndex] = max_scores[table][1]

    #sort tables by score then return tables with their column matchings
    sortedTables = sorted(matching_tables.items(), key = lambda i: i[1][0], reverse=True)
    return [(table_list[item[0]], item[1][1]) for item in sortedTables]
