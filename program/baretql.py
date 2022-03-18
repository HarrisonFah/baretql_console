import sys
import os
import csv
import re
import time
import math
import json
import configparser
import string
import traceback
import TableSearch
import re
from datetime import datetime
from R import R

config = configparser.ConfigParser()
config.read('config.ini')
col_mat_base = config['parameters']['col-mat-base']
col_mat_dir = config['parameters']['col-mat-dir']
col_mat_config = config['parameters']['col-mat-config']
sys.path.append(col_mat_base + col_mat_dir)
from data_processor import Matcher
from pattern import Finder
import main as ColumnMatcher
from Transformation.Pattern import Pattern
from Transformation.Blocks.LiteralPatternBlock import LiteralPatternBlock
from Transformation.Blocks.PositionPatternBlock import PositionPatternBlock
from Transformation.Blocks.TokenPatternBlock import TokenPatternBlock
from Transformation.Blocks.SplitSubstrPatternBlock import SplitSubstrPatternBlock
from Transformation.Blocks.TwoCharSplitSubstrPatternBlock import TwoCharSplitSubstrPatternBlock

num_tables = 0
user_inp_tables = {}
recent_table_search = None

def handle_search(line):
    '''
    Handles the user input and processes keyword or table search if entered.

    Arguments:
        line: the user inputted line containing the command

    Returns:
        a boolean stating whether or not the command was keyword/table search and a result table of the search, None if no singular table to return
    '''

    #The parameters for BM-25
    global b 
    global k

    lower = line.lower()
    if len(lower) >= 14 and lower[0:14] == "keyword_search":
        #create 'keyword_search' command for searching for a set of keywords in the database
        formats = [
            "keyword_search\(\"(.*)\",\s*(.*)\)",
            "keyword_search\('(.*)',\s*(.*)\)",
            "keyword_search\(\"(.*)\"\)",
            "keyword_search\('(.*)'\)"
        ]
        correct_format = False
        split_line = line.split(".")
        line = split_line[0]
        try:
            index = split_line[1]
            try:
                index = int(index)
            except:
                print("Error: Index must be an integer greater than or equal to 0.")
                index = None
        except:
            index = None
        for re_format in formats:
            result = re.search(re_format, lower)
            if result is not None:
                keywords = result.group(1).strip(" ")
                if ("," in re_format):
                    try:
                        numResults = int(result.group(2).strip(" "))
                        if numResults < 0:
                            print("Error: numResults should be greater than or equal to 0.")
                            return True, None
                        return keyword_search(keywords.split(","), numResults, index=index, b=b, k=k)
                        correct_format = True
                    except Exception as e:
                        print("Error: numResults should be an integer.")
                        return True, None
                else:
                    correct_format = True
                    return keyword_search(keywords.split(","), index=index, b=b, k=k)
                break                            
        if not correct_format:
            print("Keyword search command must be formatted as: ")
            print("\t -keyword_search(\"*key1, key2*\")")
            print("\t -keyword_search(\"*key1, key2*\", *numResults*)")
        return True, None
    elif len(lower) >= 14 and lower[0:12] == "table_search":
        #create 'table_search' command for searching for a set of keywords in the database
        formats = [
            "table_search\((.*),\s*\"(.*)\"\,\s*\"(.*)\"\)",
            "table_search\((.*),\s*\'(.*)\'\,\s*\'(.*)\'\)",
            "table_search\((.*),\s*\'(.*)\'\,\s*\"(.*)\"\)",
            "table_search\((.*),\s*\"(.*)\"\,\s*\'(.*)\'\)",
            "table_search\((.*),\s*\"(.*)\",\s*\"(.*)\",\s*(.*)\)",
            "table_search\((.*),\s*\'(.*)\',\s*\'(.*)\',\s*(.*)\)",
            "table_search\((.*),\s*\"(.*)\",\s*\'(.*)\',\s*(.*)\)",
            "table_search\((.*),\s*\'(.*)\',\s*\"(.*)\",\s*(.*)\)"
        ]
        correct_format = False
        split_line = line.split(".")
        line = split_line[0]
        try:
            index = split_line[1]
            try:
                index = int(index)
            except:
                print("Error: Index must be an integer greater than or equal to 0.")
                index = None
        except:
            index = None
        for re_format in formats:
            result = re.search(re_format, line)
            if result is not None:
                tableName = result.group(1).strip(" ")
                if (re_format.count(",") == 2):
                    searchType = result.group(2).strip(" ").lower()
                    columnPrio = result.group(3).strip(" ").lower()
                    return True, table_search(tableName, searchType, columnPrio, index=index, b=b, k=k)
                    correct_format = True
                elif (re_format.count(",") == 3):
                    searchType = result.group(2).strip(" ").lower()
                    columnPrio = result.group(3).strip(" ").lower()
                    try:
                        numResults = int(result.group(4).strip(" "))
                        if numResults < 0:
                            print("Error: numResults should be greater than or equal to 0.")
                            return True, None
                        correct_format = True
                        return True, table_search(tableName, searchType, columnPrio, numResults, index=index, b=b, k=k)
                    except Exception as e:
                        print("Error: numResults should be an integer.")
                        return True, None
                break                            
        if not correct_format:
            print("Table search command must be formatted as: ")
            print("\t -table_search(*tableName*, \"*approach*\", \"columnGroups\")")
            print("\t -table_search(*tableName*, \"*approach*\", \"columnGroups\", *numResults*)")
        return True, None
    return False, None

def concatenate_columns(table, columns):
    '''
    Concatenates together the given columns into one column and returns the resulting table.

    Arguments:
        table: the table being performed on
        columns: a list of column indices to concatenate together

    Returns:
        A table with the given columns concatenated together.
    '''
    new_table = []
    for entry in table.get_rows():
        min_column = min(columns)
        concat_column = ""
        current_row = []
        for column_index in range(len(entry)):
            column = entry[column_index]
            if column_index in columns:
                if concat_column == "":
                    concat_column += column
                else:
                    concat_column += '\u254d' + column
            else:
                current_row.append(column)
        current_row.insert(min_column, concat_column)
        new_table.append(tuple(current_row))
    return R(new_table)

def create_cm_table(srcTable, tgtTable, matchedColumns):
    '''
    Creates a dictionary matching the format required from column-matcher functions

    Arguments:
        srcTable: The designated source table
        tgtTable: The designated target table
        matchedColumns: A dictionary containing srcColumn:tgtColumn pairs

    Returns:
        A dictionary that can be used in column-matcher functions
    '''
    tables = {'tableJoin':{}}

    src = {'name': 'src_tableJoin'}
    srcColumns = []
    for i in range(srcTable.get_num_cols()):
        srcColumns.append("src_col_" + str(i))
    src['titles'] = srcColumns
    src['items'] = [list([entry.lower() for entry in srcRow]) for srcRow in srcTable.get_rows()]
    tables['tableJoin']['src'] = src

    tgt = {'name': 'target_tableJoin'}
    tgtColumns = []
    for i in range(tgtTable.get_num_cols()):
        tgtColumns.append("tgt_col_" + str(i))
    tgt['titles'] = tgtColumns
    tgt['items'] = [list([entry.lower() for entry in tgtRow]) for tgtRow in tgtTable.get_rows()]
    tables['tableJoin']['target'] = tgt

    tables['tableJoin']['name'] = 'tableJoin'
    srcCol = list(matchedColumns)[0]
    tables['tableJoin']['rows'] = {'src': 'src_col_' + str(srcCol), 'target': 'tgt_col_' + str(matchedColumns[srcCol])}
    tables['tableJoin']['source_col'] = 'source'

    return tables


def column_matcher(tables, exportFile=None, swapSrcTarget=True):
    '''
    Generate and print/return a list of ranked transformations from a source column to a target column

    Arguments:
        tables: a dictionary following the specified column-matcher format or by using create_cm_table() above
        exportFile: the name of a file to export the transformations to if input
    '''

    #global variables in column-matcher/src/main.py here temporarily
    ROW_MATCHING_N_START = 4
    ROW_MATCHING_N_END = 20
    SWAP_SRC_TARGET = swapSrcTarget
    verbose = False
    PT_PARAMS = {
        'max_tokens': 3,  # maximum number of allowed placeholders
        'max_blocks': 3,  # maximum number of allowed blocks (either placeholder or literal)
        'generalize': False,

        'token_splitters': [' ', ],  # set to None to disable. Break placeholders into other placeholders based on these chars
        'remove_duplicate_patterns': True,  # After generating all possible transformation, delete the duplicates, may take time
        'switch_literals_placeholders': True,  # Replace placeholder with literals and add them as new pattern
        'only_first_match': False,  # Take only first match for the placeholder or look for all of possible matches.

        'units_to_extract': [LiteralPatternBlock, PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock],
        # literal must be included
        # 'units_to_extract': [PositionPatternBlock, TokenPatternBlock, LiteralPatternBlock],  # literal must be included
        # 'units_to_extract': [LiteralPatternBlock, PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock, TwoCharSplitSubstrPatternBlock],  # not including literal

    }
    params_diff_list = ['units_to_extract']
    cnf_path = col_mat_base + col_mat_config

    tf_output = []

    #get the parameters from the json file and update the PT_PARAMS dictionary
    with open(cnf_path, "r") as f:
        cnf = json.load(f)
    pt_params_cnf = cnf.get('pt_params', {})
    for key in PT_PARAMS:
        if key not in params_diff_list and key in pt_params_cnf:
            PT_PARAMS[key] = pt_params_cnf[key]
    if 'units_to_extract' in pt_params_cnf:
        tmp = []
        for unit in pt_params_cnf['units_to_extract']:
            tmp.append(globals()[unit])
        PT_PARAMS['units_to_extract'] = tmp

    res = Matcher.get_matching_tables_golden(tables, bidi=False)
    for item in res['items']:
        #generate row matchings
        try:
            rows, is_swapped = Matcher.get_matching_rows(tables, item, ROW_MATCHING_N_START, ROW_MATCHING_N_END, swap_src_target=SWAP_SRC_TARGET)
        except:
            print("Error matching rows, rows in a table must all have the same number of columns.")
        #generate patterns
        new_res = Finder.get_patterns(rows, params=PT_PARAMS, table_name=item['src_table'][4:], verbose=verbose)
        transformations = {} #store a dictionary of transformation strings and the rows it applies to
        #print out ranked patterns
        tf_output.append("-"*20 + "\n")
        tf_output.append("Coverage: " + str(new_res["covered"]) + "/" + str(new_res["input_len"]) + "\n")
        tf_output.append("Ranked Transformations:" + "\n")
        rank = 0
        for patterns in new_res['ranked']:
            rank += 1
            tf_output.append("(" + str(rank) + ") ")
            operation_index = 0
            blocks_list = patterns[2].blocks
            current_transformation = ''
            #for each pattern generate a string using the blocks/units
            for operation in blocks_list:
                operation_index += 1
                if operation_index < len(blocks_list):
                    end_token = " + "
                else:
                    end_token = " "
                if type(operation) is LiteralPatternBlock:
                    current_transformation += "Literal('" + operation.text + "')" + end_token
                    tf_output.append("Literal('" + operation.text + "')" + end_token)
                elif type(operation) is TokenPatternBlock:
                    current_transformation += "Split('" + operation.splitter + "', " + str(operation.index) + ")" + end_token
                    tf_output.append("Split('" + operation.splitter + "', " + str(operation.index) + ")" + end_token)
                elif type(operation) is PositionPatternBlock:
                    current_transformation += "Substring(" + str(operation.start) + ", " + str(operation.end) + ")" + end_token
                    tf_output.append("Substring(" + str(operation.start) + ", " + str(operation.end) + ")" + end_token)
                elif type(operation) is SplitSubstrPatternBlock:
                    current_transformation += "SplitSubstr('" + operation.splitter + "', " + str(operation.index) + ", " + str(operation.start) + ", " + str(operation.end) + ")" + end_token
                    tf_output.append("SplitSubstr('" + operation.splitter + "', " + str(operation.index) + ", " + str(operation.start) + ", " + str(operation.end) + ")" + end_token)
                else:
                    current_transformation += "UnkownTransformation" + end_token
                    tf_output.append("Error: Unkown transformation" + end_token)
            transformations[current_transformation] = (str(patterns[1]) + "/" + str(new_res["input_len"]), [])
            tf_output.append("[Coverage: " + str(patterns[1]) + "/" + str(new_res["input_len"]) + "]:" + "\n")
            #print out transformation examples and add them to the transformation dictionary
            for transformation in patterns[3]:
                transformations[current_transformation][1].append(str(transformation[0]) + " --> " + str(transformation[1]))
                tf_output.append("\t" + str(transformation[0]) + " --> " + str(transformation[1]) + "\n")
        #export the transformations to a text file
        if exportFile is not None:
            export_transformations(transformations, str(new_res["covered"]) + "/" + str(new_res["input_len"]), is_swapped, tables['tableJoin']['rows'], exportFile)
        #ColumnMatcher.pt_print(new_res, item['src_table'][4:], None, None)
        return tf_output

def export_transformations(transformations, coverageString, isSwapped, matchedColumns, fileName):
    '''
    Exports a list of transformations to a text file.

    Arguments:
        transformations: a dictionary with transformation strings as the key and a list of the coverage and examples as the value
        coverageString: the fraction of rows that are covered by transformations
        isSwapped: a boolean representing if the transformations go from src->tgt or tgt->src
        matchedColumns: a dictionary containing the indices of the src and tgt tables that were transformed
        fileName: the name of the file to be exported to
    '''

    global num_tf_examples
    try:
        with open(fileName, "a", encoding="UTF-8") as f:
            src_col = matchedColumns['src'].split("src_col_")[1]
            tgt_col = matchedColumns['target'].split("tgt_col_")[1]
            if isSwapped:
                f.write("# TGT[" + tgt_col + "] --> SRC[" + src_col + "]\n")
            else:
                f.write("# SRC[" + src_col + "] --> TGT[" + tgt_col + "]\n")
            f.write("# total support: " + coverageString + "\n\n")
            for tf in transformations:
                f.write(tf + "\n")
                f.write("# support: " + transformations[tf][0] + "\n")
                f.write("# examples: \n")
                for i in range(min(len(transformations[tf][1]), num_tf_examples)):
                    f.write("# " + transformations[tf][1][i] + "\n")
                f.write("\n")
            f.write("-"*40 + "\n")
    except Exception as e:
        print("Error exporting transformations:")
        print(e)

def import_transformations(fileName, srcCol, tgtCol):
    '''
    Reads the transformations from a file for a src and tgt column and returns them.

    Arguments:
        fileName: the name of the file to read from
        srcCol: the index of the src table column
        tgtCol: the index of the tgt table column

    Returns:
        A dictionary of transformations and their coverage as well as a boolean stating the direction of the transformations.
    '''

    is_swapped = False
    transformations = {}
    try:
        with open(fileName, "r", encoding="UTF-8") as f:
            fileText = f.read()
            columnTransformations = fileText.split("-"*40 + "\n")
            for transformation_text in columnTransformations:
                header = transformation_text.split("\n")[0].strip("\n")
                formats = [
                    ("# TGT\[(.*)\] --> SRC\[(.*)\]", "tgt_first"),
                    ("# SRC\[(.*)\] --> TGT\[(.*)\]", "src_first")
                ]
                #correct_format = False
                for re_format, first in formats:
                    result = re.search(re_format, header)
                    if result is not None:
                        try:
                            header_first_col = int(result.group(1))
                            header_second_col = int(result.group(2))
                            if first == "tgt_first":
                                if header_first_col != tgtCol or header_second_col != srcCol:
                                    break
                                is_swapped = True
                            else:
                                if header_first_col != srcCol or header_second_col != tgtCol:
                                    break
                            currentBlocks = []
                            currentSupport = None
                            for line in transformation_text.split("\n")[3:]:
                                line = line.strip("\n")
                                if line == "":
                                    currentPattern = Pattern(currentBlocks)
                                    transformations[currentPattern] = currentSupport
                                    currentBlocks = []
                                    currentPattern = 0
                                elif line[0] == "#":
                                    if "support" in line:
                                        currentSupport = line.split(" ")[2].strip("\n")
                                else:
                                    for block in line.split("+"):
                                        blockString = block.strip(" ")
                                        if blockString[:7] == "Literal":
                                            text = blockString[9:(len(blockString)-2)]
                                            currentBlocks.append(LiteralPatternBlock(text))
                                        elif blockString[:11] == "SplitSubstr":
                                            result = re.search("SplitSubstr\('(.*)',\s*(.*),\s*(.*),\s*(.*)\)", blockString)
                                            splitter = result.group(1)
                                            index = int(result.group(2))
                                            start = int(result.group(3))
                                            end = int(result.group(4))
                                            currentBlocks.append(SplitSubstrPatternBlock(splitter, index, start, end))
                                        elif blockString[:5] == "Split":
                                            result = re.search("Split\('(.*)',\s*(.*)\)", blockString)
                                            splitter = result.group(1)
                                            if "[" in result.group(2):
                                                index = []
                                                for entry in result.group(2).strip("[]").split(", "):
                                                    index.append(int(entry))
                                            else:
                                                index = int(result.group(2))
                                            currentBlocks.append(TokenPatternBlock(splitter, index))
                                        elif blockString[:9] == "Substring":
                                            result = re.search("Substring\((.*),\s*(.*)\)", blockString)
                                            start = int(result.group(1))
                                            end = int(result.group(2))
                                            currentBlocks.append(PositionPatternBlock(start, end))
                            return transformations, is_swapped
                        except Exception as e:
                            pass
    except Exception as e:
        print("Error: Transformation file either does not exist or file is formatted improperly.")
    return (None, None)

def joinTablesOnTransform(src_table, tgt_table, matchedColumns, transformations=None):
    '''
    Joins together two tables on the given columns using transformations.

    Arguments:
        src_table: the left table
        tgt_table: the right table
        matchedColumns: a dictionary of src_column:tgt_column index pairs to join on
        transformations: a dictionary of the transformations for src_column and whether or not the src and tgt are swapped

    Returns:
        The resulting joined tables.
    '''
    global user_inp_tables
    ROW_MATCHING_N_START = 4
    ROW_MATCHING_N_END = 20
    SWAP_SRC_TARGET = True
    verbose = False
    PT_PARAMS = {
        'max_tokens': 3,  # maximum number of allowed placeholders
        'max_blocks': 3,  # maximum number of allowed blocks (either placeholder or literal)
        'generalize': False,

        'token_splitters': [' ', ],  # set to None to disable. Break placeholders into other placeholders based on these chars
        'remove_duplicate_patterns': True,  # After generating all possible transformation, delete the duplicates, may take time
        'switch_literals_placeholders': True,  # Replace placeholder with literals and add them as new pattern
        'only_first_match': False,  # Take only first match for the placeholder or look for all of possible matches.

        'units_to_extract': [LiteralPatternBlock, PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock],
        # literal must be included
        # 'units_to_extract': [PositionPatternBlock, TokenPatternBlock, LiteralPatternBlock],  # literal must be included
        # 'units_to_extract': [LiteralPatternBlock, PositionPatternBlock, TokenPatternBlock, SplitSubstrPatternBlock, TwoCharSplitSubstrPatternBlock],  # not including literal

    }
    params_diff_list = ['units_to_extract']
    cnf_path = col_mat_base + col_mat_config
    ignore_literal_and_single = True

    now = datetime.now().time()
    current_time = str(now).replace(":", "").replace(".", "")
    exportFileName = "temp_transformations/export_transformations_" + current_time + ".txt"

    first_column = True #the first column will not perform an intersection with the current row matchings
    row_matchings = {} #contains the row matchings and the columns/transformations that support it
    #for each column pair, apply transformations and find intersected row matchings with other column pairs
    for src_column in matchedColumns:
        tgt_column = matchedColumns[src_column]

        #if no transformations were given then generate transformations
        if transformations is None:
            #generate transformations
            cm_tables = create_cm_table(src_table, tgt_table, {src_column:tgt_column})
            print("No transformations given, generating transformations under " + exportFileName)
            column_matcher(cm_tables, exportFile=exportFileName, swapSrcTarget=SWAP_SRC_TARGET)
            current_transformations, is_swapped = import_transformations(exportFileName, src_column, tgt_column)
        else:
            current_transformations, is_swapped = transformations[src_column]

        #swap the columns and tables if specified by the transformation
        if is_swapped:
            src_column, tgt_column = tgt_column, src_column
            src_tables_rows = tgt_table.get_rows()
            tgt_tables_rows = src_table.get_rows()
        else:
            src_tables_rows = src_table.get_rows()
            tgt_tables_rows = tgt_table.get_rows()

        transformed_entries = []
        #apply transformation to every row of the srcTable
        for row_index in range(len(src_tables_rows)):
            row = list(src_tables_rows[row_index])
            transformed_entries.append((row_index, row[src_column], None))
            for tf in current_transformations:
                #if the transformation only covers 1 row or contains only a literal then ignore it
                if ignore_literal_and_single and current_transformations[tf] != None and (int(current_transformations[tf].split("/")[0]) < 2 or (len(tf.blocks) == 1 and type(tf.blocks[0]) is LiteralPatternBlock)):
                    continue
                applied_tf = tf.apply(row[src_column].lower())
                if applied_tf is not None:
                    #if pattern can be applied to this row then store the updated row in intermediate table
                    transformed_entries.append((row_index, applied_tf, tf))
        #join transformed entries with tgt table and save the row matchings
        current_row_matchings = {}
        for left_row_index in range(len(tgt_tables_rows)):
            left_row = tgt_tables_rows[left_row_index]
            left_join_value = left_row[tgt_column].lower()
            for right_row in transformed_entries:
                right_join_value = right_row[1].lower()
                if left_join_value == right_join_value:
                    #if the values in the join columns match then concatenate rows and add to list
                    if is_swapped:
                        current_row_matchings[(right_row[0], left_row_index)] = [(tgt_column, src_column, right_row[2])]
                    else:
                        current_row_matchings[(left_row_index, right_row[0])] = [(src_column, tgt_column, right_row[2])]
        #perform the intersection between current row matchings and past row matchings
        if first_column:
            row_matchings = current_row_matchings
            first_column = False
        else:
            intersected_rows = set(row_matchings.keys()) & set(current_row_matchings.keys())
            new_row_matchings = {}
            for row in intersected_rows:
                new_row_matchings[row] = row_matchings[row] + current_row_matchings[row]
            row_matchings = new_row_matchings

    #generate a table out of the resulting row matchings
    src_tables_rows = src_table.get_rows()
    tgt_tables_rows = tgt_table.get_rows()
    end_table = []
    for row_pair in row_matchings:
        left_index = row_pair[0]
        right_index = row_pair[1]
        current_row = list(src_tables_rows[right_index]) + list(tgt_tables_rows[left_index])
        for match in row_matchings[row_pair]:
            tf = match[2]
            current_row.append(str(tf))
        end_table.append(tuple(current_row))

    return R(end_table)

def keyword_search(keywords, numResults=None, index=None, b=1.2, k=0.75):
    '''
    Function used to search tables for one or more keywords. Uses the BM25 scoring method.

    Arguments:
        keywords: A list of comma separated keywords to search the keywords index for
        numResults: the number of results to print out
        index: the index of the table to return
        b: parameter used in BM25
        k: parameter used in BM25

    Returns:
        A boolean stating that the search was performed and a single table if index given or only one table returned, None otherwise.
    ''' 
    global num_tables
    global num_results
    if numResults is None:
        numResults = num_results

    scores = {}
    keywords_index = R.keywords_index
    table_list = R.table_list
    table_lengths = {}

    avg_doc_length = get_avg_table_word_length()

    for keyword in keywords:
        keyword = keyword.lower().strip(" ").translate(str.maketrans('', '', string.punctuation)) #removes punctuation from keyword
        words = keyword.split(" ")
        #if there are multiple words then use the keyword_intersection function to find score
        if (len(words) > 1):
            keyword_intersection(words, scores, table_lengths, avg_doc_length, b, k)
        else:
            #if the keyword does not exist in any table then move onto next keyword
            if keyword not in R.keywords_index:
                continue
            #get the total number of documents that this keyword is found in
            doc_freq = len(keywords_index[keyword].keys())
            #calculate the idf using the document frequency 
            idf = math.log((num_tables - doc_freq + 0.5)/(doc_freq + 0.5) + 1)
            for table in keywords_index[keyword].keys():
                #get the document length from the stored lengths, find it if not stored
                if table not in table_lengths:
                    data_table = get_table_by_index(table)
                    doc_length = data_table.get_agg_word_size()
                    table_lengths[table] = doc_length
                else:
                    doc_length = table_lengths[table]

                #get total number of rows the keyword appears in for the current table
                tf = len(keywords_index[keyword][table])

                #calculates bm25 score
                term_score_numerator = tf*(k+1)
                term_score_denominator = tf + k * (1 - b + b*(doc_length/avg_doc_length))
                term_score = idf*term_score_numerator/term_score_denominator
                
                if (table in scores):
                    scores[table] += term_score
                else:
                    scores[table] = term_score

    if (len(scores.keys()) > 0):
        #sort the tables by score and print the results
        scoresDesc = sorted(scores.items(), key=lambda item: item[1], reverse=True) #orders the scores descending
        totalMatches = len(scoresDesc)
        if index is not None:
            if index > totalMatches:
                print("Error: Index is greater than the number of results.")
            elif index < 0:
                print("Error: Index is negative.")
        if index is not None and index <= totalMatches and index >= 0:
            print(get_table_by_index(scoresDesc[index-1][0]))
            return True, get_table_by_index(scoresDesc[index-1][0])
        else:
            print("\nTotal Matches Found: %d" % (totalMatches))
            print('-'*20)
            for i in range(min(numResults, totalMatches)):
                print("Result #%d:" % (i+1))
                print(get_table_by_index(scoresDesc[i][0]))
                print('-'*20)
            if totalMatches == 1:
                return True, get_table_by_index(scoresDesc[0][0])
            else:
                return True, None
    else:
        print("No tables found containing keyword(s)")
        return True, None
    return True, None

def keyword_intersection(keywords, scores, table_lengths, avg_doc_length, b, k):
    '''
    Function used to search tables for search terms consisting of multiple words separated by spaces. 
    These words are considered to be one keyword so we find tables that have every one of these words in a cell.

    Arguments:
        keywords: A list of keywords to search the keywords index for
        scores: the dict of tf-idf scores to add to
        table_lengths: a dictionary storing the lengths of tables to prevent recalculating 
        avg_doc_length: the average words in a document
        b: parameter for bm25
        k: parameter for bm25

    Returns:
        False if one of the keywords is not found in any table (zero matches), True otherwise
    ''' 
    global num_tables
    keywords_index = R.keywords_index
    table_list = R.table_list
    intersections = {} # a dictionary for each table which contains a dictionary of sets of occurences of all keywords

    #finds tables that contain all keywords by finding the intersection of rows that contain each keyword
    if keywords[0] not in R.keywords_index:
        return False
    for table in keywords_index[keywords[0]].keys():
        #starts each intersection with the rows that contain the first keyword
        intersections[table] = set(keywords_index[keywords[0]][table])
        for keyword in keywords[1:]:
            if keyword not in R.keywords_index:
                return False
            if table in keywords_index[keyword].keys() and len(intersections[table]) > 0:
                # if the intersection dict does not have a set stored for this table then add all occurences of keyword
                intersections[table] = set(keywords_index[keyword][table]) & intersections[table]
            else:
                intersections[table] = {}

    #count total number of tables that the search term appeared in
    doc_freq = 0
    for table in intersections:
        if len(intersections[table]) > 0:
            doc_freq += 1
    idf = math.log((num_tables - doc_freq + 0.5)/(doc_freq + 0.5) + 1)
    #compute scores using number of occurrences in intersections
    for table in intersections:
        if len(intersections[table]) > 0:
            tf = len(intersections[table])

            if table not in table_lengths:
                data_table = get_table_by_index(table)
                doc_length = data_table.get_agg_word_size()
                table_lengths[table] = doc_length
            else:
                doc_length = table_lengths[table]

            #get total number of rows in table and number of times keyword appears in any cell
            tf = len(keywords_index[keyword][table])

            #calulate bm25 score
            term_score_numerator = tf*(k+1)
            term_score_denominator = tf + k * (1 - b + b*(doc_length/avg_doc_length))
            term_score = idf*term_score_numerator/term_score_denominator
            
            if (table in scores):
                scores[table] += term_score
            else:
                scores[table] = term_score
    return True

def table_search(tableName, searchType, columnPriority, numResults=None, index=None, b=1.2, k=0.75):
    '''
    Function used to find tables related to a query table based on the searchtype (e.g. related, joinable)

    Arguments:
        tableName: the name of the query table to search with
        searchType: the approach for finding related tables
        columnPriority: characters specifying which columns should be required/ignored
        numResults: the number of related tables to return
        index: if not None then return the table that relates to the index

    Returns:
        A singular table if index is given or if there is one result, None otherwise
    ''' 
    global user_inp_tables
    global relational_threshold
    global overlap_threshold
    global ngram_size
    global edit_dist_threshold
    global recent_table_search
    global num_results
    if numResults is None:
        numResults = num_results

    #if the table does not exist
    if tableName not in user_inp_tables.keys():
        print("Error: Query table does not exist.")
        return None
    table = user_inp_tables[tableName]

    #if the user did not mark each column
    if len(columnPriority) != table.get_num_cols():
        print("Error: Column priority is not equal to number of columns in query table")
        return None
    else:
        #group columns in dictionary by alphanumeric character
        columnGroups = {}
        optionalColumns = [] #currently unused
        for i in range(len(columnPriority)):
            currentVal = columnPriority[i]
            if currentVal.isalnum():
                if currentVal in columnGroups:
                    columnGroups[currentVal].append(i)
                else:
                    columnGroups[currentVal] = [i]
            elif currentVal == '-':
                optionalColumns.append(i)
            else:
                print("Column priority characters must be alphanumeric or '-'")
                return None
    #if the user is performing the most recent search again, return saved results
    if recent_table_search is not None and recent_table_search[0] == (tableName, table.get_num_rows(), table.get_num_cols(), searchType, columnGroups, edit_dist_threshold, ngram_size, overlap_threshold):
        related_tables = recent_table_search[1]
    else:
        if searchType == "x":
            #perform exact match table search
            related_tables = TableSearch.get_matching_tables(R.table_list, table, columnGroups, exactMatchIndex=R.exact_match_index, b=b, k=k)
        elif searchType == "n":
            #perform ngram exact match table search
            related_tables = TableSearch.get_matching_tables(R.table_list, table, columnGroups, exactMatchIndex=R.exact_match_index, ngramIndex = R.ngrams_index, ngramSize=ngram_size, b=b, k=k)
        elif searchType == "u":
            #perform unionable table search
            #generates a new table by concatenating together columns marked with the same character
            marked_columns = [] #stores the list of column indices used for the mapping
            for group in columnGroups:
                marked_columns.append(min(columnGroups[group]))
                if len(columnGroups[group]) > 1:
                    table = concatenate_columns(table, columnGroups[group])
            related_tables = table.related(relational_threshold, edit_dist_threshold, marked_columns)
        else:
            print("Error: Invalid search type. Search type must be one of; 'x', 'n', or 'e'")
            return None
        recent_table_search = ((tableName, table.get_num_rows(), table.get_num_cols(), searchType, columnGroups, edit_dist_threshold, ngram_size, overlap_threshold), related_tables)

    #print the results of the table search
    if index is not None:
        if index > len(related_tables):
            print("Error: Index is greater than the number of results.")
        elif index < 0:
            print("Error: Index is negative.")
    if index is not None and index <= len(related_tables) and index >= 0:
        try:
            print(related_tables[index-1][0])
            return related_tables[index-1][0]
        except:
            print(related_tables[index-1])
            return related_tables[index-1]
    elif index is None and len(related_tables) > 0:
        totalMatches = len(related_tables)
        print("\nTotal Matches Found: %d" % (totalMatches))
        print('-'*20)
        for i in range(min(numResults, totalMatches)):
            print("Result #%d:" % (i+1))
            try:
                print(related_tables[i][0])
            except:
                print(related_tables[i])
            print('-'*20)
        if totalMatches == 1:
            try:
                returnValue = related_tables[0][0]
            except:
                returnValue = related_tables[0]
            return returnValue
    else:
        print("No matching tables found.")

    return None

def get_table_by_index(index):
    '''
        Returns a table from the dataset by the index
    '''
    return R.table_list[index]

def get_avg_col_size():
    '''
        Calculates the average number of rows in all columns in the dataset 
    '''
    size_sum = 0
    for table in R.table_list:
        for column in range(table.get_num_cols()):
            size_sum += table.get_col_size(column)
    return size_sum / len(R.table_list)

def get_avg_document_length(ngram=True):
    '''
        Calculates the average column length on characters or ngrams

        Arguments:
            ngram: If True then calcualate length as number of ngrams, if False then use characters

        Return: The average column length
    '''
    global ngram_size
    size_sum = 0
    for table in R.table_list:
        for column in range(table.get_num_cols()):
            if ngram:
                size_sum += table.get_col_size(column)*(max(1, (table.get_avg_col_char_size(column) - ngram_size - 1)))
            else:
                size_sum += table.get_col_size(column)
    return size_sum / len(R.table_list)

def get_avg_table_word_length(ngram=True):
    '''
        Calculates the average number of words in a table in the dataset
    '''
    global ngram_size
    size_sum = 0
    for table in R.table_list:
        size_sum += table.get_agg_word_size()
    return size_sum / len(R.table_list)

def set_vars():
    '''
    Function that sets the global variables of the python file by reading the
    config.ini file.

    Returns:
        True if succesfully extracted all parameters, false otherwise.
    '''
    global relational_threshold
    global grouping_threshold
    global edit_dist_threshold
    global overlap_threshold
    global ngram_size
    global xc_operation
    global xc_multiplier
    global xc_empty_threshold
    global fill_operation
    global fill_multiplier
    global num_results
    global num_tf_examples
    global b
    global k
    global verbose

    config = configparser.ConfigParser()
    config.read('config.ini')
    # Make sure that numerical parameters are indeed numerical
    try:
        relational_threshold = float(config['parameters']['related_threshold'])
        grouping_threshold = float(config['parameters']['grouping_threshold'])
        overlap_threshold = float(config['parameters']['overlap_threshold'])
        ngram_size = int(config['parameters']['ngram_size'])
        edit_dist_threshold = float(config['parameters']['editdist_threshold'])
        xc_multiplier = float(config['parameters']['xc_smoothing_multiplier'])
        xc_empty_threshold = float(config['parameters']['xc_empty_threshold'])
        fill_multiplier = float(config['parameters']['fill_smoothing_multiplier'])
        b = float(config['parameters']['b'])
        k = float(config['parameters']['k'])
        num_results = int(config['parameters']['num_results'])
        num_tf_examples = int(config['parameters']['num_tf_examples'])
    except ValueError:
        # Report that not all numerical parameters are numerical
        print('ERROR')
        print('Related_threshold, grouping_threshold, editdist_threshold,\n'
              + 'xc_smoothing_multiplier, the xc_empty_threshold, num_results\n'
              + ', and the fill_smoothing_multiplier must all be \n'
              + 'int or floating point numbers in config.ini.')
        return False

    # Make sure the operation for xc is 'sum' or 'product'
    xc_operation = config['parameters']['xc_group_scoring']
    xc_operations = ["sum", "product"]
    if xc_operation not in xc_operations:
        print('ERROR')
        print('xc_group_scoring must be set to either "sum" or "product".')
        return False

    # Make sure the operation for fill is 'sum', 'product' or 'probabilistic'
    fill_operation = config['parameters']['fill_group_scoring']
    fill_operations = ["sum", "product", "probabilistic"]
    if fill_operation not in fill_operations:
        print('ERROR')
        print('fill_group_scoring must be set to either "sum", "product" or\n'
              + '"probabilistic".')
        return False
    
    # Make sure verbose parameter is either 'on' or 'off'
    verbose = config['parameters']['verbose'].lower()
    options = ["on", "off"]
    if verbose not in options:  
        print("ERROR")
        print('verbose parameter must be set to on or off.')
        return False

    return True

def parse_file(filename):
    ''' 
    Function used to extract all tables from a text file.

    Arguments:
        filename: The name of the file to parse the tables from.

    Returns:
        True if the file was succesfully parsed, false otherwise.
    '''
    global num_tables
    global ngram_size

    #If tables have already been loaded then extend on indexes, otherwise start them off as empty
    if (R.table_list != None):
        tables_list = R.table_list
        key_table_index = R.key_table_index
        keywords_index = R.keywords_index
        exact_match_index = R.exact_match_index
        ngrams_index = R.ngrams_index
    else:
        tables_list = []
        key_table_index = {}
        keywords_index = {}
        exact_match_index = {}
        ngrams_index = {}

    # Variable that contains the different percentages where when parsing
    # at each percentage, let the user know *percentage* number of 
    # tables have been parsed.
    thresholds = [10, 25, 50, 75, 90, 100]

    # https://stackoverflow.com/a/6475407
    # accessed on 2018-07-04
    try:
        with open(filename, "r", encoding="UTF-8") as infile:
            print("\nParsing file '%s'." %(filename))
            print("This may take a while if the file is large.")
            # Variable header is set to True one the program has already
            # tried to parse the header line and False otherwise.
            # Variable table_count is set to True if the table count was
            # succesfully retrieved from the header line and False otherwise.
            table = '' 
            header = False
            table_count = False
            i = num_tables #table index
            lines = list(infile)
            for line_index in range(len(lines)):
                line = lines[line_index]
                if not header:
                    # Determine whether first line of file was a header line
                    # containing number of tables, average row length and
                    # average column length
                    header = True
                    line = line.strip('\n').split()
                    try:
                        num_tables = int(line[0])
                        table_count = True
                        avg_row_len = float(line[1])
                        avg_col_len = float(line[2])
                    except (ValueError, IndexError):
                        if table_count == False:
                            print("\nNo table count description at the"
                                  + " top of the text file.")
                    continue
                
                # Empty line can indicate end of a table
                if line == '\n' or line_index == len(lines)-1:
                    # If table is empty, go to next line in file.
                    if table == '':
                        continue

                    # If table is not empty, extract each row and 
                    # add each key to key_table_index.
                    table = table.strip('\n')
                    rows = []
                    j = 0 #row index
                    for entry in table.split("\n"):
                        entry = extract_entries(entry.split(","))
                        key = entry[0].lower()
                        if key in key_table_index:
                            key_table_index[key].append(i)
                        else:
                            key_table_index[key] = [i]
                        rows.append(tuple(entry))
                        
                        #add or append to keywords index using the table, column, and row indexes
                            #{key1: {1: [(2,3),(4,3)], 3: [(1,2)]}}
                        k = 0 #column index
                        for column in entry:
                            #add to exact match index
                            column = column.lower()
                            if column in exact_match_index:
                                if i in exact_match_index[column]:
                                    exact_match_index[column][i].append((j,k))
                                else:
                                    exact_match_index[column][i] = [(j,k)]
                            else:
                                exact_match_index[column] = {i: [(j,k)]}

                            #add n-grams to n-gram index
                            #if len(column) >= ngram_size:
                            for ngram_length in range(min(ngram_size, len(column))):
                                ngrams = TableSearch.get_qgrams(ngram_length+1, column)
                                for ngram in ngrams:
                                    if ngram in ngrams_index:
                                        if i in ngrams_index[ngram]:
                                            ngrams_index[ngram][i].append((j,k))
                                        else:
                                            ngrams_index[ngram][i] = [(j,k)]
                                    else:
                                        ngrams_index[ngram] = {i: [(j,k)]}

                            for keyword in column.split(" "):
                                keyword = keyword.lower().translate(str.maketrans('', '', string.punctuation))
                                #add table index, and column/row index's
                                if keyword in keywords_index:
                                    if i in keywords_index[keyword]:
                                        keywords_index[keyword][i].append((j,k))
                                    else:
                                        keywords_index[keyword][i] = [(j,k)]
                                else:
                                    keywords_index[keyword] = {i: [(j,k)]}
                            k += 1
                        j += 1


                    # Create table and append it to table list
                    tables_list.append(R(rows))
                    i += 1
                    # Print statements to update user on how much
                    # of the file has been parsed.
                    if table_count:
                        score = math.floor((i / num_tables) * 100)
                        if score in thresholds:
                            print("%d%% of the tables parsed..." %score)
                            thresholds.remove(score)
                    else:
                        if (i % 100000 == 0) and i != 0:
                            print("%d tables parsed..." %i)
                    # Clear the table string
                    table = ''                   
                else:
                    table += line
    except FileNotFoundError:
        print("\nFile '%s' does not exist." %filename)
        return False

    print("\nParsing complete.")

    # Set the class variables tables_list and key_table_index of R
    R.table_list = tables_list
    R.key_table_index = key_table_index
    R.keywords_index = keywords_index
    R.exact_match_index = exact_match_index
    R.ngrams_index = ngrams_index
    
    num_tables = len(tables_list)
    return True

def add_query_table_to_db(tablename):
    ''' 
    Adds a user inputted query table to the database

    Arguments:
        table: The name of the query table

    Returns:
        True if the table exists and was successfully added
    '''
    pass
    '''global user_inp_tables
    global 
    if tablename in user_inp_tables:
        for '''

def export_tables(filename, tableList):
    '''
        Exports all the table in the dataset to a file.

        Arguments:
            filename: the name of the file to export to
            tableList: a list of the tables to export
    '''
    with open(filename, "w", encoding="UTF-8") as file:
        #write num_tables, avg num_rows, avg num_cols at top of file
        num_tables = len(tableList)
        avg_rows = 0
        avg_cols = 0
        for table in tableList:
            avg_rows += table.get_num_rows()
            avg_cols += table.get_num_cols()
        avg_rows = avg_rows/num_tables
        avg_cols = avg_cols/num_tables

        file.write("%d %f %f\n\n" % (num_tables, avg_rows, avg_cols))

        for table in tableList:
            for row in table.get_rows():
                file.write(str(row).strip("()") + "\n")
            file.write("\n")

def extract_entries(entries):
    '''
    Function used in order to extract row entries from an inputed row.

    Arguments:
        entries: A row input that was split upon ","
    
    Returns:
        A tuple of the inputed row without quotations marks
        and where each entry is separated by a ","
    '''
    # new_entry is a list of the entries where each element is an
    # entry for one column.
    new_entry = []
    i = 0
    while i < len(entries):
        entries[i] = entries[i].strip(" ")
        # If empty string, append it to new_entry
        if len(entries[i]) == 0:
            new_entry.append(entries[i])
        # If entry begins with ", determine if it also ends with ".
        # If it does not, find the rest of the row entry for this column.
        # Otherwise, append it to new_entry without the quotation marks.
        elif entries[i][0] == '"':
            if entries[i][-1] != '"':
                i = find_corresponding_pair(entries, new_entry, i, '"')
            else:
                new_entry.append(entries[i][1:-1])
        # If entry begins with ', determine if it also ends with '.
        # If it does not, find the rest of the row entry for this column.
        # Otherwise, append it to new_entry without the quotation marks.
        elif entries[i][0] == "'":
            if entries[i][-1] != "'":
                i = find_corresponding_pair(entries, new_entry, i, "'")
            else:
                new_entry.append(entries[i][1:-1])            
        else:
            new_entry.append(entries[i].strip(' "\''))
        i += 1
    return tuple(new_entry)

def find_corresponding_pair(entries, new_entry, ind, separator):
    '''
    Function used in order help with splitting a string on commas
    only if the comma is not between quotation marks or apostrophes.
    This function concatenates the split up portion of a string if it 
    had quotation marks or apostrophes and a comma.

    Arguments:
        entries: A list that was a string split on commas
        new_entry: The prior entries for the row
        ind: The beginning index of the string that has been split up
        separator: Indicates if it was either an apostrophe or a
            quotation mark used to encapsulate the entry

    Returns:
        The index from where to continue the parsing 
        of the user inputed entries
    '''
    entry = entries[ind]
    if entries[ind][-1] != separator:
        # Iterate over each other entry and find the one that ends with
        # a quotation mark. Once found, end the for loop.
        for j in range(ind + 1, len(entries)):
            entry += "," + entries[j]
            try:
                if entries[j][-1] == separator:
                    break
            except IndexError:
                continue
    # Append the concatenated entry without the quotation marks or apostrophes
    new_entry.append(entry.strip(separator))
    try:
        return j
    except NameError:
        return ind

def set_parameters(line):
    '''
    Function that sets the parameters of BareTQL.

    Arguments:
        line: Command input
    '''
    global relational_threshold
    global grouping_threshold
    global edit_dist_threshold
    global overlap_threshold
    global ngram_size
    global num_results
    global num_tf_examples
    global xc_operation
    global xc_multiplier
    global xc_empty_threshold
    global fill_operation
    global fill_multiplier
    global b
    global k
    global verbose

    xc_operations = ["sum", "product"]
    fill_operations = ["sum", "product", "probabilistic"]
    verbose_options = ["on", "off"]
    line = line.split()
    length = len(line)
    # If the line was only 'set' then display a help message on how to
    # use the set command
    if length == 1 and line[0] == "set":
        print(
            "You can set the global parameters of the program by entering:\n"
            + "set related_threshold *value*\n"
            + "set grouping_threshold *value*\n"
            + "set overlap_threshold *value*\n"
            + "set editdist_threshold *value*\n"
            + "set ngram_size *value*\n"
            + "set group_scoring *xc operations*\n"
            + "set xc_multiplier *value*\n"
            + "set xc_empty_threshold *value*\n"
            + "set fill_operation *fill operations*\n"
            + "set fill_multiplier *value*\n"
            + "set num_results *value*\n"
            + "set num_tf_examples *value*\n"
            + "set verbose *verbose options*\n\n"
            + "where 0 <= value <= 1, xc operations are sum or product,\n"
            + "fill operations are sum, product or probabilistic and\n"
            + "verbose options are on or off\n")
        print_parameters()

    # If there were 3 strings in the command separated by spaces
    elif length == 3:
        # Determine if the last string is a floating number
        try:
            line[2] = float(line[2])
            # If the last string was a floating number, determine
            # if it was for the related_threshold, grouping_threshold
            # or editdist_threshold parameters and if the value
            # was between 0 and 1.
            if line[2] < 0 or line[2] > 1:
                print("Value chosen for set command is not between 0 and 1")
            elif line[1] == "related_threshold":
                relational_threshold = line[2]
                print("Related threshold set to %.3f" % (relational_threshold))
            elif line[1] == "grouping_threshold":
                grouping_threshold = line[2]
                print("Grouping threshold set to %.3f" % (grouping_threshold))
            elif line[1] == "editdist_threshold":
                edit_dist_threshold = line[2]
                print("Edit distance threshold set to %.3f" 
                      % (edit_dist_threshold))
            elif line[1] == "overlap_threshold":
                overlap_threshold = line[2]
                print("Overlap threshold set to %.3f" 
                      % (overlap_threshold))
            elif line[1] == "b":
                b = line[2]
                print("b set to %.3f" 
                      % (b))
            elif line[1] == "k":
                k = line[2]
                print("k set to %.3f" 
                      % (k))
            elif line[1] == "ngram_size":
                ngram_size = int(line[2])
                print("n-gram size set to %.3f" 
                      % (ngram_size))
            elif line[1] == "num_results":
                num_results = int(line[2])
                print("Num results set to %.3f" 
                      % (num_results))
            elif line[1] == "num_tf_examples":
                num_tf_examples = int(line[2])
                print("Num TF examples set to %.3f" 
                      % (num_results))
            elif line[1] == "xc_multiplier":
                xc_multiplier = line[2]
                print("xc multiplier set to %.3f" % (xc_multiplier))
            elif line[1] == "xc_empty_threshold":
                xc_empty_threshold = line[2]
                print("xc empty threshold set to %.3f" % (xc_empty_threshold))
            elif line[1] == "fill_multiplier":
                fill_multiplier = line[2]
                print("fill multiplier set to %.3f" % (fill_multiplier))
            else:
                print("Improper formatting for set command")
        except ValueError:
            # If the last string was not a floating number, determine
            # if it was for the group_scoring parameter and if 
            # the last string was either sum or product.
            if line[1] == "group_scoring" and line[2] in xc_operations:
                xc_operation = line[2]
                print("Operation set to %s" % (xc_operation))
            elif line[1] == "fill_operation" and line[2] in fill_operations:
                fill_operation = line[2]
                print("Operation set to %s" % (fill_operation))
            elif line[1] == "verbose" and line[2] in verbose_options:
                verbose = line[2]
                print("Verbose parameter set to %s" % (verbose))
            else:
                print("3rd argument was not a float.")
    else:
        print("Improper set command")

def handle_dot_ops(line):
    '''
    Function used to handle the dot operations of this program 
    (.related, .related.i, .xr, .xc, .fill, .insert, 
    .delete, .delrow, .delcol, .delcell, .updatecell, 
    .sort, .swap).

    Arguments:
        line: The user inputed line in the form of 
            *tablename*.*dotOperation*

    Returns:
        A boolean or table. Return false if the input was not a dot op.
        Return True if there is nothing to print (most likely due to
        an incomplete dot operation) and a table otherwise.
    '''
    global user_inp_tables
    global relational_threshold
    global grouping_threshold
    global edit_dist_threshold
    global xc_operation
    global xc_multiplier
    global xc_empty_threshold
    global fill_operation
    global fill_multiplier
    global num_tables

    multi_allowed_ops = ["related", "xr", "xc", "fill"]
    non_multi_ops = [
        "insert", "delete", "delrow", 
        "delcol", "sort", "swap", 
        "delcell", "updatecell",
        "export"
    ]
    if "export" in line:
        line = line.split(".", 1)
    else:
        line = line.split(".")
    table_name = line[0] 
    # If table_name not initialized already then no dot operation can
    # be performed
    if table_name not in user_inp_tables:
        return False
    else:
        table = user_inp_tables[table_name]

    ops = line[1:]
    num_ops = len(ops)
    # If there are more than one operation, make sure they are all multiple
    # allowed operations. If not return false. (In the case of related.i, 
    # since we are separating upon .'s, make sure the operation before was
    # 'related')
    if num_ops > 1:
        for i in range(num_ops):
            if ops[i].lower() not in multi_allowed_ops:
                if not (is_integer(ops[i]) and i > 0 and ops[i - 1] == "related"):
                    return False
    # If there is only one operation make sure it is valid operation
    elif num_ops == 1:
        op = ops[0].split("(")[0].lower()
        if (op not in non_multi_ops) and (op not in multi_allowed_ops):
            return False
    # num_ops == 0
    # Print table command or setting a new table to an already existing table.
    else:
        print(table)
        return R(table.get_rows())

    for op in ops:
        relatedi = False
        related = False
        lowercase_op = op.lower()
        if lowercase_op == "related":
            # Operation is related, call .related on the table
            related = True
            tables = table.related(relational_threshold, edit_dist_threshold)

        elif is_integer(op):
            # Operation is an integer, call related.i on the table
            index = int(op)
            table, value = table.get_related_by_index(index)
            relatedi = True
            
        elif lowercase_op == "xr":
            # Case .xr command
            table = table.xr(relational_threshold, 
                             edit_dist_threshold, num_tables)

        elif lowercase_op == "xc":
            # Case .xc command
            table = table.xc(relational_threshold, grouping_threshold, 
                             xc_empty_threshold, xc_operation, 
                             xc_multiplier, edit_dist_threshold)

        elif lowercase_op == "fill":
            # Case .fill command
            table = table.fill(relational_threshold, fill_operation, 
                               fill_multiplier, edit_dist_threshold)

        elif lowercase_op[:6] == "insert":
            # .insert command
            match_obj = re.match("insert\((.*?)\)$", lowercase_op)
            if match_obj:
                # Need this new re.search to not lowercase user input
                match_obj = re.search("\((.*?)\)$", op)
                row = match_obj.group(1)
                row = extract_entries(row.split(","))
                if not table.insert(row):
                    return True
            else:
                print("Improper .insert command.")
                return True
        
        elif lowercase_op[:6] == "delete":
            # .delete command
            match_obj = re.match("delete\((.*?)\)$", lowercase_op)
            if match_obj:
                # Need this new re.search to not lowercase user input
                match_obj = re.search("\((.*?)\)$", op)
                row = match_obj.group(1)
                row = extract_entries(row.split(","))
                if not user_inp_tables[table_name].delete(row):
                    return True
            else:
                print("Improper .delete command.")
                return True

        elif lowercase_op[:6] == "delrow":
            # delrow command
            # Determine whether it is a single delete (match_obj1) or
            # if it is a range delete (match_obj2)
            match_obj1 = re.match("delrow\((\d+)\)$", lowercase_op)
            match_obj2 = re.match("delrow\((\d+)-(\d+)\)$", lowercase_op)
            if match_obj1:
                ind = int(match_obj1.group(1))
                if not user_inp_tables[table_name].del_row(ind):
                    return True
            elif match_obj2:
                beg_ind = int(match_obj2.group(1))
                end_ind = int(match_obj2.group(2))
                if beg_ind > end_ind:
                    print("First index must be smaller than second.")
                    return True
                elif beg_ind >= 1:
                    for ind in range(end_ind, beg_ind - 1, -1):
                        if not user_inp_tables[table_name].del_row(ind):
                            return True
                else:
                    print("First index is too small.")
                    return True
            else:
                print("Improper .delRow command.")
                return True

        elif lowercase_op[:6] == "delcol":
            # delcol command
            # Determine whether it is a single delete (match_obj1) or
            # if it is a range delete (match_obj2)
            match_obj1 = re.match("delcol\((\d)\)$", lowercase_op)
            match_obj2 = re.match("delcol\((\d+)-(\d+)\)$", lowercase_op)
            if match_obj1:
                ind = int(match_obj1.group(1))
                if not user_inp_tables[table_name].del_col(ind): 
                    return True
            elif match_obj2:
                beg_ind = int(match_obj2.group(1))
                end_ind = int(match_obj2.group(2))
                if beg_ind > end_ind:
                    print("First index must be smaller than second.")
                    return True
                elif beg_ind >= 1:
                    for ind in range(end_ind, beg_ind - 1, -1):
                        if not user_inp_tables[table_name].del_col(ind):
                            return True
                else:
                    print("First index is too small.")
                    return True
            else:
                print("Improper .delCol command.") 
                return True 

        elif lowercase_op[:7] == "delcell":
            # .delcell command
            match_obj = re.match("delcell\((\d+),[ ]*(\d+)\)$", lowercase_op) 
            if match_obj:
                row_index = int(match_obj.group(1))
                col_index = int(match_obj.group(2))
                if not table.delcell(row_index, col_index):
                    return True
            else:
                print("Improper .delcell command")
                return True

        elif lowercase_op[:10] == "updatecell":
            # .updatecell command
            match_obj = re.match("updatecell\((\d+),[ ]*(\d+),[ ]?([^ \)]+)\)$", 
                                 lowercase_op) 
            if match_obj:
                # Need this new match in order to not lowercase entry
                match_obj = re.search("\((\d+),[ ]*(\d+),[ ]?([^ \)]+)\)$", op)
                row_index = int(match_obj.group(1))
                col_index = int(match_obj.group(2))
                entry = match_obj.group(3)
                if entry == "''" or entry == '""':
                    entry = ''
                if not table.update_cell(row_index, col_index, entry):
                    return True
            else:
                print("Improper .updateCell command")
                return True
        
        elif lowercase_op[:4] == "sort":
            # .sort command
            match_obj = re.match("sort\((.*?)\)$", lowercase_op)
            if match_obj:
                args = match_obj.group(1)
                args = args.split(",")
                if len(args) == 1:
                    # len(args) == 1 so the only parameter is the column index
                    # to sort upon
                    if not is_integer(args[0]):
                        # If column index was not an integer than print out
                        # help message
                        print(
                            "sort the table on any column by entering '*tablename*.sort(i)'\n"
                            + "or '*tablename*.sort(i, bool)' where i is an integer between 1\n"
                            + "and the number of columns in *tablename* and bool is an optional\n"
                            + "boolean indicating if the table should be sorted in descending\n"
                            + "order. Table is sorted in ascending order by default")
                        return True           
                    # If only parameter is integer then call .sort on table    
                    if not table.sort(int(args[0]), False):
                        return True

                elif len(args) == 2:
                    # len(args) == 2 so the user provided both a column index 
                    # to sort upon and if they want it in ascending order
                    if not is_integer(args[0]):
                        # If column index was not an integer than print out
                        # help message
                        print(
                            "sort the table on any column by entering '*tablename*.sort(i)'\n"
                            +  "or '*tablename*.sort(i, bool)' where i is an integer between 1\n"
                            +  "and the number of columns in *tablename* and bool is an optional\n"
                            +  "boolean indicating if the table should be sorted in descending\n"
                            +  "order. Table is sorted in ascending order by default")
                        return True  
                    # Determine if second parameter was true or false
                    desc_param = args[1].strip()
                    if desc_param == "true":
                        if not table.sort(int(args[0]), True):
                            return True
                    elif desc_param == "false":
                        if not table.sort(int(args[0]), False):
                            return True   
                    else:
                        print("desc parameter must be true or false")
                        return True
                else:
                    print("Too many arguments")
                    return True
            else:
                print("Improper .sort command.") 
                return True

        elif lowercase_op[:4] == "swap":
            # .swap command
            match_obj = re.match("swap\((\d+),[ ]*(\d+)\)$", lowercase_op)
            if match_obj:
                indx1 = int(match_obj.group(1))
                indx2 = int(match_obj.group(2))
                if not table.swap(indx1, indx2):
                    return True
            else:
                print("Improper .swap command")
                return True

        elif lowercase_op[:6] == "export":
            if "\"" in op:
                result = re.search("export\(\"(.*)\"\)", op)
            elif "\'" in op:
                result = re.search("export\('(.*)'\)", op)
            else:
                print("Improper export format.")
                return True
            try:
                filename = result.group(1).strip(" ")
                export_tables(filename, [table])
                print("Exported to " + filename)
                return True
            except:
                print("Improper export format.")
                return True
                    
        else:
            return False 

    if related:
        # Related command so print the rows of each table
        for table in tables:
            print(table.get_rows())
        print("# of related tables: %d" %len(tables))
    elif relatedi:
        # Related i command so print out number of overlap related
        # table i had with the query table
        if table != None:
            print(table)
            print("\nOverlap = %.3f" %(value))
        else:
            return True
    else:
        # For any other command, print the table
        print(table) 
    return table

def import_table(file_name, header=True):
    if ".csv" in file_name.lower():
        try:
            with open(file_name, "r", encoding="UTF-8") as f:
                reader = csv.reader(f)
                table = []
                for row in reader:
                    if header:
                        header = False
                    else:
                        entries = extract_entries(row)
                        table.append(entries)
                return table
        except Exception as e:
            return None
    else:
        try:
            with open(file_name, "r", encoding="UTF-8") as f:
                table = []
                for row in f:
                    row = row.strip("\n").strip('"')
                    if header:
                        header = False
                    elif row != "\n" and row != "":
                        entries = extract_entries(row.split(","))
                        table.append(entries)
                return table
        except Exception as e:
            return None

def extract_table(line_segments):
    '''
    Function used in order to extract the user inputed relational table.

    Arguments:
        line_segments: A list of the line segments where the first element 
            references the table name and the second element references the 
            table elements

    Returns:
        The table name and relational table extracted from user input 
        or None, None if input was invalid.
    '''
    table_name = line_segments[0].strip(" ")
    table = []
    line_segments[1] = line_segments[1].strip(" ")
    # INPUT Method 1. Multiple rows separated by ),(
    if (line_segments[1].find('),(') != -1 and 
        line_segments[1].find("((") != -1 and 
        line_segments[1].find("))") != -1):
        # [2:-2] is to remove '((' and '))'
        for entries in line_segments[1][2:-2].split('),('):
            entries = extract_entries(entries.split(","))
            table.append(entries)
    # INPUT Method 2. Multiple rows separated by ), (
    elif (line_segments[1].find('), (') != -1 and
          line_segments[1].find("((") != -1 and 
          line_segments[1].find("))") != -1): 
        # [2:-2] is to remove '((' and '))'
        for entries in line_segments[1][2:-2].split('), ('):
            entries = extract_entries(entries.split(","))
            table.append(entries)
    # INPUT Method 3. Singular row with '((' and '))' as beginning and ending tags
    elif (line_segments[1].find("((") != -1 and 
          line_segments[1].find("))") != -1):
        # [2:-2] is to remove '((' and '))'
        entries = extract_entries(line_segments[1][2:-2].split(","))
        table.append(entries)
    # Input Method 4. Singular row with '(' and ')' as beginning and ending tags
    elif (line_segments[1].find("(") != -1 and 
          line_segments[1].find(")") != -1 and 
          line_segments[1].find("((") == -1):
        # [1:-1] is to remove '((' and '))'
        entries = extract_entries(line_segments[1][1:-1].split(","))
        table.append(entries)
    # Improper input
    else:
        print(
            "Improper entry.\nEnter a table in the form R = "
            + "((i11, i12),(i21, i22)) or R = ((i11, i12), (i21, i22)).")
        return None, None
    return table_name, table

def print_parameters():
    '''
    Function that prints out the parameters
    '''
    global relational_threshold
    global grouping_threshold
    global edit_dist_threshold
    global xc_operation
    global xc_multiplier
    global xc_empty_threshold
    global fill_operation
    global fill_multiplier
    global verbose

    print("Currently:\n"
          + "related_threshold is %.2f\n" % (relational_threshold)
          + "grouping_threshold is %.2f\n" % (grouping_threshold)
          + "editdist_threshold is %.2f\n" % (edit_dist_threshold)
          + "group_scoring is %s\n" % (xc_operation)
          + "xc_multiplier is %.2f\n" % (xc_multiplier)
          + "xc_empty_threshold is %.2f\n" % (xc_empty_threshold)
          + "fill_operation is %s\n" % (fill_operation)
          + "fill_multiplier is %.2f\n" % (fill_multiplier)
          + "verbose is %s\n" % (verbose))

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
        + "Enter 'set' for information on setting the parameters of the\n"
        + "program. Enter 'params' to find out what the value of each\n"
        + "parameter is currently.\n"
        + "Type 'quit' to exit the console.")

def is_integer(inp):
    '''
    Function used to determine if the input was an integer.
    
    Arguments:
        inp: Input
        
    Returns:
        A boolean indicating if the input was an integer or not
    '''
    try:
        int(inp)
        return True
    except ValueError:
        return False
