# BareTQL 
---  
BareTQL (sounds as "bear tickle") stands for "bare table query language". The language 
provides an algebra for querying tables that have no schema, meaning columns and their 
types are not known. Many tables inside web pages, referred to as Web tables, are of this 
type and can be queried using BareTQL. It is generally assumed that the first column of a 
table is its key column.

# Use of Table String Transformer
The bareTQL program uses the Table-String-Transformer library developed by Arash Dargahi, it can be found [here](https://github.com/arashdn/table-string-transformer).

# Starting BareTQL
To launch BareTQL, in the 'program' folder type the following line:

    python console.py *file name*

Where *file name* is the location of the database file, if this line is left out then the user is promted to enter it after the program starts.

# Query Table Operations + Database File Format

For information about operations on user inputted tables and how database files are formatted refer to [here](https://github.com/HarrisonFah/baretql_console/blob/master/queryTableOperations.md).

# Import/Export

## Databases
To **import** a database use the command (with apostrophes or quoatation marks): 

    import("*file name*")
    
This will append the database to the current one.

To **export** a database use the command (with apostrophes or quoatation marks): 

    export("*file name*")
    
The output will also include the number of tables and the average number of rows and columns.

## Query Tables

To **import** a query table use the command

    *table name* = open("*file name*")

or

    *table name* = open("*file name*", "*y/n*")

Where the second parameter specifies if the file contains a header on the first line or not.

Example:

    R = open("fruits.csv", "y")

To **export** a query table use the command (with apostrophes or quoatation marks):

    *table name*.export("*file name*")

The output will contain a header listing the number of tables (1) and the number of rows and columns. 

Example:

    R.export("fruits_export.txt")

# Keyword Search

To perform a keyword search use one of the following commands (with apostrophes or quoatation marks):

    keyword_search("*keywords*")
    keyword_search("*keywords*", *number of results*)

Keywords are separated by commas and number of results should be an integer greater than zero. Keywords are case-insensitive and punctuation is removed. The user can retrieve a specific table by appending a .i to the end of the command where i is the index of the table beginning at 1. A singular table can also be assigned to a query table.

Example: If the user wants to create a query table R as the top result of searching for tables with the keyword "Obama" and "Trump" they can enter the following:

    R = keyword_search("Obama, Trump").1

Example: If the user wants to search for the top 5 tables that contain "The Beatles" and "John Lennon" they can enter:

    keyword_search("The Beatles, John Lennon", 5)

# Table Search

To perform a table search a user can use one of the following commands (with apostrophes or quoatation marks):

    table_search(*table name*, "*search method*", "*column groups*")
    table_search(*table name*, "*search method*", "*column groups*", *num results*)

Search method is either "x", "n", or "u". "x" refers to exact matching where an entire column entry is used as a search term while "n" uses n-gram as search terms (default length is n=5, this can be changed in config.ini), "u" finds unionable tables by mapping the marked columns to columns in candidate tables. Column groups are formatted by using alphanumeric characters to "mark" a column or group of columns as being used in the search, columns marked by the same alphanumeric character will be concatenated together (since column entries are concatenated using an obscure unicode character, it is extremely unlikely that exact match will return any results). If a column is not used in the search then it should be marked with "-". The number of results, table indexing, and query table assignment follows the same format as keyword search.

Example: If a user wants to find a table S as the second best table to join with R (5 columns) on both the first and third column using n-gram matching they will enter the following command:

    S = table_search(R, "n", "a-b--").2

Example: If a user wants to see the top 5 results for joinable tables on the fourth and fifth columns of R concatenated together using exact matching they will enter the following command:

    table_search(R, "x", "---11", 5)

# Finding Transformations

To find a set of transformations for joining two query tables a user can use the following command:

    joinable(*src table name*, "*src column groups*", *tgt table name*, "*tgt column groups*")

The column groups are formatted the same as in table search. A set of transformations is found for each matching column group between the src and tgt tables. The user can export the transformations to a file by appending .export("*file name*") to the end of the command. The file contains the direction of the transformations for each column pair and separates the sets by a row of hyphens. The user can then edit the transformations to remove/add/fix transformations.

Example: If a user wants to find transformations for joining table R's column 1 and 3 with table S' column 2 and 5 they can use the command:

    joinable(R, "x-y-", S, "-x--y")

Example: If a user wants to export transformations for the joinn between table A's column 1 and 2 concatenated together with column B's column 3 they can type:

    joinable(A, "11-", B, "--1").export("a_b_join.txt")

# Performing Joins

To perform a join between two tables the user can use the following command:

    join(*src table name*, "*src column groups*", *tgt table name*, "*tgt column groups*")
    join(*src table name*, "*src column groups*", *tgt table name*, "*tgt column groups*", "*transformation file name*")

If the user does not give a transformation file in the command then transformation are automatically generated and stored in the folder "temp_transformations". The resulting join contains both the src and tgt rows concatenated together as well as the transformation(s) that led to the join. The user can also assign the results to a query table.

Example: If a user wants to generate table T as the joined tables R (5 columns) and S (4 columns) on columns 1 and 3 of R with 2 and 4 of S respectively using the transformations in "table_tfs.txt" they can use the following command:

    T = join(R, "a-b--", S, "-a-b", "table_tfs.txt")

Example: If a user wants to join together table A's columns 1 and 3 concatenated together and column 2 with B's columns 2 and 5 without using a transformation file they can type:

    join(A, "121-", B, "-1--2")

# Developing Team

BareTQL is developed at the University of Alberta. The developing team includes Harrison Fah (fah@ualberta.ca), Thomas Lafrance (tlafranc@ualberta.ca) and Davood Rafiei (drafiei@ualberta.ca).

# Citing This Work
This work can be cited using the following:

    @inproceedings{rafiei2022baretql,
      title={BareTQL: An Interactive System for Searching and Extraction of Open Data Tables},
      author={Rafiei, Davood and Fah, Harrison and Lafrance, Thomas and Dargahi Nobari, Arash},
      booktitle={27th International Conference on Intelligent User Interfaces},
      pages={30--33},
      year={2022}
    }



