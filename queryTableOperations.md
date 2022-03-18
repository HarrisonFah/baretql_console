# User Table Operations
  
A video demo can be found at https://youtu.be/MGy4oG2gzcQ.
  
## Database File Format 
  
The .txt file contains a set of web tables. The format of the .txt file is as follows:  
- Tables are separated by a new line  
- Each row is written on a different line  
- Each entry in a row is separated by a comma and is encompassed by either quotation marks or an apostrophe  
- *OPTIONAL* The first line of the file may contain statistics on the file in the format
\*Number of Tables\* \*Average Number of Rows\* \*Average Number of Columns\*
e.g
```
3 5.333 4

"Joe", "Male", "19", "UofA"
"John", "Male", "21", "UofA"
"Mary", "Female", "22", "UofA"
"Jeff", "Male", "20", "UofA"
"Sarah", "Female", "24", "UofA"

"John", "Male", "21", "UofA"
"Mary", "Female", "22", "UofA"
"Jim", "Male", "22", "UofA"
"Jeff", "Male", "20", "UofA"

"John", "Male", "21", "UofA"
"Jeff", "Male", "20", "UofA"
"Sarah", "Female", "24", "UofA"
"Alexander", "Male", "21", "UofA"
"Rachel", "Female", "26", "UofC"
"Vera", "Female", "40", "Concordia"
"Joe", "Male", "19", "UofC"
```
In this example, there are 3 tables where each table has 4 columns and table 1 has 
5 rows, table 2 has 4 rows and table 3 has 7 rows.  
  
Large datasets of 450,000 - 500,000 tables can be found at 
https://drive.google.com/drive/folders/1o6wzJOr8yESHBspkJqkIgYsxpMVb1h-7?usp=sharing
  
#### Scripts  
Under the folder "parsingScripts", there are a couple of scripts that I have used 
for different formats of data including .xml and .json. Ready made files of web
tables can be found under program/data/ or at the google drive folder linked above.
    
## Initializing a Table  
  
Once the program has started, a user can initialize a query table by writing:  
'*table name* = ((*r1c1*, *r1c2*, ..., *r1cn*), (*r2c1*, *r2c2*, ..., *r2cn*), ..., (*rnc1*, *rnc2*, ..., *rncn*))'  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
```  
  
The field values can be enclosed within quotation marks or apostrophes especially 
when the content includes commas. 
    
e.g.    
```
R = (("Doe, John", "21"), ('Jane, Mary', '22'), (Jeff, 20))
```  
  
## Table Operations  
  
The following operations can be performed on a query table:  
  
#### .related  
Find "related" data tables to *tablename* (see full definition in 'Relatedness' section that is under the section
'Definitions and Program Details') by entering '*tablename*.related'. To summarize relatedness, a data table
is related to a query table if *x* > *y* where *x* is the number of rows from the query table that are also in the 
data table and *y* is the number of rows in the query table multiplied by the *related_threshold* (definition 
given in 'Thresholds and Parameter' section).
This will display a list of all tables that are related to the query table as well as the number of tables
that are related to the query table.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.related
```  
  
#### .related.i  
Find a particular related table by doing '*tablename*.related.i' where i the index 
for the table requested (range is between 1 and the number of related tables).  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.related.2
```  
  
#### .xr  
Extend the rows of a table by entering '*tablename*.xr'. The result of this operation
is not saved upon *tablename* but is rather returned. If the user would like to save
this result, they can use table assignment shown below.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))  
S = R.xr 
```  
In this example, R remains unchanged and the result of the operation is stored 
into the table S.  
  
#### .xc  
Extend the columns of a table by entering '*tablename*.xc'.The result of this operation
is not saved upon *tablename* but is rather returned. If the user would like to save
this result, they can use table assignment shown below.    
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))  
R.xc
S = R.xc
```  
In this example, R remains unchanged and the result of the operation is stored 
into the table S.  
  
#### .fill
Fill any empty values in *tablename* by entering '*tablename*.fill'. The result of this operation
is not saved upon *tablename* but is rather returned. If the user would like to save
this result, they can use table assignment shown below.  

e.g.
```
R = ((John, '', UofA), (Mary, 22, UofT), (Jeff, 20, ''))  
S = R.fill
```
In this example, R remains unchanged and the result of the operation is stored 
into the table S.  
  
#### .sort  
Sort the table on any column by entering '*tablename*.sort(i)' 
or '*tablename*.sort(i, bool)' where i is an integer between 1 
and the number of columns in *tablename* and bool is an optional 
boolean indicating if the table should be sorted in descending 
order. Table is sorted in ascending order by default.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))  
R.sort(1)  
  
R.sort(2, True)  
```  
  
#### .swap  
Swap two columns in a table by entering '*tablename*'.swap(i, j) 
where i and j are integers between 1 and the number of columns 
in *tablename*.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))  
R.swap(2,3)
```  
  
Note: You are only able to swap the key column of a table with a column
that does not contain ANY entries that are empty.
  
#### .insert
Insert a row into the table by entering 
'*tablename*.insert(*r1c1*, *r1c2*, ..., *r1cn*)'.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.insert(Sarah, 24, UofA)
```  
  
#### .delete
Delete a row from the table by entering 
'*tablename*.delete(*r1c1*, *r1c2*, ..., *r1cn*)' provided 
this row is already in *tablename*.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.delete(John, 21, UofA)
```  
  
#### .delRow
Delete a row from the table by entering 
'*tablename*.delrow(index)' where index is an integer between
1 and the number of rows in *tablename*.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.delrow(3)
```  
  
Delete multiple rows from the table by entering '*tablename*.delrow(*index1*-*index2*)' 
where index1 and index2 are integers between 1 and the number of rows in *tablename*.  
  
e.g.
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.delrow(1-2)
```
  
#### .delCol  
Delete a column from the table by entering 
'*tablename*.delcol(index)' where index is an integer between 
1 and the number of columns in *tablename*.  
  
e.g.  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.delcol(1) 
```  
  
Delete multiple columns from the table by entering '*tablename*.delcol(*index1*-*index2*)' 
where index1 and index2 are integers between 1 and the number of columns in *tablename*.  
  
e.g.
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.delcol(2-3)
```

Note: You are only able to delete the key column of a table if the table only has 1 column or if
there is another column in the table that does not contain ANY entries that are empty.

#### .updateCell
Update a specific cell from a table by entering '*tablename*.delcell(rowIndex, colIndex, value)'
where rowIndex is an integer between 1 and the number of rows in *tablename*, 
colIndex is an integer between 1 and the number of columns in *tablename* and value is the desired
new entry at (rowIndex, colIndex). When updating a cell in the key column (1st column), the value
cannot be empty.  
  
e.g.
```
R = ((John, 21), (Mary, 20))
R.updateCell(2, 2, 22)
```
  
#### .delCell
Delete a specific cell from a table by entering '*tablename*.delcell(rowIndex, colIndex)'
where rowIndex is an integer between 1 and the number of rows in *tablename* and 
colIndex is an integer between 1 and the number of columns in *tablename*. A user is not
allowed to delete a cell in the key column (1st column).  
  
e.g.
```
R = ((John, 21), (Mary, 22))
R.delcell(1,2)
```

  
### Table Operation Extras
The result of an operation can be assigned to a table using "=". For example,  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
S = R.xr
```  
  
Multiple related, related.i, xr, xc and fill operations can be called at once. For example,  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))
R.xr.xc.related 
```  
  
All operations are case insensitive. Thus,
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))  
R.xr 
```
and  
```
R = ((John, 21, UofA), (Mary, 22, UofT), (Jeff, 20, McGill))  
R.XR
```
will both work.  
  
## Other Operations  

#### help
The help operation will display a help message regarding how to perform operations on
a table.  
  
e.g.
```
help
```

#### Table Display
It is possible to display the contents of a table that has been assigned to a variable 
by simply entering the variable.  
  
e.g.
```
R = ((John, 21), (Mary, 22))
S = R.xr.xc
S
```

#### params
The params operation will display the current values for all thresholds and parameters 
used in the program. These parameters can be found below under the section 'Thresholds
and Parameters'.  
  
e.g.
```
params
```

#### set
The set command is used to set the value of different thresholds and parameters in the 
program. The way to do this is as follows:  
```
set *threshold/parameter* *argument*
```
Concrete examples of the set command will be given in the section "Thresholds and Parameter".  
Typing only 'set' will display a help message on how to use this function.
  
e.g.
```
set
```

## Thresholds and Parameters  
  
In order to determine the result of certain operations and for other features of the program,
there are 9 thresholds/parameters. Default values for the parameters are set in 'config.ini' file
and these values can be changed manually whilst running the program by using the set command 
that was mentioned earlier or by updating the config.ini file.  
  
### Setting the Thresholds  
A user is able to change the thresholds and scoring parameter with the following 
syntax:  
```
set related_threshold *value*
set grouping_threshold *value*
set group_scoring *xc_operations*
set editdist_threshold *value*
set xc_multiplier *value*
set xc_empty_threshold *value*
set fill_operation *fill operations*
set fill_multiplier *value*
set verbose *verbose options*
```  
where 0 <= value <= 1, xc operations are sum or product, fill operations are sum, 
product or probabilistic and verbose options are on or off.  
  
#### related_threshold  
This threshold is used to determine if two tables are considered related. 
The full explanation of how to determine if two tables are related can be found 
in the 'Relatedness' section under 'Definitions and Program Details' but to summarize, 
let x be the number of rows from the query table that are also in the data table and 
let y be the number of rows in the query table multiplied by the *related_threshold*. 
The data table is considered related to the query table if x > y.
The *related_threshold* is a floating number between 0 and 1.  
  
#### grouping_threshold  
This threshold is used to help determine if two columns are to be considered 
in the same group. The full explanation of how column grouping is done can be found under 
the 'Extending a Table' section under 'Definitions and Program Details' secion.
The role of the *grouping_threshold* in extending a table horizontally is to determine the 
minimum amount of overlap between two columns for them to be considered similar. If the number 
of matches between row entries of the two columns divided by the length of the columns 
(#matches between columns / column length) is less than the *grouping_threshold*, 
these two columns will not be grouped together. If it is greater than or equal to the 
*grouping_threshold* than the columns will be grouped together.  

#### editdist_threshold  
When determining if two strings are the same, this program is using the Levenshtein
distance between the two strings divided by the sum of their lengths (dist / (l1 + l2)).
This threshold determines how small the result of dist / (l1 + l2) must be between two 
strings for them to be considered the same.

#### group_scoring
This parameter determines how the representative row entries should be calculated 
for a column group. A column group can contribute two or more different values for the same
row entry, in which case one must be chosen. The two methods that this parameter can be set
to are sum and product. These methods are explained under the section 'Definitions and Program
Details'. 
  
#### xc_multiplier
This parameter is the smoothing multiplier used by the .xc operation when using the product
rule as the operation. 

#### xc_empty_threshold
This parameter is the minimum decimal amount of rows that must have a value other than the
empty string in order for the column to be concatenated to the table when using the .xc operation.

#### fill_operation
This parameter determines how the value for an empty cell is determined when using the .fill
operation. The possible operations are sum, product and probabilistic. These methods are 
explained under the section 'Definitions and Program Details'. 

#### fill_multiplier
This parameter is the smoothing multiplier used by the .fill operation when using the product
rule as the operation. 

#### verbose
The verbose parameter is set to 'On' when the user would like to know the time it takes to 
perform a query and is set to 'Off' otherwise.
  
## Definitions and Program Details  

#### Relatedness 
There is a match between two rows if the key columns are the same (the key column is 
assumed to be the first row entry e.g. the key entry for (John, 21, UofA) is John) 
and there is mapping between the value columns (all columns except for the key 
column) in the query row to the columns in the row from the data table. For 
example, if we are comparing the query row (John, 21, UofA) to the data row
(John, Male, UofA, 21), there is a match for this column mapping as all value columns from the query row 
can be mapped to the data row. In the case where the query row is (John, 21, UofA) 
and the data row is (John, Male, UofA) there is no match as 21 is not mapped to any column in the data row.
The column mapping with the greatest \# of matches is used to determine the related score of this table
which is defined as: \# of matches of highest scoring column mapping / len(query table).  
A data table is considered related to a query table if the related score is above the *related_threshold*. 
  
#### Extending a Table
The related tables are utilized to extend the table horizontally and vertically. 
For the .xr operation, each row from tables that are related to the query table 
but not already in the query table are appended to the resulting table. The table is ordered
in such a way where first the rows that were in the query table initially are sorted based
on most frequent occuring in the related tables. Afterwards, all rows that were in the related
tables but not already in the query table are sorted based on how frequent they appeared in 
the related tables. If the query table contains more than one column, we want the new rows
to also contain a value for this column. Thus, using the column mapping extracted in the 
'Relatedness' section, we also extract all values that are in this column mapping.
  
For the .xc operation, each value column from all the related data tables that are not 
being mapped to one of the value columns from the query table are considered for potential
horizontal expansion. To avoid duplicate columns (for example if two related columns
both contain information on Countries, they will most likely be very similar), if two columns
have an overlap greater than the *grouping_threshold*, these columns are placed into a group.
Since there is a possibilty of columns in the same group having a high overlap yet some
discrepancies between them, a representative column is calculated. This is done by using either 
the sum rule or the product rule which are described in the 'Sum Rule and Product Rule' section.
  
#### Sum Rule and Product Rule
Both of these rules are a method to determine the representative column for a 
group of columns. For each column in a group, it belongs to a data table, and this table
is associated to a related score which is calculated as describe in the relatedness section. 
For each row entry of the representative column, it can take any of the entries found
in that row from the columns in the group it is being calculated for. Each possible row 
entry for the representative row entry will be associated to a score. This score can be 
calculated using the sum rule or the product rule.  
  
##### Sum Rule
The score for each possible representative row entry is the sum of the related scores from 
each column that contain this entry in this row. The row entry with the highest score is 
the representative row entry for that row.  
  
##### Product Rule  
The score for each possible representative row entry is the product of the related scores
from each column that contain this entry in this row and a smoothing multiplier when this
row entry is absent from a column. We use a smoothing multiplier because the related scores are decimal 
numbers less than 1 thus, if we were to take the product of only the related scores of the tables it
appeared in, an entry that appears in multiple tables will most likely have a score less than an entry that 
appears in only one table. As a result, we multiply each entries score by the smoothing multiplier when it is missing from
a column in order to scale down the scores proportionally.

##### Example Sum and Product Rule
Example Setting:
```
Table: ((John, Male, UofA), (Mary, Female, UofT), (Jeff, Male, McGill), (Sarah, Female, McGill))
group = [(23, 22, 20, 21), (22, 22, '', 21), (22, 22, 20, ''), (23, '', 20, '')]
scores = [1.0 .75, .75, .5]
```
The table that we are trying to expand has 4 rows and 3 columns. One possible column expansion group is
the group displayed in line #2. The group is a collection of tuples where for each tuple, index i represents the
value for the ith row. There are 4 different tuples in this group, each from different tables
thus each have their own related scores. These related scores are shown in line 3 where 
the ith score is the score for the ith tuple.  
  
As we can see, these columns agree for row entries 2, 3 and 4 however, there are two different
potential entries for row 1.  
Using the sum rule, 
```
scores = [{23: 1.5, 22:1.5}, {22: 2.5}, {20: 2.25}, {21: 1.75}]
```
Thus, the representative column will be (23, 22, 20, 21) or (22, 22, 20, 21) due to an
arbitrary tiebreaker.  
  
Using the product rule,
```
scores = [{23: .005, 22:.005625}, {22: .05625}, {20: .0375}, {21: .0075}]
```
Thus, the representative column will be (22, 22, 20, 21) due to 22's superior score compared
to 23 for the first column when using the product rule.  

#### Probabilistic Rule
The probabilistic rule is a method of determining the values for all empty cells in a query
table. This is done by finding the set of cell-value pairs for each empty cell that maximizes a
likelihood score L.
  
## Example Run
  
Example run:  
C:\Users\Thomas Lafrance\...>main.py database.txt  
  
Default relational threshold, column threshold, column grouping  
operation and edit distance threshold are 0.50, 0.40, sum and 0.10 respectively.  
Type "help" if you need help.  
  
\>R = ((John, 21), (Mary, 22), (Jeff, 20))  
('John', '21')  
('Mary', '22')  
('Jeff', '20')  
Number of rows: 3  
Number of cols: 2  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>R.related  
Find related tables.  # of related tables: 3  
(('Joe', 'Male', '19', 'UofA'), ('John', '', '21', 'UofA'), ('Mary', 'Female', '22', 'UofA'), ('Jeff', 'Male', '20', 'UofA'), ('Sarah', 'Female', '24', 'UofA'))  
(('John', 'Male', '21', 'UofA'), ('Mary', '', '22', 'UofA'), ('Jim', 'Male', '22', 'UofA'), ('Jeff', '', '20', 'UofA'))  
(('John', 'Male', '21', 'UofA'), ('Jeff', 'Male', '20', 'UofA'), ('Sarah', 'Female', '24', 'UofA'), ('Alexander', 'Male', '21', 'UofA'), ('Rachel', 'Female', '26', 'UofC'), ('Vera', 'Female', '40', 'Concordia'), ('Joe', 'Male', '19', 'UofC'))  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>R.related.2  
Related table 2  
('John', 'Male', '21', 'UofA')  
('Mary', '', '22', 'UofA')  
('Jim', 'Male', '22', 'UofA')  
('Jeff', '', '20', 'UofA')  
Number of rows: 4  
Number of cols: 4  
  
Overlap = 1.000  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>set related_threshold 0.9  
Related threshold set to 0.900  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>R.xr  
('John', '21')  
('Mary', '22')  
('Jeff', '20')  
('Joe', '19')  
('Sarah', '24')  
('Jim', '22')  
Number of rows: 6  
Number of cols: 2  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>set related_threshold 0.5  
Related threshold set to 0.500  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>R.xr  
('John', '21')  
('Jeff', '20')  
('Mary', '22')  
('Joe', '19')  
('Sarah', '24')  
('Jim', '22')  
('Alexander', '21')  
('Rachel', '26')  
('Vera', '40')  
Number of rows: 9  
Number of cols: 2  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>R.xc  
('John', '21', 'Male', 'UofA')  
('Mary', '22', 'Female', 'UofA')  
('Jeff', '20', 'Male', 'UofA')  
Number of rows: 3  
Number of cols: 4  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>X = R.xr.xc  
('John', '21', 'Male', 'UofA')  
('Jeff', '20', 'Male', 'UofA')  
('Mary', '22', 'Female', 'UofA')  
('Joe', '19', 'Male', 'UofC')  
('Sarah', '24', 'Female', 'UofA')  
('Jim', '22', '', '')  
('Alexander', '21', 'Male', 'UofA')  
('Rachel', '26', 'Female', 'UofC')  
('Vera', '40', 'Female', 'Concordia')  
Number of rows: 9  
Number of cols: 4  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>Y = R.xr  
('John', '21')  
('Jeff', '20')  
('Mary', '22')  
('Joe', '19')  
('Sarah', '24')  
('Jim', '22')  
('Alexander', '21')  
('Rachel', '26')  
('Vera', '40')  
Number of rows: 9  
Number of cols: 2  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>Z = R.xc  
('John', '21', 'Male', 'UofA')  
('Mary', '22', 'Female', 'UofA')  
('Jeff', '20', 'Male', 'UofA')  
Number of rows: 3  
Number of cols: 4  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>X  
('John', '21', 'Male', 'UofA')  
('Jeff', '20', 'Male', 'UofA')  
('Mary', '22', 'Female', 'UofA')  
('Joe', '19', 'Male', 'UofC')  
('Sarah', '24', 'Female', 'UofA')  
('Jim', '22', '', '')  
('Alexander', '21', 'Male', 'UofA')  
('Rachel', '26', 'Female', 'UofC')  
('Vera', '40', 'Female', 'Concordia')  
Number of rows: 9  
Number of cols: 4  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>Y  
('John', '21')  
('Jeff', '20')  
('Mary', '22')  
('Joe', '19')  
('Sarah', '24')  
('Jim', '22')  
('Alexander', '21')  
('Rachel', '26')  
('Vera', '40')  
Number of rows: 9  
Number of cols: 2  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
  
\>Y.xc  
('John', '21', 'Male', 'UofA')  
('Jeff', '20', 'Male', 'UofA')  
('Mary', '22', 'Female', 'UofA')  
('Joe', '19', 'Male', 'UofC')  
('Sarah', '24', 'Female', 'UofA')  
('Jim', '22', '', '')  
('Alexander', '21', 'Male', 'UofA')  
('Rachel', '26', 'Female', 'UofC')  
('Vera', '40', 'Female', 'Concordia')  
Number of rows: 9  
Number of cols: 4  
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-  
