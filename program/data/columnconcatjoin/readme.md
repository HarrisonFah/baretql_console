Dataset containing tables without schema for testing table search and join using column concatenation.
The data is taken from WikiTables (http://websail-fe.cs.northwestern.edu/TabEL/).
Each folder has three files, left.csv, right.csv, and metadata.txt.
The left.csv contains a table in which two or more of the columns concatentate together to join a column in the table in right.csv.
The metadata.txt file contains information about which column from the table was split, which delimiter was used to split, and how many columns it was split into.