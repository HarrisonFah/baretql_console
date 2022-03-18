"""
Tables.py contains all the necessary code for handling the different formats of tables
that are found on wikipedia. Although they may all look the same, the structure of the 
markup that created them can very wildly. Below are implementations of the most common 
markup structures, and methods for how to handle them.

For more information on markup syntax, check out 
https://en.wikipedia.org/wiki/Category:Wikipedia_how-to
Ctrl-F 'table'

Specifically, https://en.wikipedia.org/wiki/Help:Basic_table_markup
might be of use.
"""


import re
import sys

"""
An instance of this class represents a table on a wikipedia list page
"""
class Table():
    # order of regexs is important
    regexs = {
        r"&quot;": "\"", # HTML entities
        r"&amp;ndash;": "-",
        r"&amp;": "&",
        r"&lt;": "<",
        r"&gt;": ">",
        r"&ndash;": "-",
        r"br \/": " ",
        r"&nbsp;": " ", 
        r"colspan=\"?(\d+?)\"?.*?\|([^\|]*)\|?": lambda mO: ''.join((mO.group(2), '|')) * int(mO.group(1)),
        r"\[\[File:.*?(\]\])": "", # images / links
        r"(align|scope|rowspan|width)=(\")?.*?(\")?[^\|\[\{]*": "|",
        r"!": "|",
        r"{{abbr\|.*?\|(.*?)}}": r"\1", #Types of templates
        r"{{dts\|(.*?)}}": r"\1",
        r"{{DetailsLink\|.*?}}": "",
        r"{{ntsh\|.*?}}": "",
        r"{{sortname\|(.*?)\|(.*?)\|?.*?}}": r"\g<1> \g<2>",
        r"{{(.*?)}}?": r"\1",
        r"\[\[([^\|\]]*)\|?.*?\]\]": r"\1", # [[...]] links
        r";(\/)?\w*;": "", # HTML code (;small;, etc.)
        r"[\w-]*=\".*?\"":  "", # css styling
        r"'(?!s )": "",
        r"< >": " ",
        r"<(\\)?.*?(\/)?>": "", 
        r"[ ]*\|+[ ]*": "\", \"",
    }

    nestedRegexs = {
        r"\{\{(cite|efn)+[^\{\{]*?\}\}": "",
    }

    def __init__(self, file, debug):
        self.file = file
        self.debug = debug

    # Formats according to all regexs
    def formatLine(self, line):
        for regex in self.nestedRegexs:
            while re.search(regex, line):
                line = re.sub(regex, self.nestedRegexs[regex], line) 
        for regex in self.regexs:
            line = re.sub(regex, self.regexs[regex], line)
        return line

    # TODO: Handles all lines with wierd bracket symbols
    # ex. {{GER}} or {{flagIOCmedalist", "Sergei Tarasov (biathlete)", "Sergei Tarasov", "RUS", "1994 Winter}}
    def handleBrackets(self, line):
        pass

    def writeToFile(self, row):
        newRow = self.formatLine(row)
        row += '"'
        if self.debug:
            self.file.write(row.encode('utf-8', 'surrogateescape').decode('utf-8', 'replace'))
        self.file.write(newRow[3:].encode('utf-8', 'surrogateescape').decode('utf-8', 'replace'))
        if self.debug:
            self.file.write('\n')

        return

    
"""
WikiTable handles the most common form of table: 
{| class="wikitable"
|+ Caption: example table
|-
! header1
! ...
|-
| row1cell1
| ...
|-
| row2cell1
| ...
|}
"""
class WikiTable(Table):
    def __init__(self, file, debug):
        super().__init__(file, debug)
        self.row = ""

    def readTable(self):
        # Ignore header declarations
        line = sys.stdin.readline()
        header = ''
        while not re.match('\|-', line):
            line = line.strip()
            # if re.match('!', line):
            header += re.sub(r"^[^\|\]]", "|",line)
            line = sys.stdin.readline()
        super().writeToFile(header + '\n')

        # get rows of file
        for line in sys.stdin:
            line = line.strip()
            line = re.sub(r"^.*?\|", "|",line)
            if re.match('\|}', line): # end of table
                super().writeToFile(self.row + '\n')
                break
            if re.match('\|-', line): # end of row
                super().writeToFile(self.row + '\n')
                self.row = ""
                continue
            line = re.sub(r"^!", "|", line)
            self.row += line
        self.row = ''
        self.file.write('\n' * 3)
        return            

        
class BracketTable(Table):
    def __init__(self, title, output):
        super().__init__(title, output)