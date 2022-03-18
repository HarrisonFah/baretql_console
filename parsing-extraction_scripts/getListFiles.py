"""
This file is designed to access the different types of 
wikipedia pages entitled 'List of...' in an attempt to 
understand what separates a simple list page from a table.
We want to find what makes a table file special, as those are
the pages from which we will extract our test data.
"""


import re
import sys

""" Main function of library """
def main():
    file, title = getFiles()
    readInput(file, title)

"""gets the file to be used, returns path and title"""
def getFiles():
    file = 'txtFiles/'
    title = '\s*<title>List of '
    if len(sys.argv) <= 1:
        error()
    # Want to read in specific table
    title += ' '.join(sys.argv[1:])
    file += 'ex.txt'
    
    return file, title

"""Reads from stdin, passing to writeToFile once applicable"""
def readInput(file, title):
    regexs = {
        r"&quot;": "\"", # HTML artifacts
        r"&amp;": "&",
        r"&lt;": "<",
        r"&gt;": ">",
        r"br \/": " ",
        r"(\[\[)?(\]\])?": "", # [[...]] links
        r";(\/)?\w*;": "", # HTML code (;small;, etc.)
        r"\w*=(\")?.*(\")?":  "",
        r"!": "",
        r"File:.*\.\w{3}": "", # set photos to be empty links
        r"<.*(\\)?>": "", 
        r"'''": "",
        r"\|\|": "\", \"",
        r"\|": "\", \"",
    }
    file = open(file, 'w', encoding='UTF-8')
    foundPage = False
    count = 0
    for line in sys.stdin:
        count += 1
        if count % 1000000 == 0:
            print(count // (10 ** 6), "million lines read.")
        if re.match(title, line):
            foundPage = True
        if foundPage:
            writeToFile(file, line, regexs)
        if foundPage and re.match('\s*{{DEFAULTSORT', line):
            break
    file.close()

""" Applies formatting, writes to files"""
def writeToFile(file, line, regexs):
    # # Get rid of style, css
    # styles = re.search(';\|', line)
    # if styles is not None:
    #     line = line[styles.end()-1:]
    # for regex in regexs:
    #     line = re.sub(regex, regexs[regex], line)

    # https://stackoverflow.com/questions/27366479/python-3-os-walk-file-paths-unicodeencodeerror-utf-8-codec-cant-encode-s
    file.write(line.encode('utf-8', 'surrogateescape').decode('utf-8', 'replace'))

    return

""" Prints out errors if flags are not correct """
def error():
    print("Please enter the name of a list file after the 'list of'")
    exit()

if __name__ == "__main__":
    main()