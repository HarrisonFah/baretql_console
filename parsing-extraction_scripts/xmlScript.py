import xml.etree.ElementTree
import os

def main():
    # https://eli.thegreenplace.net/2012/03/15/processing-xml-in-python-with-elementtree/
    # taken on 2018-05-02
    directoryName = "fixedWebTables"
    directory = os.fsencode(directoryName)
    f = open("database.txt", "w+")

    # https://stackoverflow.com/questions/10377998/how-can-i-iterate-over-files-in-a-given-directory
    # taken on 2018-05-02
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        f.write("\n")
        if filename.endswith(".xml"): 
            e = xml.etree.ElementTree.parse(directoryName + "/" + filename).getroot()
            prevChildTag = None
            prevNone = False
            for child in e.iter():
                if child.tag == 'text':
                    if prevChildTag == 'text':
                        if child.text == None:
                            f.write(", None")
                        else:
                            f.write(', "' + child.text + '"')
                        prevChildTag = child.tag
                    elif prevChildTag == 'row' and child.text != None:
                        f.write(str('"' + child.text + '"'))
                        prevChildTag = child.tag
                        prevNone = False
                    elif prevChildTag == 'row' and child.text == None:
                        prevNone = True
                    
                elif child.tag == 'row' and not prevNone:
                    prevChildTag = child.tag
                    f.write("\n")
    f.close

main()