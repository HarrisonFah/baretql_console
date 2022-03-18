INPUT_NAME = "../program/data/multiColumnJoinData/multiColumnJoinDataset.txt"
OUTPUT_PREFIX = "../program/data/multiColumnJoinData/multiColumnTable_"

def main():
    with open(INPUT_NAME, "r", encoding="UTF-8") as f:
        lines = list(f)
        table = []
        index = 1
        for line_index in range(len(lines)):
            line = lines[line_index]
            if line == "\n":
                if len(table) > 0:
                    writeFile(table, index)
                    index += 1
                table = []
            else:
                firstCol = line.split('",')[0].strip('"')
                colSplit = firstCol.split(" ", 1)
                start = colSplit[0]
                if len(colSplit) > 1:
                    end = colSplit[1]
                else:
                    end = ""
                table.append([start, end])

def writeFile(table, index):
    with open(OUTPUT_PREFIX + str(index) + ".txt", "w", encoding="UTF-8") as f:
        f.write("\n\n")
        for line in table:
            f.write("\"" + line[0] + "\", \"" + line[1] + "\"\n")

main()