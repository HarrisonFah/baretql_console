import UnitTagging

class Column:
    '''
    Class that represents a column
    '''
    def __init__(self, value, entries):
        self.value = value
        self.entries = entries
        self.num_entries = len(entries)
        self.tag = UnitTagging.tag_unit_for_column(self.entries)

    def __str__(self):
        return str(self.entries)

    def get_entry(self, i):
        '''
        Function that returns the entry at index i.

        Arguments:
            i: Index of the entry

        Returns:
            Entry at the index i.
        '''
        if 0 <= i < self.num_entries:
            return self.entries[i]
        else:
            print("Index given is out of range")
            return False

    def get_entries(self):
        '''
        Function that returns a copy of the entries in this column.
        '''
        return self.entries[:]

    def get_num_entries(self):
        '''
        Function that returns the number of entries in this column.
        '''
        return self.num_entries

    def get_value(self):
        '''
        Function that returns the relational value of the table this 
        column is in.
        '''
        return self.value

    def get_tag(self):
        '''
        Function that returns the tag associated to this column (either
        numeric, text or date).
        '''
        return self.tag

    def compare_col(self, col2):
        '''
        Function that compares self with another column. (Assumes that they 
        have same number of rows.)

        Arguments:
            col2: Second column

        Returns:
            A decimal number where 0 indicates theynare not related at all 
            and 1 indicates they are identical.
        '''
        count = 0
        matches = 0
        # Iterate over each row and compare the two rows entries
        for i in range(self.num_entries):
            # Do not consider rows that are empty
            if self.entries[i] == "" or col2.get_entry(i) == "":
                continue
            if self.entries[i] == col2.get_entry(i):
                matches += 1
            count += 1
        if count > 0:
            return matches / count
        else:
            return 0