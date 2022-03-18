def edit_distance(x, y):
    '''
    Function that counts the minimum Levenshtein distance between two strings.

    Arguments:
        x: String 1
        y: String 2

    Returns:
        Minimum Levenshtein distance
    '''
    n = len(x)
    m = len(y)

    if n > 0 and m > 0:
        # Initialize the matrix
        matrix = []
        matrix.append([0])            
        for j in range(1, m + 1):
            matrix[0].append(j)        
        for i in range(1, n + 1):
            matrix.append([])
            for j in range(m + 1):
                matrix[i].append(0)
            matrix[i][0] = i
            
        # Iterate over each cell in the matrix and use the previous cells
        # to determine the least costly way of changing x into y.
        # Addition and substraction have a cost of 1 and substitution has a 
        # cost of 2.
        for i in range(1, n + 1):
            for j in range(1, m + 1):
                if x[i - 1] == y[j - 1]:
                    matrix[i][j] = matrix[i - 1][j - 1]
                else:
                    k = matrix[i][j - 1] + 1
                    l = matrix[i - 1][j] + 1
                    matrix[i][j] = k if (k) <= (l) else (l)
                    
        return matrix[n][m]
    
    else:
        return max(n, m)

def edit_distance_ratio(x, y):
    '''
    Function that calcalutes the edit distance ratio between two strings
    '''
    e = edit_distance(x, y)
    # Doing 1 - because in ApproxMatch that is what is required
    return (1 - e / (len(x) + len(y)))

def print_matrix(matrix):
    n = len(matrix)
    for i in range(n):
        print(matrix[i])

def test():
    x = input("X: ")
    y = input("Y: ")
    print(edit_distance_ratio(x, y))

if __name__ == "__main__":
    test()