try:
    # Try to import Levenshtein module.
    # If they have it use Levenshtein.ratio to calculate Levenshtein distance.
    # If they don't have it then use EditDistance module instead.
    import Levenshtein
    func = Levenshtein.ratio
except ImportError:
    print(
        "WARNING:\n"
        + "You do not have the 'python-Levenshtein' module downloaded.\n"
        + "The 'python-Levenshtein' module is strongly recommended due\n"
        + "to it's processing speed.\n")  
    import EditDistance
    func = EditDistance.edit_distance_ratio

def approximate_match_distance(str1,str2):
    '''
    Function that uses global variable func to calculate the Levenshtein 
    distance between two strings
    '''
    global func
    ans = (1 - func(str1, str2))
    return ans

def is_approx_match(str1, str2, threshold):
    '''
    Function that determines if str1 and str2 are approximate matches 
    based on the threshold. Two string are considered matches if the 
    approximate distance of two strings is lower than 
    approximate_match_distance_threshold.

    Arguments:
        str1: First string
        str2: Second string
        threshold: approximate_match_distance_threshold
    
    Returns:
        A boolean indicating whether the two strings are approximately similar
    '''
    len1 = len(str1)
    len2 = len(str2)
    # Determine whether it's necessary to calculate Levenshtein distance
    # or if we can do a straight comparison.
    if threshold == 0 or (len1 * threshold) < 1 or (len2 * threshold) < 1:
        return str1 == str2
    else:
        distance = approximate_match_distance(str1, str2)
        if (distance < threshold):
            return True
        else:
            return False

if (__name__ == "__main__"):
    x = input("X: ")
    y = input("Y: ")    
    (approximate_match_distance(x, y))
    #(is_approx_match("Manchester United 1","unites states of ame", 0.2))
    #(approximate_match_distance("America, United States","unites states of america"))
