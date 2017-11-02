# from pig_utils import outputSchema
# Pig UDF returns a bag of 2-element tuples

@outputSchema('result:{t:(day:chararray,count:chararray)}')
def getcount(bag):
    result = []
    # Select first 2 items i group only
    for num in bag:
        n = int(num)
        result.append(n)

    return result
