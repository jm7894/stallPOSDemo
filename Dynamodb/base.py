notNone = lambda x: x is not None
toFloat = lambda x: float(x)
toInt = lambda x: int(x)
notZero = lambda x: x != 0
convertEmptyString = lambda x: x or ""
toString = lambda x: str(x)
isPositive = lambda x: x > 0
notNegative = lambda x: x >= 0
# _run = lambda x, xs: xs[0](x) and _run(x, xs[1:]) if xs[1:] else xs[0](x)

def run(x, *args):
    try:
        for func in args:
            r = func(x)
            if isinstance(r, bool):
                if r is False:
                    return False
            else:
                x = r
        return x
    except ValueError as e:
        return False

def batch_run(*args):
    results = []
    for set in args:
        r = run(set[0], *set[1:])
        if r is False:
            return False
        else:
            results.append(r)
    return results

def removeEmptyValues(dictionary):
    for key, val in dictionary.items():
        if not val:
            del dictionary[key]

# print(run(1, notNone, isPositive, toFloat))
# print(run(-1, notNone, toFloat, isPositive))
    
