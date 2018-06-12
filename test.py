import pandas as pd

df = pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]})


def test(x):
    if x < 10:
        return x/2, x/4
    else:
        return x+1, x+2


df['new1'], _ = zip(*map(test, df['x']))



print(df)

