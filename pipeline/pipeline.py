import sys
import pandas as pd

month = int(sys.argv[1])
df = pd.DataFrame(
    {
        "col A":[1, 2, 3],
        "col B":[3, 4, 5]
    }
)
df['Month'] = pd.Series(dtype='Int64')
i=0
while i<3:
    df.loc[i, 'Month'] = month+i
    i+=1

# df.to_parquet(f"output_{month}.parquet")

print(df)
print(df.dtypes)
print("Month=",month)