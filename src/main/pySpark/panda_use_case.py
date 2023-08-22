import pandas as pd
import csv
import numpy as np

pd.set_option('display.max_columns',10)
pd.set_option('display.width',900)

df1 = pd.DataFrame([[1, 2, 3],
                    [3, 4, 5],
                    [5, 6, 7],
                    [7, 8, 9]])
print(df1)
print(type(df1))