#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from pandas import Series,DataFrame
import pandas_profiling


df = DataFrame(pd.read_pickle('./黑色星期五数据.pkl'))

# #删除 Product_ID 为空值的数
df.dropna(axis=0,how='any', inplace=True)

# #删除 Purchase 为负数的记录
for x in df.index:
    if df.loc[x, "Purchase"] < 0:
        df.drop(x, inplace = True)

df.to_excel('./清洗完成数据.xlsx',index = False)
