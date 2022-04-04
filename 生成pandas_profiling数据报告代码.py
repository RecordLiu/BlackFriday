#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import pandas_profiling

data = pd.read_pickle('./黑色星期五数据.pkl')
pfr = pandas_profiling.ProfileReport(data)
pfr.to_file("./黑色星期五数据概览.html")
