#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from pandas import Series, DataFrame
from pandasql import sqldf, load_meat, load_births
from numpy import *
from decimal import *


#计算 sql 配置
config = {
'name':[
'男女消费额及占比',
'男女用户数及占比',
'男女人均消费额(客单价)及占比',
'男女件单价及占比',
'不同年龄段消费额及占比',
'不同年龄段用户数及占比',
'不同年龄段人均消费额(客单价)及占比',
'不同年龄段件单价及占比',
'已婚未婚消费额及占比',
'已婚未婚用户数及占比',
'已婚未婚人均消费额(客单价)及占比',
'已婚未婚件单价及占比',
'产品类别消费额排行',
'Top10产品消费额',
'Top10产品男女消费额及占比',
'男性消费Top10产品',
'女性消费Top10产品',
'不同年龄段各产品类别消费额',
'不同年龄段各产品类别消费次数',
'各产品类别已婚未婚消费额及占比',
'各产品类别已婚未婚消费次数及占比',
"已婚未婚各类别产品消费额及总占比",
'不同城市消费额及占比',
'不同城市用户数及占比',
'不同城市人均消费额(客单价)及占比',
'不同城市消费频次及占比',
'不同城市件单价及占比',
],
'sql':[
"SELECT CASE `Gender` WHEN 'M' THEN '男' ELSE '女' END AS '类别', SUM(`Purchase`) AS '合计' FROM df GROUP BY `Gender` ORDER BY `Gender` DESC",
"SELECT CASE `Gender` WHEN 'M'THEN '男' ELSE '女' END AS '类别',COUNT(DISTINCT `User_ID`) AS '合计' FROM df GROUP BY `Gender` ORDER BY `Gender` DESC",
"SELECT CASE `Gender` WHEN 'M' THEN '男' ELSE '女'  END AS '类别',ROUND(SUM(`Purchase`)*1.0/ COUNT(DISTINCT `User_ID`)) AS '合计' FROM df GROUP BY `Gender` ORDER BY `Gender` DESC",
"SELECT CASE `Gender` WHEN 'M' THEN '男' ELSE '女' END AS '类别', ROUND(SUM(`Purchase`)*1.0/COUNT(Product_ID)) AS '合计' FROM df GROUP BY `gender` ORDER BY `Gender` DESC",
"SELECT `Age` AS '类别',SUM(`Purchase`) AS '合计' FROM df GROUP BY `Age` ORDER BY `Age`",
"SELECT `Age` AS '类别',COUNT(DISTINCT `User_ID`) AS '合计' FROM df GROUP BY `Age` ORDER BY `Age`",
"SELECT `Age` AS '类别',ROUND(SUM(`Purchase`)*1.0/COUNT(DISTINCT `User_ID`)) AS '合计' FROM df GROUP BY `Age` ORDER BY `Age`",
"SELECT `Age` AS '类别',ROUND(SUM(`Purchase`)*1.0/COUNT(DISTINCT `Product_ID`)) AS '合计' FROM df GROUP BY `Age` ORDER BY `Age`",
"SELECT CASE `Marital_Status` WHEN '0' THEN '未婚' ELSE '已婚' END AS '类别', SUM(`Purchase`) AS '合计' FROM df GROUP BY `Marital_Status` ORDER BY `Marital_Status`",
"SELECT CASE `Marital_Status` WHEN '0'THEN '未婚' ELSE '已婚' END AS '类别',COUNT(DISTINCT `User_ID`) AS '合计' FROM df GROUP BY `Marital_Status` ORDER BY `Marital_Status`",
"SELECT CASE `Marital_Status` WHEN '0' THEN '未婚' ELSE '已婚'  END AS '类别',ROUND(SUM(`Purchase`)*1.0/ COUNT(DISTINCT `User_ID`)) AS '合计' FROM df GROUP BY `Marital_Status` ORDER BY `Marital_Status`",
"SELECT CASE `Marital_Status` WHEN '0' THEN '未婚' ELSE '已婚' END AS '类别', ROUND(SUM(`Purchase`)*1.0/COUNT(Product_ID)) AS '合计' FROM df GROUP BY `Marital_Status` ORDER BY `Marital_Status`",
"SELECT `Product_Category` AS '类别',SUM(`Purchase`) AS '合计' FROM df GROUP BY `Product_Category` ORDER BY `合计` DESC",
"SELECT `Product_ID`,SUM(`Purchase`) AS '总和' FROM df GROUP BY `Product_ID` ORDER BY `总和` DESC LIMIT 10",
[
"SELECT `Product_ID`,CASE `Gender` WHEN 'M' THEN '男' ELSE '女' END AS '类别',SUM(`Purchase`) AS '类别合计' FROM df WHERE `Product_ID` IN(SELECT `Product_ID` FROM df GROUP BY `Product_ID` ORDER BY SUM(`Purchase`) DESC LIMIT 10)  GROUP BY `Product_ID`,`Gender`",
"SELECT `Product_ID`,SUM(`Purchase`) AS '总和' FROM df  GROUP BY `Product_ID` ORDER BY `总和` DESC LIMIT 10",
],
"SELECT `Product_ID`,SUM(`Purchase`) AS '总消费额' FROM df  WHERE `Gender`='M' GROUP BY `Product_ID` ORDER BY `总消费额` DESC LIMIT 10",
"SELECT `Product_ID`,SUM(`Purchase`) AS '总消费额' FROM df  WHERE `Gender`='F' GROUP BY `Product_ID` ORDER BY `总消费额` DESC LIMIT 10",
"SELECT `Age` AS '年龄',`Product_Category` AS '产品分类',SUM(`Purchase`) AS '总和' FROM df GROUP BY `Age`,`Product_Category`",
"SELECT `Age` AS '年龄',`Product_Category` AS '产品分类',COUNT(`Product_ID`) AS '总和' FROM df GROUP BY `Age`,`Product_Category`",
[
"SELECT `Product_Category`,CASE `Marital_Status` WHEN '0' THEN '未婚' ELSE '已婚' END AS '类别',SUM(`Purchase`) AS '类别合计' FROM df  GROUP BY `Product_Category`,`Marital_Status`",
"SELECT `Product_Category`,SUM(`Purchase`) AS '总和' FROM df   GROUP BY `Product_Category` ORDER BY `总和` DESC",
],
[
"SELECT `Product_Category`,CASE `Marital_Status` WHEN '0' THEN '未婚' ELSE '已婚' END AS '类别',COUNT(`Product_Category`) AS '类别合计' FROM df  GROUP BY `Product_Category`,`Marital_Status`",
"SELECT `Product_Category`,COUNT(`Product_Category`) AS '总和' FROM df   GROUP BY `Product_Category` ORDER BY `总和` DESC",
],
"SELECT `Product_Category`,CASE `Marital_Status` WHEN '0' THEN '未婚' ELSE '已婚' END AS '类别',SUM(`Purchase`) AS '合计' FROM df  GROUP BY `Product_Category`,`Marital_Status` ORDER by `合计` DESC",
"SELECT `City_Category` AS '类别', SUM(`Purchase`) AS '合计' FROM df GROUP BY `City_Category` ORDER BY `City_Category`",
"SELECT `City_Category` AS '类别',COUNT(DISTINCT `User_ID`) AS '合计' FROM df GROUP BY `City_Category` ORDER BY `City_Category`",
"SELECT `City_Category` AS '类别',ROUND(SUM(`Purchase`)*1.0/ COUNT(DISTINCT `User_ID`)) AS '合计' FROM df GROUP BY `City_Category` ORDER BY `City_Category`",
"SELECT `City_Category` AS '类别',COUNT(`Product_ID`) AS '合计' FROM df GROUP BY `City_Category` ORDER BY `City_Category`",
"SELECT `City_Category` AS '类别', ROUND(SUM(`Purchase`)*1.0/COUNT(Product_ID)) AS '合计' FROM df GROUP BY `City_Category` ORDER BY `City_Category`",
],
#是否计算占比 0-不需要 1-需要
"is_cal_pt":[1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,0,0,0,0,1,1,1,1,1,1,1,1],
#计算函数，为空默认调用 run_sql函数
"f":["","","","","","","","","","","","","","","run_multi_sql","","","run_age","run_age","run_multi_sql","run_multi_sql","","","","","",""],
#函数参数：计算函数传递的参数，为空默认传递 {'is_cal_pt': is_cal_pt}
"p":["","","","","","","","","","","","","","", {'mg_id': 'Product_ID', 'isOrder': True},"","","","",{'mg_id': 'Product_Category', 'isOrder': True},{'mg_id': 'Product_Category', 'isOrder': True},"","","","","",""],
"index":[False,False,False,False,False,False,False,False,False,False,False,False,False,False,False,False,False,True,True,False,False,False,False,False,False,False,False]#是否需要写入索引列
}

#格式化百分比
def format_pt(p):
    p = p * 100
    p = Decimal(p).quantize(Decimal('0.00'),ROUND_HALF_UP) #四舍五入保留两位小数
    result = "{}%".format(str(p)) #百分比展示
    return result

#计算占比
def cal_pt(df, total):
    df['类别总和'] = total
    p = df['合计'] / total
    df['占比'] = format_pt(p)
    return df

#计算占比
def cal_sqls_pt(df, isOrder = False):
    p = df['类别合计'] / df['总和']
    df['占比'] = format_pt(p)
    if(isOrder):
       index = getattr(df, 'name')
       if(index % 2 == 0):
           df['order'] = index + 1
       else:
           df['order'] = index - 1
    return df

#执行单条sql
def run_sql(sql,**p):
    df = pysqldf(sql)
    if(p['is_cal_pt']):
        total = df['合计'].sum()
        df = df.apply(cal_pt, axis = 1, args = (total,)) 
    return df

#运行多条sql
def run_multi_sql(sqls,**p):
    rs1 = pysqldf(sqls[0])
    rs2 = pysqldf(sqls[1])
    rs = pd.merge(rs2, rs1,on = p['mg_id'])
    rs = rs.apply(cal_sqls_pt, axis = 1, args = (p['isOrder'],)) 
    if(p['isOrder']):
        rs = rs.sort_values(by = 'order')
        rs = rs.drop(columns=['order'])
    return rs    

#运行年龄相关sql
def run_age(sql,**p):
    df = pysqldf(sql)
    c = pd.DataFrame()
    for x in df.index:
        age = df.loc[x,"年龄"]    
        cg = df.loc[x,"产品分类"]
        c.loc[age,cg] = df.loc[x,"总和"]
    print(c)    
    return c

#执行 ql语句
pysqldf = lambda sql: sqldf(sql, globals())

df = DataFrame(pd.read_pickle('./黑色星期五销售数据.pkl'))
df_cf = DataFrame(config)
#执行 sql 语句，并将结果写入 excel 的工作表中
writer = pd.ExcelWriter("数据验证.xlsx")
for x in df_cf.index:
    name = df_cf.loc[x,"name"]
    sql = df_cf.loc[x,"sql"]
    is_cal_pt = df_cf.loc[x,"is_cal_pt"]
    f = df_cf.loc[x,"f"]
    p = df_cf.loc[x,"p"]
    if(f == ""):
        f = 'run_sql'
    if(p == ""):
        p  = {'is_cal_pt': is_cal_pt} 
    rs = globals()[f](sql,**p)
    rs.to_excel(writer, sheet_name = name, index=df_cf.loc[x,"index"])
    print(rs)        
writer.save()     
