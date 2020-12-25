from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Data Compare").getOrCreate()

#table1_query = spark.conf.get("")
#table2_query = spark.conf.get("")
#table1_alias = spark.conf.get("")
#table2_alias = spark.conf.get("")
#primary_key_list = spark.conf.get("")
#record_to_print = spark.conf.get("")


data1 =[('vini','raikwar',25,'mds','tcs',3),('pulkit','agarwal',27,'bry','deloitteö',5),('teju','gherade',23,'pune','tcs',1)] 
schema1 = ['name','surname','age','place','company','exp']

data2 = [('vini',25,'hadoop',2, 'tcs'),('pulkit',27,'spark',4,'deloitte')]
schema2 = ['name','age','technology','exp','company']

primary_key = ['name']

tbl1_alias = 'a'
tbl2_alias = 'b'

record_to_print = 3

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)

#df1 = spark.sql(table1_query)
#df2 = spark.sql(table2_query)

df1= df1.dropDuplicates()
df2= df2.dropDuplicates()

df1_col = df1.columns
df2_col = df2.columns
#common_col = list(set(df1_col).intersection(set(df2_col))) -- result set
common_col = list(set(df1_col) & set(df2_col))

##extra columns
extra_df1 = list(set(df1_col) - set(df2_col))
extra_df2 = list(set(df2_col) - set(df1_col)) 


if df1.count() != 0 or df2.count() != 0:
    print "***************** table1 count  *******************"
    df1.count()
    print "***************** table2 count  *******************"
    df2.count()    
else:
    print "no data is present in table."
    exit()
    
df_tbl1 = df1.select(common_col).selectExpr([ x + " " + "as" + " " + tbl1_alias + "_" + x for x in common_col]).na.fill("")
df_tbl2 = df2.select(common_col).selectExpr([ x + " " + "as" + " " + tbl2_alias + "_" + x for x in common_col]).na.fill("")

cond = [df_tbl1[tbl1_alias + "_" + x] == df_tbl2[tbl2_alias + "_" + x] for x in primary_key]
df_common = df_tbl1.join(df_tbl2, on = cond,how = "inner")


df_extra1 = df_tbl1.selectExpr([tbl1_alias + "_" + x for x in primary_key]).subtract(df_common.selectExpr([tbl1_alias + "_" + x for x in primary_key]))
df_extra2 = df_tbl2.selectExpr([tbl2_alias + "_" + x for x in primary_key]).subtract(df_common.selectExpr([tbl2_alias + "_" + x for x in primary_key]))


df_table1 = df_common.select(df_tbl1.columns)
df_table2 = df_common.select(df_tbl2.columns)
df_exact_match = df_table1.subtract(df_table1.subtract(df_table2))

if (record_to_print == -1):
    df_extra1.show(df_extra1.count(),False)
else:
    df_extra1.show(record_to_print,False)

col_unordered = [x for x in common_col if x not in primary_key]

columns = [x for x in df1_col if x in col_unordered]

df_join = None

for col in columns:
    try:
        col_select = primary_key + [col]
        df_diff = df_table1.selectExpr([tbl1_alias + "_" + x for x in col_select]).na.fill(" ").subtract(df_table2.selectExpr([tbl2_alias + "_" + x for x in col_select]).na.fill(" "))
        count = df_diff.count()
        print "column compare",col
        print "count: ",count
        if (count != 0):
                df_table2_temp = df_table2.selectExpr([tbl2_alias + "_" + x for x in col_select]).na.fill(" ")
                #df_diff_temp = df_diff.selectExpr([tbl1_alias + "_" + x for x in col_select]).na.fill(" ")
                cond1 = [df_diff[tbl1_alias + "_" + x] == df_table2_temp[tbl2_alias + "_" + x] for x in primary_key]
                df_join = df_diff.join(df_table2_temp,on = cond1,how = "left")
                compare_col = [tbl1_alias + "_" + col , tbl2_alias + "_" + col]
                remaining_cols = df_join.columns
                for i in compare_col:
                    remaining_cols.remove(i)
                final_cols = compare_col + remaining_cols
                df_join = df_join.select(final_cols)
                if (record_to_print == -1):
                    df_join.show(df_join,False)
                else:
                    df_join.show(record_to_print,False)
    except Exception as e:
        if str(e).lower().find("codec can't encode") != -1 and df_join != None:
            print "warn"
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
     

