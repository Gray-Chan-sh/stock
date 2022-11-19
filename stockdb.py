import akshare as ak
import pandas as pd 
import numpy as np
import datetime
import sqlite3





def initDB(db_name="db.db"):
    
    ## 如果数据库已经存在，则退出
    print("%s\tinitDB开始......"%(datetime.datetime.now().strftime("%H:%M:%S")))
    try :
        with open(db_name,"r") as f:
            print("%s\t数据库已存在，退出！"%(datetime.datetime.now().strftime("%H:%M:%S")))
            return False
    except:
        pass
    

    ## 初始化数据库
    try:
        conn = sqlite3.connect('db.db') 
        cursor=  conn.cursor()
        cursor.execute("CREATE TABLE \"DayK\" ("+
                "\"stockname\"	TEXT NOT NULL,"+
                "\"date\"	TEXT NOT NULL,"+
                "\"open\"	REAL NOT NULL,"+
                "\"close\"	REAL NOT NULL,"+
                "\"high\"	REAL NOT NULL,"+
                "\"low\"	REAL NOT NULL,"+
                "\"volume\"	REAL NOT NULL,"+
                "PRIMARY KEY(\"stockname\",\"date\")"+
                ");")
        conn.commit()
        cursor.close()
        conn.close()
        print("%s\t初始化数据库完成！"%(datetime.datetime.now().strftime("%H:%M:%S")))
    except Exception as e:
        print(str(e))
        print("%s\t数据库初始化失败！"%(datetime.datetime.now().strftime("%H:%M:%S")))
        return False
    
    return True

## 插入单个股票数据
def insertDayKDataToDb(db="db.db",dayklist=[]):
    try:
        print("%s\t开始写入%s日K线数据，共计%d条......"%(datetime.datetime.now().strftime("%H:%M:%S"),dayklist[0][0],len(dayklist)))
        try:
            conn = sqlite3.connect(db) 
            cursor=  conn.cursor()
            try:
                cursor.executemany("insert into DayK values(?,?,?,?,?,?,?)",dayklist)
            except Exception as e1:
                pass
            finally:
                conn.commit()
            print("%s：\t写入%s日K线完成，共写入%d条！"%(datetime.datetime.now().strftime("%H:%M:%S"),dayklist[0][0],cursor.rowcount))
            cursor.close()
            conn.close()
        except Exception as e :
            print("%s\t写入%s失败！"%(datetime.datetime.now().strftime("%H:%M:%S"),dayklist[0][0]))
            print(str(e))
            return False
    except:
        pass 
    return True


## 获得股票列表

def getStockList():
    try:
        print("%s\t开始抓取股票清单......"%datetime.datetime.now().strftime("%H:%M:%S"))
        stock_info_list = ak.stock_info_a_code_name()
        print("%s\共抓取股票数据%d个"%(datetime.datetime.now().strftime("%H:%M:%S"),len(stock_info_list)))
        return stock_info_list.values.tolist()
    except Exception as e :
        print(str(e))
        print("%s\t抓取股票清单失败！"%datetime.datetime.now().strftime("%H:%M:%S"))
        
        
        
        
## 抓取指定股票在指定日期的数据
## 如果抓取到了，返回相应数据列表
## 如果出错，返回None
def getDayK(stock_code,start_date=datetime.datetime.now().strftime("%Y%m%d"), end_date=datetime.datetime.now().strftime("%Y%m%d")):
    print("%s\t开始获取%s %s至%s的日K线数据......"%(datetime.datetime.now().strftime("%H:%M:%S"),stock_code,start_date,end_date))
    try:
        stock_qfq_df = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust='qfq').iloc[:, :6]
        # 名称	类型	描述
        # 日期	    object	    交易日
        # 开盘	    float64	    开盘价
        # 收盘	    float64	    收盘价
        # 最高	    float64	    最高价
        # 最低	    float64     最低价
        # 成交量	int32       注意单位: 手
        # 成交额	float64	    注意单位: 元
        # 振幅	    float64	    注意单位: %
        # 涨跌幅	float64	    注意单位: %
        # 涨跌额	float64	    注意单位: 元
        # 换手率	float64	    注意单位: %
        stock_qfq_df.columns = ['date', 'open', 'close', 'high', 'low', 'volume']
        stock_qfq_df.index = pd.to_datetime(stock_qfq_df['date'])
        ns = stock_qfq_df.values.tolist()
        ## ones_col=[[stock_code] for i in range(len(stock_qfq_df.values))]
        ns = np.insert(ns,0,stock_code,axis=1)
        print("%s\t%s日K线数据获取完成，共计%d条！"%(datetime.datetime.now().strftime("%H:%M:%S"),stock_code,len(ns.tolist())))
        return ns.tolist()

    except Exception as e:
        print(str(e))
        return None

    return None
