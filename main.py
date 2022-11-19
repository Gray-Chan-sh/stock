import stockdb

v = stockdb.getStockListFromDB()
# for s in sl:
#     stockdb.insertDayKDataToDb("db.db",stockdb.getDayK(s[0],start_date='20220701'))

print(v)