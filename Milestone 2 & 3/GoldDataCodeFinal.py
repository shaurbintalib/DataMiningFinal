from urllib.request import Request, urlopen
import urllib
import string
from datetime import datetime
import json
import pymysql

url = "https://advcharts.investing.com/advinion2016/advanced-charts/1/1/8/GetRecentHistory?strSymbol=8836&iTop=1500&strPriceType=bid&strFieldsMode=allFields&strExtraData=lang_ID=1&strTimeFrame=1D"

class Gold:
    def __init__(self, url):
        self.url = url

    def get_data(self):
        res = urllib.request.urlopen(self.url)
        data_body = res.read().decode("utf-8")
        raw_data = json.loads(data_body)
        try:
            data_list = raw_data["data"]
            for item_dict in data_list:
                self.store(item_dict)
        except KeyError:
            print("Invalid Key!!!")

    def store(self,data):
        conn = pymysql.connect(host="127.0.0.1", user="root", passwd="password", db="mysql")
        cur = conn.cursor()
        try:
            cur.execute("USE DataMiningAssignment")
            sql_query = "INSERT INTO GoldDataFinal (golddate, openprice, highprice, lowprice, closeprice, volume)" \
                        " VALUES (%s,%s,%s,%s,%s,%s)"
            try:
                cur.execute(sql_query,
                            (data["date"], data["open"], data["high"], data["low"], data["close"], data["volume"]))
                cur.connection.commit()
                print("--done--")
            except KeyError:
                print("Invalid Key!!!")

        finally:
            cur.close()
            conn.close()

gold_data = Gold(url)
gold_data.get_data()