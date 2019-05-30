from urllib.request import Request, urlopen
import urllib
import string
from datetime import datetime
import json
import pymysql

cat_dictionary = [{"main_market": "healthcare"}, {"main_market": "energy"}, {"main_market": "tecnology"},
                  {"main_market": "properties"}, {"main_market": "utilities"},{"main_market": "finance"},
                  {"main_market": "telco_media"}, {"main_market": "consumer"},{"main_market": "constructn"},
                  {"main_market": "reits"},{"main_market": "ind-prod"}, {"main_market": "plantation"},
                  {"main_market": "transport"},{"main_market": "specialpurposeacquis"}, {"main_market": "closedfund"},
                  {"ace_market": "healthcare"}, {"ace_market": "technology"}, {"ace_market": "transport"},
                  {"ace_market": "finance"}, {"ace_market": "consumer"}, {"ace_market": "constructn"},
                  {"ace_market": "ind-prod"}, {"ace_market": "telco_media"}, {"ace_market": "utilities"},
                  {"bond_loan": "finance"}, {"bond_loan": "telco_media"}, {"bond_loan": "ind-prod"},
                  {"bond_loan": "properties"}, {"bond_loan": "energy"}, {"bond_loan": "bondislamic"},
                  {"warrants": "healthcare"}, {"warrants": "energy"}, {"warrants": "consumer"}, {"warrants": "properties"},
                  {"warrants": "finance"}, {"warrants": "telco_media"}, {"warrants": "transport"}, {"warrants": "constructn"},
                  {"warrants": "ind-prod"}, {"warrants": "plantation"}, {"warrants": "tecnology"}, {"warrants": "utilities"},
                  {"etf": "etfbond"},{"etf":"etfequity"},{"etf":"etfcommodity"}]



new_list = []
for item in cat_dictionary:
    for k, v in item.items():
        new_list.append(["https://s3-ap-southeast-1.amazonaws.com/biz.thestar.com.my/json/sectors/{0}/{1}/stocks.js".format(k, v), k, v])


def company_name(js_urls):

    for url in js_urls:
        try:

            res = urllib.request.urlopen(url[0])
            body = str(res.read()).split("=")
            body = body[1].replace("\\r\\n","")
            body = body.replace(body[0:14],"")
            body = body.replace("]};\'", "")
            data = eval(body)
            try:
                for item in data:
                    get_data(item,url[1],url[2])
            except:
                get_data(data,url[1],url[2])
        except:
             print("error3")
             continue


def get_data(name, sector, subsector):
    
    start_date = "1554123600"

        # "1426118400"
    end_date = "1556715600"

    # ""1555545600"

    url = "https://charts.thestar.com.my/datafeed-udf/history?symbol={0}&resolution=D&from={1}&to={2}".format(name["counter"], start_date, end_date)
    res = urllib.request.urlopen(url)
    body = res.read().decode()
    data = eval(body)
    store(data, name["counter"],name["stockcode"],sector,subsector)




def store(data,company_name, stock_code,sector,subsector):
    try:
        data_len = len(data["t"])
        conn = pymysql.connect(host="127.0.0.1", user="root", passwd="password", db="mysql")
        cur = conn.cursor()
        try:
            cur.execute("USE DataMiningAssignment")
            sql_query = "INSERT INTO StockDataFinal (stockdate,sector,subsector,companyname, stockcode, openprice, highprice," \
                        " lowprice, lastprice, volume)" \
                        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for i in range(data_len):
                try:
                    cur.execute(sql_query, (datetime.utcfromtimestamp(int(data["t"][i])).strftime("%Y-%m-%d %H:%M:%S"),
                                                sector,subsector,company_name, stock_code, data["o"][i], data["h"][i],data["l"][i],
                                                data["c"][i], data["v"][i]))
                except KeyError:
                    print(company_name, stock_code, sector, subsector, data)
                    continue
                cur.connection.commit()
        finally:
            print("--Done--")
            cur.close()
            conn.close()
    except KeyError:
        print("No Data")
        print(company_name, stock_code,sector,subsector,data)

if __name__ == "__main__":
   company_name(new_list)