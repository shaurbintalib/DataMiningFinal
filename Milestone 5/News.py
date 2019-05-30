from bs4 import BeautifulSoup
import requests
import pymysql.cursors

def get_url(urls):

    source = requests.get(urls[0]).text
    soup = BeautifulSoup(source, "lxml")
    news_links = soup.find_all("div", {"class": "views-field views-field-title"})
    company_name = urls[1]

    for _ in news_links:
        headline = _.find("a").get_text()
        url = _.find("a").attrs["href"]
        get_data(company_name,headline,url)


def get_data(company_name,headline,url):
    article = ""
    new_url = "https://www.theedgemarkets.com" + url
    print(new_url)
    try:
         source = requests.get(new_url).text
         soup = BeautifulSoup(source, "lxml")
         news = soup.find("div",{"property": "content:encoded"}).find_all("p")
         for _ in news:
             article += _.getText()
         news_date = soup.find("span", {"class": "post-created"}).getText()
         if(news_date and company_name and headline and article and new_url ):
             store(news_date,company_name, headline,article,new_url)
    except:
        print("Something went Wrong in get_data function")


def store(newsdate,company_name,headline,article,link):


    conn = pymysql.connect(host="localhost", user="root", passwd="root", db="mysql")
    cur = conn.cursor()
    try:
        cur.execute("USE newsdata")
        sql_query = "INSERT INTO companynews (newsdate,companyname,headline,article,link)" \
                    " VALUES (%s,%s,%s,%s,%s)"
        try:
            cur.execute(sql_query,(newsdate,company_name,headline,article,link))

        except ValueError:
                print("The is no Data to Store")
        cur.connection.commit()

    finally:
        cur.close()
        conn.close()


company_news_urls = []
company_names = ["CIMB", "RHBBANK"]

for item in company_names:
    for i in range (0,10):
        company_news_urls.append(["https://www.theedgemarkets.com/search-results?page={0}&keywords={1}".format(i,item), item])
for item in company_news_urls:
    get_url(item)