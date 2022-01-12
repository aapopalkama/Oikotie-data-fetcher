from selenium import webdriver
from bs4 import BeautifulSoup
import time
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
import datetime

#   Ignore
def add_latlng(dicts):
    lat = 66.192
    lng = 24.945

    return lat,lng

def fetch_oikotie(fetch_what):
#   Create connection to MongoDB
    client = MongoClient("mongodb+srv://palkaap:Nillaniemi123@cluster0.cslgm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client["Apartment_Data"]
    collection = db["Oikotie"]

    start_time = datetime.datetime.now()
    start_date = start_time.strftime("%Y/%m/%d %H:%M:%S")
    date_1 = datetime.datetime.strptime(start_date,"%Y/%m/%d %H:%M:%S")
    inactive_time = date_1 + datetime.timedelta(days=-2) 
       

#   Open url in webdriver and search number of pages needed to scrape
    driver = webdriver.Chrome(ChromeDriverManager().install())
    page = "https://asunnot.oikotie.fi/myytavat-asunnot?pagination=1&locations=%5B%5B64,6,%22Helsinki%22%5D%5D&cardType=100"
    driver.get(page)
    con = driver.page_source
    soup = BeautifulSoup(con,features="html.parser")
    x = soup.find("pagination-indication",class_="ng-isolate-scope")
    result=x.find("span", attrs={"class": "ng-binding"})
    result = result.string
    result = result.split("/")
    result = result[1]
    result = int(result)

#   Make list of urls to scrape
    list = []
    for i in range(1,result):
        url_1="https://asunnot.oikotie.fi/myytavat-asunnot?pagination="
        url_2=str(i)
        url_3="&locations=%5B%5B64,6,%22Helsinki%22%5D%5D&cardType=100"
        list.append(url_1+url_2+url_3)
    print("************************",len(list))
    urls = []
    osoite = []
    length = (len(list)-1)


    print("First - loop through ",length, "urls")
    for i in list[0:length]:
        url = i
        print("URL:",url)
        driver.get(url)
#   Time.sleep so u dont get ip ban
        time.sleep(0.5)
        con = driver.page_source
        soup = BeautifulSoup(con,features="html.parser")
        try:
            for x in soup.find_all("div",href=False, attrs={"class":"cards__card"}):
                Osoite = x.find("div", attrs={"class": "ot-card__street"})
                osoite.append(Osoite.text)
                all_atags = x.find_all("a")
                for one_atag in all_atags:
                    url = one_atag.get("href")
                    urls.append(url)
                    print(" --> found ",url)
                    
        except AttributeError:
            print("attribute error",url)
            pass
        
        print("  number of urls",len(urls))

    length = (len(urls)-1)
    info_dict = {}
    i = 0
    print("second************************",length)
    
    Velaton_hinta = 0

    for url in urls[0:length]:

        now = datetime.datetime.now()
        dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
        dicts = {}
        dicts["source"] = "oikotie"
        dicts["Status"] = "Active"
        dicts["url"] = url
        dicts["osoite"] = osoite[i]
        i += 1
        driver.get(url)
        time.sleep(0.5)
        con = driver.page_source
        soup = BeautifulSoup(con, features="html.parser")
        try:
            for f in soup.find_all("div", href=False, attrs={"class": "info-table__row"}):
                title = f.find("dt", attrs={"class": "info-table__title"})
                value = f.find("dd", attrs={"class": "info-table__value"})
                title = title.string
                value = value.string
                dicts[title] = value
        except AttributeError:
            print("attribute error")
            pass
        try:
            for f in soup.find_all("div", href=False, attrs={"class": "details-grid__item-text"}):
                valuepairs = f.find_all("dl")
                for vp in valuepairs:
                    title = vp.find("dt", attrs={"class": "details-grid__item-title"})
                    value = vp.find("dd", attrs={"class": "details-grid__item-value"})
                    title = title.string
                    value = value.string

                    if title == "Velaton hinta" or title == "Myyntihinta":
                        
                        value = value.replace("€", "")
                        value = value.replace(u"\xa0", u"")
                        Velaton_hinta = value
                    elif title == "Asuinpinta-ala" or title == "Kokonaispinta-ala": # "85 m2"
                        result = value.split(" ")
                        value = result[0]
                        
                
                    
                    dicts[title] = value

        except AttributeError:
            print("attribute error")
            pass
    #   Insert dictionarys to MongoDB
        
        existing = collection.find_one({"url":url})
        if existing == None:
            print("Uusi",url,i,"/",length)
            lat,lng = add_latlng(dicts)
            dicts["Status"] = "New"
            dicts["lat"] = lat
            dicts["lng"] = lng
            dicts["created"] = dt_string
            dicts["timestamp"] = dt_string
            collection.insert_one(dicts)
        else:
            print("Päivitän",url,i,"/",length)
            newvalues = {"$set":{"timestamp":dt_string,"Velaton hinta":Velaton_hinta,"Status":"Updated"}}      
            collection.update_one({"url":url}, newvalues)

    print("now update those which are inactive") 
    result =  collection.update_many({"timestamp": {"$lt": inactive_time} }, {"$set" : {"Status":"Inactive"}})

    return length, result.modified_count


def main():
    print("start")
    count,old_modified = fetch_oikotie("asunnot")
    print("found count:",count)
    print("found old:",old_modified)
#    fetch_oikotie("tontit")
#    fetch_oikotie("loma-asunnot")
#    fetch_oikotie("vuokra-asunnot")

main()
