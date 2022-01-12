from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


client = MongoClient("mongodb+srv://<User>:<Password>@cluster0.cslgm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["Apartment_Data"]
collection = db["Oikotie_vuokra"]
col = db["Oikotie_vuokrattavat"]
def plot_df(df,title,name):
        print("plot....",title,name)
        Vuokrahinnat = df.pivot_table(index=["Kaupunginosa"], values=["Neliövuokra"], aggfunc=np.mean)
        Vuokrahinnat = Vuokrahinnat.rename(columns={"Neliövuokra":"Keskineliövuokra"})
        Vuokrahinnat = Vuokrahinnat.reset_index()
        df = df.merge(Vuokrahinnat, left_on="Kaupunginosa",right_on="Kaupunginosa")
        df = df.sort_values("Keskineliövuokra")
        plt.bar("Kaupunginosa", "Keskineliövuokra", data = df, color=(0.1, 0.1, 0.1, 0.1),  edgecolor="blue")
        neliöhinta_ka = df["Neliövuokra"].mean()
        plt.axhline(neliöhinta_ka, color = "k")
        plt.xlabel("Kaupunginosa")
        plt.xticks(fontsize = 10, rotation = 90)
        plt.ylabel("Neliövuokra")
        plt.title(title)
        plt.grid(axis="y")
        fig = plt.gcf()
        fig.set_size_inches(32,18)     
        plt.savefig("img/"+name+".pdf")
        plt.close()



def main():
    all_data = pd.DataFrame(list(collection.find()))
    
#   Preparing data 
    df = all_data[["Kaupunginosa","osoite","Huoneiston kokoonpano","Huoneita","Asuinpinta-ala","Vuokra/kk","Vesimaksu","Asunnossa sauna","Rakennuksen tyyppi","Kerros"]]
    df["Vuokra/kk"] = df["Vuokra/kk"].str.split("€").str[0]
    df["Vuokra/kk"] = df["Vuokra/kk"].str.strip()
    df["Vuokra/kk"] = df["Vuokra/kk"].str.replace(",", ".")
    df["Vuokra/kk"] = df["Vuokra/kk"].str.replace("\xa0","")
    df = df[df["Vuokra/kk"] != "Kysy hintaa"]
    df["Vuokra/kk"] = df["Vuokra/kk"].astype(float)
    df["Vesimaksu"] = df["Vesimaksu"].str.split("€").str[0] 
    df["Vesimaksu"] = df["Vesimaksu"].str.strip()
    df["Vesimaksu"] = df["Vesimaksu"].str.replace(",", ".")
    df["Vesimaksu"] = df["Vesimaksu"].str.replace("\xa0","")
    
    df = df.replace([np.inf, -np.inf], np.nan).dropna(axis=0)
    df["Huoneita"] = df["Huoneita"].astype(int)

    df_list = []
    df = df[["Kaupunginosa","Huoneita","Asuinpinta-ala","Vuokra/kk"]]
    print(df)
    
#   All apartments
    df["Asuinpinta-ala"] = df["Asuinpinta-ala"].str.replace(",",".") 
    df["Asuinpinta-ala"] = df["Asuinpinta-ala"].astype(float)
    df["Neliövuokra"] = ((df["Vuokra/kk"]/df["Asuinpinta-ala"]))
    df_list.append(df)
    print("DF------------")
    #to mongo
    dataframe = df
    dataframe.reset_index(inplace=True)
    data_dict = dataframe.to_dict("records")
#   Insert collection
    col.insert_many(data_dict)
    
    print(df)
#   1 room
    df2 = df[df["Huoneita"]==1]
    df2["Neliövuokra"] = (df2["Vuokra/kk"]/df2["Asuinpinta-ala"])
    print("DF2------------")
    df_list.append(df2)
    print(df2)

#   2 room
    df3 = df[df["Huoneita"]==2]
    df3["Neliövuokra"] = (df3["Vuokra/kk"]/df3["Asuinpinta-ala"])
    df_list.append(df3)
    print("DF3------------")
    print(df3)

#   Aparments 31m^2-50m^2
    df4 = df[ (df["Asuinpinta-ala"]>30) & (df["Asuinpinta-ala"]<=50)]
    df4["Neliövuokra"] = (df4["Vuokra/kk"]/df4["Asuinpinta-ala"])
    df_list.append(df4)
    print("DF4(31-50neliöiset------------")
    print(df4)

    for i in range(len(df_list)):
        df = df_list[i]
        if i == 0:
            title = "Kaikki vuokra-asunnot"
            name = "Kaikkivuokra-asunnot"
        elif i == 1:
            title = "Yksiöt"
            name = "Yksiöt-vuokralla"
        elif i == 2:
            title = "Kaksiot"
            name = "Kaksiot-vuokralla"
        elif i == 3:
            title = "30-50 neliöiset helsingissä"
            name = "30m^2-50m^2-vuokralla"

        plot_df(df,title,name)






if __name__ == "__main__":
    main()

