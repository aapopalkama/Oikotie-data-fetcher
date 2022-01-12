from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import date


client = MongoClient("mongodb+srv://<User>:<Password>@cluster0.cslgm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["Apartment_Data"]
collection = db["Oikotie"]
col = db["Oikotie_myytävät"]
db1 = client["Dash"]
col_neliöhinnat = db1["col_neliöhinnat"]


def plot_df(df,title,name):
        
        df_neliöhinnat_alueittain = df.pivot_table(index=["Kaupunginosa"], values=["Neliöhinta"], aggfunc=np.mean)
        df_neliöhinnat_alueittain = df_neliöhinnat_alueittain.rename(columns={"Neliöhinta":"Keskineliöhinta"})
        df_neliöhinnat_alueittain = df_neliöhinnat_alueittain.reset_index()
        df = df.merge(df_neliöhinnat_alueittain, left_on="Kaupunginosa",right_on="Kaupunginosa")
        df = df.sort_values("Keskineliöhinta")
        plt.bar("Kaupunginosa", "Keskineliöhinta", data = df, color=(0.1, 0.1, 0.1, 0.1),  edgecolor="blue")
        neliöhinta_ka = df["Neliöhinta"].mean()
        plt.axhline(neliöhinta_ka, color = "k")
        plt.xlabel("Kaupunginosa")
        plt.xticks(fontsize = 10, rotation = 90)
        plt.ylabel("Neliöhinta")
        
        plt.title(title)
        plt.grid(axis="y")
        fig = plt.gcf()
        fig.set_size_inches(32,18)     
        plt.savefig("img/"+name+".pdf")
       
        plt.close()
        


def main():
    
    all_data = pd.DataFrame(list(collection.find()))
#   Preparing data 
    df = all_data[["Kaupunginosa","Rakennuksen tyyppi","Asuinpinta-ala","Velaton hinta","Rakennusvuosi","Huoneiston kokoonpano","url","status","Hoitovastike","osoite"]]

#   Get how many rooms in house (No kitchen included)
    df["Huone_lkm"] = df["Huoneiston kokoonpano"].astype("str").str[0]

#   Clean data if there is more than 1 value    
    df["Velaton hinta"] = df["Velaton hinta"].str.split().str[0]
    df = df[df["Velaton hinta"].notna()]

#   Decimal point to dot 
    df["Asuinpinta-ala"] = df["Asuinpinta-ala"].str.replace(",",".")  
    df = df[["Kaupunginosa","Rakennuksen tyyppi","Asuinpinta-ala","Velaton hinta","Rakennusvuosi","Huone_lkm","Hoitovastike"]]

#   Size to float and price to int
    indexNames = df[ df["Velaton hinta"] == "Kysy" ].index 
    df.drop(indexNames , inplace=True)

    df = df.astype({"Asuinpinta-ala": float, "Velaton hinta": int})
    
#   Convert apartment maintenance charge to numeric
    df["Hoitovastike"] = df["Hoitovastike"].str.split("€").str[0]
    df["Hoitovastike"] = df["Hoitovastike"].str.strip()
    df["Hoitovastike"] = df["Hoitovastike"].str.replace(",", ".")
    df["Hoitovastike"] = df["Hoitovastike"].str.replace("\xa0","")

#   Calculate price per square meter of apartment maintenance
    df = df.astype({"Hoitovastike": float})
    df["Hoitovastike"] = (df["Hoitovastike"]/df["Asuinpinta-ala"])

#   Calculate sqm price and add it to df   
    df = df[df["Asuinpinta-ala"].notna()]


# ----- Check dataframe if there is inf values
#   count = np.isinf(data).values.sum()
#   print("iNF VALUES",count)
#   indexNum = data.index[np.isinf(data).any(1)]
#   print("\nDisplay row indexes with infinite values...\n "),indexNum
#   dataFrame = df_small.replace([np.inf, -np.inf], np.nan).dropna(axis=0)
# -----


#   Dataframe list
    df_list = []

#   All aparments
    df["Neliöhinta"] = (df["Velaton hinta"]/df["Asuinpinta-ala"])   
    df = df[df["Neliöhinta"].notna()]
    df = df.replace([np.inf, -np.inf], np.nan).dropna(axis=0)
    df_list.append(df)
   
    dataframe = df
    dataframe.reset_index(inplace=True)
    data_dict = dataframe.to_dict("records")
#   Insert collection
    col.insert_many(data_dict)

#   Apartment less than 30 square meters
    df2 = df[df["Asuinpinta-ala"] <= 30]
    df2["Neliöhinta"] = (df2["Velaton hinta"]/df2["Asuinpinta-ala"])  
    df_list.append(df2)

#   Aparments 31m^2-50m^2
    df3 = df[ (df["Asuinpinta-ala"]>30) & (df["Asuinpinta-ala"] <= 50)]
    df3["Neliöhinta"] = (df3["Velaton hinta"]/df3["Asuinpinta-ala"])
    df3 = df3.replace([np.inf, -np.inf], np.nan).dropna(axis=0)

    df_list.append(df3)

#   Omakotitalot, rivarit, paritalot
    df4 = df[ (df["Rakennuksen tyyppi"] == "Omakotitalo") | (df["Rakennuksen tyyppi"] == "Rivitalo") | (df["Rakennuksen tyyppi"] == "Paritalo")]
    df4["Neliöhinta"] = (df4["Velaton hinta"]/df4["Asuinpinta-ala"])
    df_list.append(df4)

#   kerrostalot, luhtitalot
    df5 = df[ (df["Rakennuksen tyyppi"] == "Kerrostalo") | (df["Rakennuksen tyyppi"] == "Luhtitalo")]
    df5["Neliöhinta"] = (df5["Velaton hinta"]/df5["Asuinpinta-ala"])
    df_list.append(df5)
    db1.col_neliöhinnat.insert_many(df.to_dict('records'))
    df_list = df_list
    today = str(date.today())


    

#   Loop through dataframe list and plot with matplotlib
    for i in range(len(df_list)):
        df = df_list[i]  
        
        if i == 0:
            title = "Neliöhinnat helsingissä kaikki asunnot"
            name = "Kaikki"
        elif i == 1:
            title = "Alle 30 neliöiset helsingissä"
            name = "Alle30"
        elif i == 2:
            title = "30-50 neliöiset helsingissä"
            name = "30m^2-50m^2"
        elif i == 3:
            title = "Omakotitalot, rivitalot ja paritalot "
            name = "Omakotitalor"
        elif i == 4:
            title = "Kerrostalot"
            name = "kerrostalot"
        plot_df(df,title,name) 

      



if __name__ == "__main__":
    main()
