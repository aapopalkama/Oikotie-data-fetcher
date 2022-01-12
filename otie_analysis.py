from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

client = MongoClient("mongodb+srv://<User>:<Password>@cluster0.cslgm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["Apartment_Data"]
collection = db["Oikotie_vuokrattavat"]
col = db["Oikotie_myytävät"]
db2 = client["Dash"]
col_dat = db2["fig"]
col_dat1 = db2["mer_dat"]
col_dat2 = db2["vuokra_count"]
col_dat3 = db2["myytävät_count"]

def ready_dat(df_scat):
    db2.col_dat.insert_many(df_scat.to_dict('records'))
    print(df_scat)



def database(df3,vuokra_count,myytävät_count,):
    print(df3)
    db2.col_dat1.insert_many(df3.to_dict('records'))
    print(vuokra_count)
    db2.col_dat2.insert_many(vuokra_count.to_dict('records'))
    print(myytävät_count)
    db2.col_dat3.insert_many(myytävät_count.to_dict('records'))
    



def create_price_rent_df(vuokra_df,hinta_df):
#   Count how many apartments are for sale and rent in a given number of rooms
    dups = vuokra_df[["Kaupunginosa","Huoneita"]].value_counts()
    dups = dups.reset_index(name="for_rent")

    dups1 = hinta_df[["Kaupunginosa","Huoneita"]].value_counts()
    dups1 = dups1.reset_index(name="for_sale")
   
    vuokra_df = vuokra_df.groupby(['Kaupunginosa', 'Huoneita']).mean()
    vuokra_df = vuokra_df.rename(columns={"Neliövuokra":"Keskineliövuokra"})
    vuokra_df = vuokra_df.reset_index()
    
    hinta_df = hinta_df.groupby(["Kaupunginosa","Huoneita"]).mean()
    hinta_df = hinta_df.rename(columns={"Neliöhinta":"Keskineliöhinta"})
    hinta_df = hinta_df.reset_index()
#   Connect two dataframes to one, with two conditions district and how many rooms   
    df3 = pd.merge(hinta_df, vuokra_df, on=["Kaupunginosa","Huoneita"])

    database(df3,dups,dups1)
    
    return df3,dups,dups1



def plot_comparison(df,vuokra_count,myytävät_count,rooms):
   
    rooms_list = ["None","Yksiöiden","Kaksioiden","Kolmioiden","Neliöiden","Viisiöiden","Kuusioiden"]
    if rooms <1 or rooms > 6:
        print("huonelkm pitää olla 1-6")
        return
    title = rooms_list[rooms]


    df_hoitovastike = df[["Kaupunginosa","Huoneita","Hoitovastike","Keskineliöhinta","Keskineliövuokra"]]
    df_hoitovastike = df_hoitovastike.rename(columns={"Hoitovastike":"Hoitovastike_per_neliö"})
    df_hoitovastike = df_hoitovastike[df_hoitovastike["Huoneita"]== str(rooms)]
    df_hoitovastike["Keskineliöhinta"] = df_hoitovastike["Keskineliöhinta"]*0.001
    df_hoitovastike.sort_values(by="Hoitovastike_per_neliö", inplace=True)
   
    df_hoitovastike.plot(x="Kaupunginosa",y=["Hoitovastike_per_neliö","Keskineliöhinta","Keskineliövuokra"],kind="bar")
    plt.xlabel("Kaupunginosa")
    plt.ylabel("Suhde")

#    dataframe = df
#    dataframe.reset_index(inplace=True)
#    data_dict = dataframe.to_dict("records")
#   Insert collection
#    col.insert_many(data_dict)


   
#   Select from dataframe only those apartments with a certain number of rooms
    vuokra_count = vuokra_count[vuokra_count["Huoneita"] == str(rooms)]
    myytävät_count = myytävät_count[myytävät_count["Huoneita"] == str(rooms)]
#   Merge to same dataframe based on district
    data = pd.merge(vuokra_count,myytävät_count, on="Kaupunginosa")
    
   

    data.plot(x="Kaupunginosa",y=["for_rent","for_sale"],kind ="bar")
    plt.xlabel("Kaupunginosa")
    plt.ylabel("Lukumäärä")
    plt.title(title+" "+"lukumäärät")

    df = df[df["Huoneita"] == str(rooms)]
    df["Tehokkuus"] = (df["Keskineliövuokra"]/(df["Keskineliöhinta"]*0.001))
    df.sort_values(by="Tehokkuus", inplace=True)
    df.plot(x="Kaupunginosa",y="Tehokkuus",kind="bar")
    neliöhinta_ka = df["Tehokkuus"].mean()
    plt.title(title+" "+"vuokra/myyntihinta tehokkuus")
    plt.axhline(neliöhinta_ka, color = "r")

#   Plot scatter plot
    ready_dat(df)
    ax = df.plot(x="Keskineliöhinta",y="Keskineliövuokra",kind="scatter",figsize=(10,10))
    df[["Keskineliöhinta","Keskineliövuokra","Kaupunginosa"]].apply(lambda x: ax.text(*x),axis=1)
#   Define the title 
    plt.title(title+" "+"Keskineliöhinnan ja keskineliövuokra korrelaatio")
    
    plt.show() 

def main():
#   Connect to mongodb and edit the dataframe 
    
    data1 = pd.DataFrame(list(collection.find()))
    
    data2 = pd.DataFrame(list(col.find()))  
    
    df = data1[["Kaupunginosa","Huoneita","Asuinpinta-ala","Vuokra/kk","Neliövuokra"]]
    df1 = data2[["Kaupunginosa","Rakennuksen tyyppi","Huone_lkm","Neliöhinta","Hoitovastike"]]
    df1 = df1.rename(columns={"Huone_lkm":"Huoneita"})  
    df1["Huoneita"] = df1['Huoneita'].astype(str) 
    df["Huoneita"] = df['Huoneita'].astype(str) 
    df1["Kaupunginosa"] = df1['Kaupunginosa'].astype(str) 
    df["Kaupunginosa"] = df['Kaupunginosa'].astype(str)    
    print("this program shows the correlation between housing prices per square meter and rental prices using scatterplot")    
    #rooms = input("Number of rooms\n")
    

    common_df,vuokra_count,myytävät_count = create_price_rent_df(df,df1)
    plot_comparison(common_df,vuokra_count,myytävät_count,1)

 

if __name__ == "__main__":
    main()
    
