import pandas as pd
import numpy as np
colors = {
    '#FFFFFF' : 0,
    '#FF4500' : 1,
    '#B44AC0' : 2,
    '#3690EA' : 3,
    '#00A368' : 4,
    '#000000' : 5, 
    '#FFD635' : 6,
    '#FFA800' : 7,
}

def open_data(file):
    df = pd.read_csv(file)
    # attention ici j'ai du aller modifier le csv parceque certain on pas de miliseconde 
    # ajouté je savais pas comment indiqué le double format
    df["timestamp"] = pd.to_datetime(df.timestamp, format = '%Y-%m-%d %H:%M:%S.%f %Z')
    return df

def string_to_color(s : str):
    return colors[s]

def check_data_chronologically_sorted(file):
    df = open_data(file)
    return df['timestamp'].is_monotonic_increasing

def string_to_coordinate(s : str):
    l = s.split(",",2)
    return int(l[0]), int(l[1])

def summary(startingTimeStamp, EndingTimeStamp, startingData : np.ndarray, file, summary_function):

    df = open_data(file)
    # à faire : garder que les indices entre les timeStamp fort probablement binary search et réduction aux indices avec un iloc


    # itération sur les lignes du dataframe
    for row in df.itertuples() :
        try :
            x,y = string_to_coordinate(row.coordinate)
            startingData[x,y] = summary_function(startingData[x,y],string_to_color(row.pixel_color))
        except :
            # traiter les cas de modération ici ou en modifiant la partie au dessus 
            pass
    
start = np.ndarray((500,500), dtype=np.int32)
summary(pd.Timestamp("2023-07-20 13:00:26.088 UTC"), pd.Timestamp("2023-07-20 16:00:26.088 UTC"),start, "2023_place_canvas_history-000000000000.csv", lambda _,y : y)
print(start)
print(check_data_chronologically_sorted("2023_place_canvas_history-000000000000.csv"))