import pandas as pd
import glob
import os

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

def string_to_color(s : str):
    return colors[s]

def open_data(file):
    df = pd.read_csv(os.path.join("cleaned",file))
    # pour faire ca besoin de la 
    df["timestamp"] = pd.to_datetime(df.timestamp, format = '%Y-%m-%d %H:%M:%S.%f %Z')
    return df

def clean_data(file : str):
    file_path = os.path.split(file)
    print(file_path)
    if file_path[0] != "raw" :
        raise ValueError(" file to be cleaned are only in the raw folder")
    df = pd.read_csv(file)
    df.timestamp.replace(":(..) UTC", ":$1.000 UTC", regex=True, inplace=True)
    df.pixel_color.replace(colors, inplace=True)
    df.to_csv("cleaned/" + file_path[1])


def clean_all_files():
    folder_path = 'raw'
    file_extension = '*.csv'

    # Iterate over all .txt files in the folder
    for filename in glob.glob(os.path.join(folder_path, file_extension)):
        print(filename)
        clean_data(filename)


clean_all_files()