import pandas as pd
import glob
import os


def open_data(file):
    df = pd.read_csv("cleaned/" + file)
    # pour faire ca besoin de la 
    df["timestamp"] = pd.to_datetime(df.timestamp, format = '%Y-%m-%d %H:%M:%S.%f %Z')
    return df

def clean_data(file : str):
    file_path = file.split("/", maxsplit= 2)
    if file_path[0] != "raw" :
        raise ValueError(" file to be cleaned are only in the raw folder")
    df = pd.read_csv(file)
    df['timestamp'].replace(":(..) UTC", ":$1.000 UTC", regex=True, inplace=True)
    df.to_csv("cleaned/" + file_path[1])


def clean_all_files():
    folder_path = 'raw/'
    file_extension = '*.csv'

    # Iterate over all .txt files in the folder
    for filename in glob.glob(os.path.join(folder_path, file_extension)):
        clean_data(filename)


clean_all_files()