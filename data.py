import pandas as pd
import glob
import os



startEndStamps = {}

def check_data_chronologically_sorted(file):
    df = open_data(file)
    return df.timestamp.is_monotonic_increasing

def check_all_files():
    folder_path = 'raw'
    file_extension = '*.csv'

    # Iterate over all .txt files in the folder
    for filename in glob.glob(os.path.join(folder_path, file_extension)):
        check_data_chronologically_sorted(filename)


def open_data(file):
    print("start loading file:", file)
    df = pd.read_csv(os.path.join("cleaned",file))
    print("start formating data")
    # pour faire ca besoin de la 
    df.timestamp = pd.to_datetime(df.timestamp)
    print("loaded file :", file)
    return df

def clean_data(file : str):
    print(f"cleaning file {file}")
    file_path = os.path.split(file)
    if file_path[0] != "raw" :
        raise ValueError(" file to be cleaned are only in the raw folder")
    df = pd.read_csv(file)
    df.timestamp.replace(":(..) UTC", r":\1.000 UTC", regex=True, inplace=True)
    df.to_csv("cleaned/" + file_path[1])
    startEndStamps[file_path[1]] = df.timestamp.iloc[0],df.timestamp.iloc[-1]


def clean_all_files():
    folder_path = 'raw'
    file_extension = '*.csv'
    
    # Iterate over all .txt files in the folder
    for filename in glob.glob(os.path.join(folder_path, file_extension)):
        clean_data(filename)
    #save startStamps in a txt file
    with open("startEndStamps.txt", "w") as f:
        for key in startEndStamps.keys():
            start,end = startEndStamps[key]
            f.write(f"{key};{start};{end}\n")

def get_startStamps(reverse = False):
    stamps = []
    with open("startEndStamps.txt", "r") as f:
        for line in f:
            key, start, end = line.split(";")
            end = end.removesuffix("\n")
            stamps.append((pd.Timestamp(start), pd.Timestamp(end) , key))
    return stamps
    
