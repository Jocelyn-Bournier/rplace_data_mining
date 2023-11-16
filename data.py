import pandas as pd
import glob
import os
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

# # Specify the file(s) you want to download
# files_to_download = ['file1.csv', 'file2.txt']  # Replace with the actual file names

# # Specify the directory to save the downloaded files
# download_path = './downloaded_data'

# # Download each specified file individually
# for file_name in files_to_download:
#     api.dataset_download_file(dataset_slug, file_name, path=download_path, unzip=True)


def DataIteratorDownload():

    def __init__(self, dataset, raw_data_folder = "raw/",cleaned_data_folder = "cleaned/", startEndStampsFile= "startEndStamps.txt"):
        # Instantiate the Kaggle API
        self.api = KaggleApi()
        self.raw_data_folder = raw_data_folder
        self.cleaned_data_folder = cleaned_data_folder
        self.startEndStampsFile = startEndStampsFile
        # Authenticate with your Kaggle API credentials
        self.api.authenticate()
        self.dataset = dataset
        self.files = self.api.dataset_view(self.dataset)["files"]

    def __iter__(self):
        return ListLikeIterator(self)
    
    def __getitem__(self, index : int):
        file = self.files[index]
        if not Path(self.raw_data_folder + file).exists:
            self.api.dataset_download_file(self.dataset, file, path=self.raw_data_folder, unzip=True)
        
        if not Path(self.cleaned_data_folder + file).exists:
            clean_data(file)
        

        


    def __setitem__(self, index, value):
        raise TypeError("Cannot set items in DataIteratorDownload")

    def __delitem__(self, key):
        raise TypeError("Cannot delete items in DataIteratorDownload")

    def __len__(self):
        return len(self.files)

    def check_data_chronologically_sorted(self,file):
        df = self.open_data(file)
        return df.timestamp.is_monotonic_increasing
    
    def open_data(self,file):
        print("start loading file:", file)
        df = pd.read_csv(os.path.join("cleaned",file))
        print("start formating data")
        # pour faire ca besoin de la 
        df.timestamp = pd.to_datetime(df.timestamp)
        print("loaded file :", file)
        return df

    def clean_data(self,file : str):
        print(f"cleaning file {file}")
        file_path = os.path.split(file)
        if file_path[0] != "raw" :
            raise ValueError(" file to be cleaned are only in the raw folder")
        df = pd.read_csv(file)
        df.timestamp.replace(":(..) UTC", r":\1.000 UTC", regex=True, inplace=True)
        df.to_csv("cleaned/" + file_path[1])
        #save startStamps in a txt file
        with open(self.startEndStampsFile, "a") as f:
            start,end = df.timestamp.iloc[0],df.timestamp.iloc[-1]
            f.write(f"{file};{start};{end}\n")
    
    def check_all_files_downloaded(self):
        folder_path = 'raw'
        file_extension = '*.csv'

        # Iterate over all .txt files in the folder
        for filename in glob.glob(os.path.join(folder_path, file_extension)):
            self.check_data_chronologically_sorted(filename)
    
    def clear_cleaned_files(self):
        # TODO
        pass

    def clean_all_files(self):
        folder_path = 'raw'
        file_extension = '*.csv'
        
        # Iterate over all .txt files in the folder
        for filename in glob.glob(os.path.join(folder_path, file_extension)):
            clean_data(filename)
    
    def get_startStamps(self,reverse = False):
        stamps = []
        with open("startEndStamps.txt", "r") as f:
            for line in f:
                key, start, end = line.split(";")
                end = end.removesuffix("\n")
                stamps.append((pd.Timestamp(start), pd.Timestamp(end) , key))
        return stamps




class ListLikeIterator:
    def __init__(self, items):
        self.items = items
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.items):
            result = self.items[self.index]
            self.index += 1
            return result
        else:
            raise StopIteration
    












    
