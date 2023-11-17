import pandas as pd
import glob
import os
from pathlib import Path
import gzip
import zipfile

from kaggle.api.kaggle_api_extended import KaggleApi

# # Specify the file(s) you want to download
# files_to_download = ['file1.csv', 'file2.txt']  # Replace with the actual file names

# # Specify the directory to save the downloaded files
# download_path = './downloaded_data'

# # Download each specified file individually
# for file_name in files_to_download:
#     api.dataset_download_file(dataset_slug, file_name, path=download_path, unzip=True)

def unzip_file(zip_file_path: str):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(zip_file_path))
    os.remove(zip_file_path)
    

def get_on_disk_file_name(kaggle_file_info):
    return str(kaggle_file_info)

class StartStamps():
    def  __init__(self, dataIterator) -> None:
        self.data = dataIterator
        self._startStamps = dataIterator.init_startStamps()
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, index : int):
        index = index if index >= 0 else len(self) + index
        self.data.make_usable(index)
        return self._startStamps[index]
        
    def __setitem__(self, index, value):
        self._startStamps[index] = value

    def __delitem__(self, key):
        raise TypeError("Cannot delete items in DataIteratorDownload")
    


class DataIteratorDownload():
    def __init__(self, dataset, raw_data_folder = "raw/",cleaned_data_folder = "cleaned/", startEndStampsFile= "startEndStamps.txt"):
        # Instantiate the Kaggle API
        self.api = KaggleApi()
        self.raw_data_folder = raw_data_folder
        self.cleaned_data_folder = cleaned_data_folder
        self.startEndStampsFile = startEndStampsFile
        # Authenticate with your Kaggle API credentials
        self.api.authenticate()
        self.dataset = dataset
        self.files = sorted([get_on_disk_file_name(file) for file in self.api.dataset_list_files(self.dataset).files])[:3]
        self.startStamps = StartStamps(self)



    def __iter__(self):
        return ListLikeIterator(self)

    def make_usable(self, index : int):
        if index >= len(self.files):
            raise IndexError("Index out of range.")
        file = self.files[index]

        if not Path(self.raw_data_folder + file).exists():
            self.api.dataset_download_file(self.dataset, file, path=self.raw_data_folder)
            unzip_file(self.raw_data_folder + file + ".zip")

        if not Path(self.cleaned_data_folder + file).exists():
            self.clean_data(index)
    
    def __getitem__(self, index : int):
        index = index if index >= 0 else len(self) + index
        self.make_usable(index)
        return self.open_data(index)
        
    def open_data(self, index: int):
        file = self.files[index]
        print("start loading file:", file)
        df = pd.read_csv(gzip.open(os.path.join(self.cleaned_data_folder,file)))
        print("start formating data")
        # pour faire ca besoin de la 
        df.timestamp = pd.to_datetime(df.timestamp)
        print("loaded file :", file)
        return df

    def __setitem__(self, index, value):
        raise TypeError("Cannot set items in DataIteratorDownload")

    def __delitem__(self, key):
        raise TypeError("Cannot delete items in DataIteratorDownload")

    def __len__(self):
        return len(self.files)

    def check_data_chronologically_sorted(self,file):
        df = self.open_data(file)
        return df.timestamp.is_monotonic_increasing

    def clean_data(self,idx : int):
        file = self.files[idx]
        print(f"cleaning file {file}")
        if not Path(self.raw_data_folder + file):
            raise ValueError(f"The file {file} is not present in the raw folder")
        df = pd.read_csv(gzip.open(self.raw_data_folder + file))
        df.timestamp.replace(":(..) UTC", r":\1.000 UTC", regex=True, inplace=True)
        df.to_csv(self.cleaned_data_folder + file, index=False, compression='gzip')
        start,end = df.timestamp.iloc[0],df.timestamp.iloc[-1]
        self.startStamps[idx] = (pd.Timestamp(start),pd.Timestamp(end))
        #save startStamps in a txt file
        with open(self.startEndStampsFile, "a") as f:
            f.write(f"{idx};{start};{end}\n")
    
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
            self.clean_data(filename)
    
    def init_startStamps(self,reverse = False):
        stamps = {}
        with open(self.startEndStampsFile, "r") as f:
            for line in f:
                index, start, end = line.split(";")
                end = end.removesuffix("\n")
                stamps[int(index)] = (pd.Timestamp(start), pd.Timestamp(end))
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
    












    
