import pandas as pd
import glob
import os
from pathlib import Path
import gzip
import zipfile
import numpy as np

from IPython.display import clear_output
from inspect import cleandoc

from kaggle.api.kaggle_api_extended import KaggleApi




cached_log = {}
internal_separator = "\n"
intra_logger_separator = "\n\n"
    

class printer(str):
    def __repr__(self):
        return cleandoc(self)

def update_output():
    clear_output(True)
    print(intra_logger_separator.join(cached_log.values()))

def display(obj, display_id, internal_separator = "\n"):
    global cached_log
    if display_id in cached_log:
        cached_log[display_id] += internal_separator + str(obj)
    else : 
        cached_log[display_id] = str(obj)
    cached_log = {key:cached_log[key] for key in sorted(cached_log)}
    update_output()

def update_display(obj, display_id):
    clear_display(display_id, wait = True)
    display(obj,display_id)

def clear_display(display_id, wait = False):
    if display_id in cached_log :
        del cached_log[display_id] 
        if not wait :
            update_output(wait)
        


class Logger:

    def __init__(self, display_id : int | None = None, internal_separator = "\n") -> None:
        if display_id is None:
            display_id = max(cached_log.keys())
        self.display_id = display_id
        self.internal_separator = internal_separator
    
    def display(self, obj):
        display(obj, self.display_id, self.internal_separator)
    
    def update(self, obj):
        update_display(obj, self.display_id)
    
    def clear(self, wait = False):
        clear_display(self.display_id, wait)






def unzip_file(zip_file_path: str):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(zip_file_path))
    os.remove(zip_file_path)


def get_on_disk_file_name(kaggle_file_info):
    return str(kaggle_file_info)


class StartStamps():
    def __init__(self, dataIterator) -> None:
        self.data = dataIterator
        self._startStamps = dataIterator.init_startStamps()

    def __len__(self):
        return len(self.data)

    def __getitem__(self, index: int):
        index = index if index >= 0 else len(self) + index
        self.data.make_usable(index)
        return self._startStamps[index]

    def __setitem__(self, index, value):
        self._startStamps[index] = value

    def __delitem__(self, key):
        raise TypeError("Cannot delete items in DataIteratorDownload")


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


class DataIteratorDownload():
    def __init__(self, dataset, raw_data_folder="raw/", cleaned_data_folder="cleaned/", startEndStampsFile="startEndStamps.txt", datasetStartTimeStampFile="datasetStartTimeStamp.txt", manually_downloaded=False):
        # Instantiate the Kaggle API
        self.api = KaggleApi()
        self.raw_data_folder = raw_data_folder
        self.cleaned_data_folder = cleaned_data_folder
        self.startEndStampsFile = startEndStampsFile
        # Authenticate with your Kaggle API credentials
        self.api.authenticate()
        self.dataset = dataset
        self.files = sorted(
            [str(file) for file in self.api.dataset_list_files(self.dataset).files])
        if not manually_downloaded:
            self.files = self.files[:20]
        self.startStamps = StartStamps(self)
        self.cache = {}
        self.datasetStartTimeStampFile = datasetStartTimeStampFile
        self.dh = Logger(display_id=0)

        try:
            with open(self.datasetStartTimeStampFile, "r") as f:
                self.datasetStartTimeStamp = int(f.read())
        except FileNotFoundError:
            # If the file doesn't exist this means there was no data stored
            self.datasetStartTimeStamp = None

        # ensure raw and cleaned data folder are present as they are not in the git to avoid bloating
        try:
            os.makedirs(self.raw_data_folder)
            print(f"Folder '{self.raw_data_folder}' created.")
        except FileExistsError:
            pass

        try:
            os.makedirs(self.cleaned_data_folder)
            print(f"Folder '{self.cleaned_data_folder}' created.")
        except FileExistsError:
            pass

    def __iter__(self):
        return ListLikeIterator(self)

    def __setitem__(self, index, value):
        raise TypeError("Cannot set items in DataIteratorDownload")

    def __delitem__(self, index):
        del self.cache[index]

    def __len__(self):
        return len(self.files)

    def __getitem__(self, index: int):
        index = index if index >= 0 else len(self) + index
        self.make_usable(index)
        return self.open_data(index)

    def make_usable(self, index: int):
        if index >= len(self.files):
            raise IndexError("Index out of range.")
        file = self.files[index]

        if not Path(self.raw_data_folder + file).exists():
            self.api.dataset_download_file(
                self.dataset, file, path=self.raw_data_folder)
            unzip_file(self.raw_data_folder + file + ".zip")

        if not Path(self.cleaned_data_folder + file).exists():
            self.clean_data(index)

    def open_data(self, index: int):
        if index in self.cache:
            return self.cache[index]

        file = self.files[index]
        self.dh.update(printer(f"start loading file : {file}"))
        df = pd.read_csv(gzip.open(os.path.join(
            self.cleaned_data_folder, file)))  # type: ignore
        self.dh.update(printer(f"loaded file : {file}"))
        self.cache[index] = df
        return df

    def clean_data(self, idx: int):
        file = self.files[idx]
        self.dh.update(printer(f"cleaning file : {file}"))

        if not Path(self.raw_data_folder + file):
            raise ValueError(
                f"The file {file} is not present in the raw folder")

        # type: ignore
        df = pd.read_csv(gzip.open(self.raw_data_folder + file))

        df.timestamp.replace(":(..) UTC", r":\1.000 UTC",
                             regex=True, inplace=True)
        df.timestamp = pd.to_datetime(df.timestamp).astype(int) // 10**6
        if idx == 0:
            self.datasetStartTimeStamp = df.timestamp.iloc[0]
            with open(self.datasetStartTimeStampFile, "w") as f:
                f.write(str(self.datasetStartTimeStamp))
        df.timestamp = df.timestamp - self.get_datasetStartTimeStamp()

        start, end = df.timestamp.iloc[0], df.timestamp.iloc[-1]
        self.startStamps[idx] = (start, end)
        # save startStamps in a txt file
        with open(self.startEndStampsFile, "a") as f:
            f.write(f"{idx};{start};{end}\n")
        
        df.to_csv(self.cleaned_data_folder + file,
                  index=False, compression='gzip')

        self.dh.update(printer(f"file cleaned : {file}"))



    def get_datasetStartTimeStamp(self) -> int:
        if self.datasetStartTimeStamp is None:
            self.make_usable(0)
        if self.datasetStartTimeStamp is None:
            raise RuntimeError(
                "make_usable forgot to set datasetStartTimeStamp when given index 0")

        return self.datasetStartTimeStamp

    def clear_cleaned_files(self):
        # List all files in the folder
        files = os.listdir(self.cleaned_data_folder)

        # Iterate through the files and delete them
        for file in files:
            file_path = os.path.join(self.cleaned_data_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

        if os.path.isfile(self.startEndStampsFile):
            os.remove(self.startEndStampsFile)

        if os.path.isfile(self.datasetStartTimeStampFile):
            os.remove(self.datasetStartTimeStampFile)

        print(f"All files of cleaned data have been deleted.")

        pass

    def init_startStamps(self, reverse=False):
        stamps = {}

        try:
            # Try to open the file for reading
            with open(self.startEndStampsFile, "r") as f:
                for line in f:
                    index, start, end = line.split(";")
                    end = end.removesuffix("\n")
                    stamps[int(index)] = (int(start), int(end))
        except FileNotFoundError:
            # If the file doesn't exist this means there was no data stored
            pass
        return stamps

    def check_all_files_downloaded(self):
        folder_path = 'raw'
        file_extension = '*.csv'

        # Iterate over all .txt files in the folder
        for filename in glob.glob(os.path.join(folder_path, file_extension)):
            self.check_data_chronologically_sorted(filename)

    def check_data_chronologically_sorted(self, file):
        df = self.open_data(file)
        return df.timestamp.is_monotonic_increasing
