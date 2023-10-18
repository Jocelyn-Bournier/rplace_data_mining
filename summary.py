import pandas as pd
import numpy as np
from data import open_data

def check_data_chronologically_sorted(file):
    df = open_data(file)
    return df['timestamp'].is_monotonic_increasing

def string_to_coordinates(s : str):
    l = s.split(",")
    match len(l) :
        case 2:
            return [(int(l[0]), int(l[1]))]
        case 3:
            #renvoyer les coords contenu dans le circle
            pass
        case 4:
            #renvoyer les coords contenu dans le rect
            pass


def binary_search(arr, target):
    """
        get the first element in arr that is superior or equal to target
    """
    left, right = 0, len(arr) - 1

    while left < right:
        mid = (left + right) // 2
        print(left,mid,right)

        if arr[mid] == target:
            return mid  # Element found, return its index
        elif arr[mid] < target:
            left = mid + 1  # Adjust the search range to the right half
        else:
            right = mid  # Adjust the search range to the left half


    return left  # Element not found

print(binary_search([1,2,3], 1.5))

def summary(startingTimeStamp, EndingTimeStamp, startingData : np.ndarray, df, summary_function):
    start_index = binary_search(df.timestamp, startingTimeStamp)
    end_index = binary_search(df.timestamp, EndingTimeStamp)
    df = df.iloc[start_index:end_index]

    for row in df.itertuples() :
            l = string_to_coordinates(row.coordinate)
            is_mod = len(l) > 1
            for coord in l :
                x,y = coord
                startingData[x,y] = summary_function(startingData[x,y],row.pixel_color)
    
start = np.ndarray((500,500), dtype=np.int32)
summary(pd.Timestamp("2023-07-20 13:00:26.088 UTC"), pd.Timestamp("2023-07-20 16:00:26.088 UTC"),start, "raw/2023_place_canvas_history-000000000000.csv", lambda _,y : y)
# print(start)
# print(check_data_chronologically_sorted("raw/2023_place_canvas_history-000000000000.csv"))
