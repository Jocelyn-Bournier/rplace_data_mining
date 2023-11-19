import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import copy
from imageio import mimsave
from tqdm import tqdm
from data import DataIteratorDownload, Logger
data = None


def initialize_data_loader(**kwargs):
    global data
    data = DataIteratorDownload(
        "ricobruland/reddit-2023-rplace-pixel-data", **kwargs)
    return data


colors = {}


def string_to_color(hex_color: str):
    global colors
    if hex_color in colors:
        return colors[hex_color]
    else:
        hex_color_no_hashtag = hex_color.removeprefix('#')

        # Convert hex to RGB
        rgb = [int(hex_color_no_hashtag[i:i+2], 16) for i in range(0, 6, 2)]
        colors[hex_color] = rgb
        return rgb


def string_to_coordinates(s: str):
    def translate(coord):
        return (int(coord[1])+500, int(coord[0])+1000)

    l = s.split(",")
    match len(l):
        case 2:
            return [translate(l)]
        case 3:
            for i in range(3):
                l[i] = l[i].removeprefix("{")
                l[i] = l[i].removesuffix("}")
                l[i] = l[i].strip()
                l[i] = l[i].split(":")[1]

            center = translate(l[:2])
            radius = int(l[2])

            # generate all points from the circle
            coords = []
            for x in range(center[0] - radius, center[0] + radius):
                for y in range(center[1] - radius, center[1] + radius):
                    if (x - center[0])**2 + (y - center[1])**2 <= radius**2:
                        coords.append((x, y))

            return coords

        case 4:

            left_up_corner = center = translate(l[:2])
            right_down_corner = center = translate(l[2:])

            # generate all points from the rectangle
            coords = []
            for x in range(left_up_corner[0], right_down_corner[0]):
                for y in range(left_up_corner[1], right_down_corner[1]):
                    coords.append((x, y))

            return coords
        case _:
            raise ValueError("String did not have proper format")


def binary_search(arr, target):
    """
        get the first element in arr that is superior or equal to target
    """
    left, right = 0, len(arr) - 1

    while left < right:
        mid = (left + right) // 2
        if arr[mid] == target:
            return mid  # Element found, return its index
        elif arr[mid] < target:
            left = mid + 1  # Adjust the search range to the right half
        else:
            right = mid  # Adjust the search range to the left half
    return left


def convertImage(image, transform):
    # convert the image to RGB
    rezImage = np.zeros((len(image), len(image[0]), 3), dtype=np.uint8)
    for i in range(len(image)):
        for j in range(len(image[i])):
            rezImage[i][j] = transform(image[i][j])
    return rezImage


def visualise(image, transform):
    # visualise with plt a matrix of 1000x1000 with colors for each coordinate
    image = convertImage(image, transform)
    plt.imshow(image)
    plt.show()


def summary(startingTimeStamp, EndingTimeStamp, startingData, df, summary_function):
    start_index = binary_search(df.timestamp, startingTimeStamp)
    end_index = binary_search(df.timestamp, EndingTimeStamp)
    df["is_mod"] = False
    df = df.iloc[start_index:end_index]
    rezData = startingData
    for row in tqdm(df.itertuples(), total= end_index - start_index):
        l = string_to_coordinates(row.coordinate)
        is_mod = len(l) > 1
        for coord in l:
            x, y = coord
            try:
                rezData[x][y] = summary_function(
                    rezData[x][y], row, is_mod)
            except Exception as e:
                print(x, y)
                print(row.coordinate)
                raise e
    return rezData


TimePassed = pd.Timedelta | int


class DynamicList:
    def __init__(self, original_list, func):
        self.original_list = original_list
        self.func = func

    def __len__(self):
        return len(self.original_list)

    def __getitem__(self, index):
        return self.func(self.original_list[index])

    def __setitem__(self, index, value):
        raise PermissionError("This is a view as such you can't modify it")

    def __delitem__(self, key):
        raise PermissionError("This is a view as such you can't modify it")


def convert_to_milliseconds(value: TimePassed) -> int:
    if isinstance(value, pd.Timedelta):
        return int(value / pd.Timedelta(milliseconds=1))
    else:
        return value


def visualise_at_interval(summary_function,
                          transforms,
                          interval: TimePassed,
                          startingState,
                          rezfilename,
                          startingTimeStamp: TimePassed = 0,
                          EndingTimeStamp: TimePassed = -1,
                          duration: TimePassed = -1,
                          rez = None):
    # make sure data exists
    global data
    if not data:
        data = initialize_data_loader()

    i = 0

    # Create a display on which most thing from this function will be printed
    dh = Logger(display_id=1)
    if not dh:
        raise RuntimeError(
            "I don't how it happened but it didn't create a display")

    # Get startStamps from the data
    files_and_timestamp = data.startStamps

    # Transform Timedelta to ms
    startingTimeStamp = convert_to_milliseconds(startingTimeStamp)
    EndingTimeStamp = convert_to_milliseconds(EndingTimeStamp)
    duration = convert_to_milliseconds(duration)
    interval = convert_to_milliseconds(interval)

    # Give actual default value to EndingTimeStamp
    if EndingTimeStamp < 0:
        EndingTimeStamp = files_and_timestamp[-1][1]

    idx = binary_search(DynamicList(files_and_timestamp,
                        lambda x: x[1]), startingTimeStamp)
    if interval > 0:
        currentTimeStamp = startingTimeStamp
        nextTimeStamp = currentTimeStamp + (duration if duration > 0 else interval)
    else :
        currentTimeStamp = startingTimeStamp
        nextTimeStamp = EndingTimeStamp

    _, nextFileTimeStamp = files_and_timestamp[idx]
    df = data[idx]

    currentState = copy.deepcopy(startingState)

    if not rez :
        rez = []
    rez.append(startingState)
    start_idx = idx

    while currentTimeStamp < EndingTimeStamp:
        # make the summary
        currentState = summary(
            currentTimeStamp, nextTimeStamp, currentState, df, summary_function)
        if nextTimeStamp > nextFileTimeStamp and idx + 1 < len(files_and_timestamp):
            idx += 1
            _, nextFileTimeStamp = files_and_timestamp[idx]
            df = data[idx]
        else:
            if nextTimeStamp > nextFileTimeStamp and duration > 0:
                try:
                    if (nextTimeStamp - nextFileTimeStamp) / duration > 0.5:
                        break
                    currentState *= 1 - \
                        (nextTimeStamp - nextFileTimeStamp) / duration
                except Exception as e:
                    # Catch any exception without specifying a specific type.
                    print(f"An error occurred: {e}")
            # save the current state
            rez.append(currentState)

            i += 1
            if duration > 0:
                currentState = copy.deepcopy(startingState)
                dh.update(
                    f"""
                        just made summary from {pd.Timedelta(milliseconds=currentTimeStamp)} to {pd.Timedelta(milliseconds=nextTimeStamp)}
                        epochs : i
                    """
                )
            else:
                dh.update(f"just made summary until {pd.Timedelta(milliseconds=nextTimeStamp)}")
            if interval < 0 :
                break
            # go to the next interval
            currentTimeStamp += interval
            nextTimeStamp += interval

            _, nextFileTimeStamp = files_and_timestamp[start_idx]
            while nextFileTimeStamp < currentTimeStamp and idx + 1 < len(data):
                del data[start_idx]
                start_idx += 1
                _, nextFileTimeStamp = files_and_timestamp[start_idx]

            idx = start_idx
            df = data[idx]

    dh.update("start making visualizations")
    transform_data_to_image(transforms, rezfilename, rez, interval < 0)



def transform_data_to_image(transforms, rezfilename, rez, total : bool = False):
    if total :
        file_extension = ".png"
    else :
        file_extension = ".gif"
    
    try:
        for name, transform in transforms.items():
            # make visualisation
            transformed_rez = transform(rez)
            if total :
                transformed_rez = transformed_rez[-1]
            # save it in a gif
            mimsave(f"visualisation/{rezfilename(name) + file_extension}" ,
                    transformed_rez, duration=3, loop=0)
            print(f"results saved on {rezfilename(name) + file_extension}")
    except Exception as e: 
        # make visualisation
        transformed_rez = transforms(rez)
        if total :
            transformed_rez = transformed_rez[-1:]
        # save it in a gif
        mimsave(f"visualisation/{rezfilename + file_extension}",
                transformed_rez, duration=3, loop=0)
        print(f"results saved on {rezfilename + file_extension}")