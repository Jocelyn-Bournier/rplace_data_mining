
import numpy as np
import matplotlib.pyplot as plt
import copy
from imageio import mimsave
from IPython.display import display
from inspect import cleandoc

from data import DataIteratorDownload

data = DataIteratorDownload("ricobruland/reddit-2023-rplace-pixel-data")

colors = {
    '#FFFFFF' : [255,255,255],
    '#FF4500' : [255,69,0],
    '#B44AC0' : [180,74,192],
    '#3690EA' : [54,144,234],
    '#00A368' : [0,163,104],
    '#000000' : [0,0,0], 
    '#FFD635' : [255,214,53],
    '#FFA800' : [255,168,0],
}

class printer(str):
	def __repr__(self):
		return cleandoc(self)



def string_to_color(s : str):
    return colors[s]


def string_to_coordinates(s : str):
    l = s.split(",")
    match len(l) :
        case 2:
            return [(int(l[1])+500, int(l[0])+500)]
        case 3:
            for i in range(3):
                l[i] = l[i].removeprefix("{")
                l[i] = l[i].removesuffix("}")
                l[i] = l[i].strip()
                l[i] = l[i].split(":")[1]

            center = (int(l[1])+500, int(l[0])+500)
            radius = int(l[2])

            #generate all points from the circle
            coords = []
            for x in range(center[0] - radius, center[0] + radius):
                for y in range(center[1] - radius, center[1] + radius):
                    if (x - center[0])**2 + (y - center[1])**2 <= radius**2:
                        coords.append((x,y))
            
            return coords
        
        case 4:
            
            left_up_corner = (int(l[1])+500, int(l[0])+500)
            right_down_corner = (int(l[3])+500, int(l[2])+500)

            #generate all points from the rectangle
            coords = []
            for x in range(left_up_corner[0], right_down_corner[0]):
                for y in range(left_up_corner[1], right_down_corner[1]):
                    coords.append((x,y))
            
            return coords


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

def convertImage(image,transform):
    #convert the image to RGB
    rezImage = np.zeros((len(image),len(image[0]),3), dtype=np.uint8)
    for i in range(len(image)):
        for j in range(len(image[i])):
            rezImage[i][j] = transform(image[i][j])
    return rezImage


def visualise(image, transform):
    #visualise with plt a matrix of 1000x1000 with colors for each coordinate
    image = convertImage(image,transform)
    plt.imshow(image)
    plt.show()

def summary(startingTimeStamp, EndingTimeStamp, startingData, df, summary_function):
    start_index = binary_search(df.timestamp, startingTimeStamp)
    end_index = binary_search(df.timestamp, EndingTimeStamp)
    df = df.iloc[start_index:end_index]
    rezData = copy.deepcopy(startingData)
    for row in df.itertuples() :
        l = string_to_coordinates(row.coordinate)
        is_mod = len(l) > 1
        for coord in l :
            x,y = coord
            rezData[x][y] = summary_function(rezData[x][y],row.pixel_color, row.user, is_mod)
    return rezData

def visualise_at_interval(summary_function, transforms, interval, startingState, rezfilename, startingTimeStamp = None, EndingTimeStamp=None, duration = None):

    i = 0
    dh = display(printer("\r"), display_id=True)
    opened_data = {}
    files_and_timestamp = data.startStamps

    if startingTimeStamp and startingTimeStamp > files_and_timestamp[0][0]:
        EndingTimeStamps = [x[1] for x in files_and_timestamp]
        idx = binary_search(EndingTimeStamps, startingTimeStamp)
        currentTimeStamp = startingTimeStamp
    else :
        idx = 0
        currentTimeStamp = files_and_timestamp[0][0]
    if not EndingTimeStamp : 
        EndingTimeStamp = files_and_timestamp[-1][1]
    nextTimeStamp = currentTimeStamp + (duration if duration else interval)

    _, nextFileTimeStamp = files_and_timestamp[idx]
    df = data[idx]
    if duration :
        opened_data[idx] = df

    currentState = copy.deepcopy(startingState)
    rez= [startingState]
    start_idx = idx

    while currentTimeStamp < EndingTimeStamp :
        # make the summary
        currentState = summary(currentTimeStamp, nextTimeStamp, currentState, df, summary_function)
        if nextTimeStamp > nextFileTimeStamp and idx + 1 < len(files_and_timestamp) : 
            idx+=1
            _, nextFileTimeStamp = files_and_timestamp[idx]
            if idx in opened_data :
                df = opened_data[idx]
            else :
                df = data[idx]
                if duration :
                    opened_data[idx] = df
        else : 
            if nextTimeStamp > nextFileTimeStamp and duration:
                try :
                    if (nextTimeStamp - nextFileTimeStamp) / duration > 0.5 :
                        break
                    currentState *= 1 - (nextTimeStamp - nextFileTimeStamp) / duration
                except Exception as e:
                    # Catch any exception without specifying a specific type.
                    print(f"An error occurred: {e}")
            # save the current state 
            rez.append(currentState)

            i+= 1
            if duration : 
                currentState = copy.deepcopy(startingState)
                dh.update(printer(
                    f"""
                        just made summary from {currentTimeStamp} to {nextTimeStamp}
                        epochs : i
                    """
                ))
            else :
                dh.update(printer(f"just made summary until {nextTimeStamp}"))



            # go to the next interval
            currentTimeStamp += interval
            nextTimeStamp +=interval
            if duration:
                _, nextFileTimeStamp = files_and_timestamp[start_idx]
                while nextFileTimeStamp < currentTimeStamp and idx + 1 < len(data) :
                    start_idx += 1
                    del opened_data[start_idx]
                    _, nextFileTimeStamp = files_and_timestamp[start_idx]
                
                idx = start_idx
                if idx in opened_data :
                    df = opened_data[idx]
                else :
                    df = data[idx]
                    opened_data[idx] = df
                


    try :
        for name, transform in transforms.items() :
            # make visualisation
            transformed_rez = transform(rez)
            # save it in a gif 
            mimsave(f"visualisation/{rezfilename(name)}.gif", transformed_rez, duration= 3, loop= 0)
            print(f"results saved on {rezfilename(name)}.gif")
    except Exception as e:

        # make visualisation
        transformed_rez = transforms(rez)
        # save it in a gif 
        mimsave(f"visualisation/{rezfilename}.gif", transformed_rez, duration= 3, loop= 0)
        print(f"results saved on {rezfilename}.gif")

