
# Import the necessary libraries
import pandas as pd
from data import clean_data

# Read the CSV file into a pandas dataframe
df = pd.read_csv('raw/2023_place_canvas_history-000000000000.csv')

# Print the first row of the dataframe
print(df.head(1))

# Print the shape of the dataframe
print(df.shape)

# Print the data types of the columns in the dataframe
print(df.dtypes)

# Print the summary statistics of the dataframe
# (count values, unique values, top values, and frequency of top values)
print(df.describe())

# save the 10 first lines of the dataframe in a new csv file
#df.head(10).to_csv('sample.csv', index=False)
print(df["pixel_color"].unique())
print(df.iloc[0].timestamp)
print(type(df.iloc[0].timestamp))

clean_data("2023_place_canvas_history-000000000000.csv")