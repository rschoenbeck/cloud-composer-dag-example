import pandas as pd

file_name = '/files/data.csv'

data = pd.read_csv(file_name)
total_price = data['PRICE'].astype(float).sum()
print("Total price: " + str(total_price))