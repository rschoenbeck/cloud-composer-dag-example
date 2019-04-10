import pandas as pd

file_name = '/files/data.csv'

data = pd.read_csv(file_name)
price_string = data['PRICE'].astype(str)
price_string = price_string[price_string.str.isnumeric() == True]
total_price = price_string.astype(float).sum()
print("Total price: " + str(total_price))