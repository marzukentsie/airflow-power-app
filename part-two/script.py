import pandas as pd # Import the Pandas library

df = pd.read_csv('YelloTaxiData.csv') # Read the data from the CSV file

print(df.head()) # Print the first 5 rows of the data to verify that it was read correctly

# Save the first 100 rows of the data to a new CSV file

df.head(100).to_csv('YellowTaxiData.csv', index=False) # Save the first 100 rows of the data to a new CSV file without including the index column

newDF = pd.read_csv('YellowTaxiData.csv') # Read the new CSV file

print(newDF.head()) # Print the first 5 rows of the new data to verify that it was saved correctly