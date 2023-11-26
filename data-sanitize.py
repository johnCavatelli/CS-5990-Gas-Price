import pandas as pd

def monthly_average_data(file1, file2, start_date=None, end_date=None):
    # Read Data1.csv and Data2.csv
    data1 = pd.read_csv(file1)
    data2 = pd.read_csv(file2)
    
    # Parse dates in both DataFrames
    data1 = data1[~data1['Date'].astype(str).str.endswith('13')]
    data1['Date'] = data1['Date'].astype(str).str[-2:] + '/' + data1['Date'].astype(str).str[:-2]
    data1['Date'] = pd.to_datetime(data1['Date'], format="%m/%Y")
    data2['Date'] = pd.to_datetime(data2['Date'], format="%m/%d/%Y")
    # data1['Date'] = data1['Date'].str.replace('`', '')
    
    # Filter data within date range
    if start_date:
        start_date = pd.to_datetime(start_date, format="%m/%Y")
        data2 = data2[data2['Date'] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date, format="%m/%Y")
        data2 = data2[data2['Date'] <= end_date]
    
    # Extract month and year separately for grouping
    data1['Month'] = data1['Date'].dt.to_period('M')
    data2['Month'] = data2['Date'].dt.to_period('M')
    
    # Group Data1 by month and calculate monthly data
    monthly_data1 = data1.groupby('Month')['Value'].sum().reset_index()
    
    # Group Data2 by month and calculate monthly average of Column2
    monthly_data2 = data2.groupby('Month')['Value'].mean().reset_index()
    
    # Merge the two monthly data sets on 'Month'
    result = pd.merge(monthly_data1, monthly_data2, on='Month', suffixes=('_Data1', '_Data2'))
    
    return result

# Replace 'Data1.csv' and 'Data2.csv' with your file names
output = monthly_average_data('data/HDD.csv', 'data/NaturalGasPrice.csv', start_date='01/1997', end_date='08/2020')

# Save the output to a new CSV file
output.to_csv('output.csv', index=False)
