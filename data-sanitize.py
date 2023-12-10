import pandas as pd

def monthly_average_data(file1, file2, file3, file4, start_date=None, end_date=None):
    data1 = pd.read_csv(file1)#HDD Data
    data2 = pd.read_csv(file2)#Natural Gas Data
    data3 = pd.read_csv(file3)#Natural Gas Futures Data
    data4 = pd.read_csv(file4)#Natural Gas Consumption Data
    
    # Parse dates from various formats DataFrames
    data1 = data1[~data1['Date'].astype(str).str.endswith('13')]#HDD has Month 13's which are yearly averages that need to be removed
    data1['Date'] = data1['Date'].astype(str).str[-2:] + '/' + data1['Date'].astype(str).str[:-2]
    data1['Date'] = pd.to_datetime(data1['Date'], format="%m/%Y")
    data2['Date'] = pd.to_datetime(data2['Date'], format="%m/%d/%Y")
    data3['Date'] = pd.to_datetime(data3['Date'], format='%m/%d/%Y')
    data4['Date'] = pd.to_datetime(data3['Date'], format='%M-%Y')
        
    # Extract month and year separately for grouping
    data1['Month'] = data1['Date'].dt.to_period('M')
    data2['Month'] = data2['Date'].dt.to_period('M')
    data3['Month'] = data3['Date'].dt.to_period('M')
    data4['Month'] = data3['Date'].dt.to_period('M')
    

    monthly_data1 = data1.groupby('Month')['Value'].sum().reset_index()
    
    monthly_data2 = data2.groupby('Month')['Value'].mean().reset_index()#Need to average daily data for Natural Gas Price on the month
    
    monthly_data3 = data3.groupby('Month')['Value'].sum().reset_index()

    monthly_data4 = data4.groupby('Month')['Value'].sum().reset_index()

    # Merge the two monthly data sets on 'Month'
    result = pd.merge(monthly_data1, monthly_data2, on='Month')
    result3 = pd.merge(result, monthly_data3, on='Month')
    result3.rename(columns={'Value_x': 'Data1','Value_y': 'Data2','Value':'Data3'},inplace=True, errors='raise')
    result4 = pd.merge(monthly_data1, monthly_data4, on='Month')
    result4.rename(columns={'Value_x': 'Data1','Value_y': 'Data2'},inplace=True, errors='raise')#Just HDD and Counsumption
    
    return result3,result4

output,consumption = monthly_average_data('data/HDD.csv', 'data/NaturalGasPrice.csv','data/OilFuture.csv', 'data/NaturalGasConsumption.csv', start_date='01/1997', end_date='08/2020')

# Save the output to new CSV files
output.to_csv('output.csv', index=False)
consumption.to_csv('consumption.csv', index=False)
