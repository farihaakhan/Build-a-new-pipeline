-- Write your SQL query here
SELECT * FROM grocery_sales

import pandas as pd
import os

# Extract function is already implemented for you 
def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on = "index")
    return merged_df

# Call the extract() function and store it as the "merged_df" variable
merged_df = extract(grocery_sales, "extra_data.parquet")
merged_df

# Create the transform() function with one parameter: "raw_data"
def transform(raw_data):
  #Using Fillna with mean to fill the missing numirical values
    raw_data.fillna(
        {
            'CPI' : raw_data['CPI'].mean(),
            'Weekly_Sales': raw_data['Weekly_Sales'].mean(),
            'Unemployment': raw_data['Unemployment'].mean(),
          }, inplace = True
    )
    #Add Month Column
    #Define the Date formate to add Month column
    raw_data["Date"] = pd.to_datetime(raw_data["Date"], format = "%Y-%m-%d")
    #Extract the Month value from the "Date" column
    raw_data["Month"] = raw_data["Date"].dt.month

    #Filter the WeeklySales rows to only have sales over 10000
    #Filter the dataframe
    raw_data = raw_data.loc[(raw_data['Weekly_Sales'] > 10000)]
    #Drop the unnecessary columns
    raw_data = raw_data.drop(["index", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "Type", "Size", "Date"], axis = 1)
    return raw_data

# Call the transform() function and pass the merged DataFrame
clean_data = transform(merged_df)

# Create the avg_weekly_sales_per_month function that takes in the cleaned data from the last step
def avg_weekly_sales_per_month(clean_data):
#Select only the columns needed
    holiday_sale = clean_data[['Month','Weekly_Sales']]
    holiday_sale = (holiday_sale.groupby('Month').agg(Avg_Sales = ('Weekly_Sales','mean')).reset_index().round(2))
    return holiday_sale

# Call the avg_weekly_sales_per_month() function and pass the cleaned DataFrame
agg_data = avg_weekly_sales_per_month(clean_data)

# Create the load() function that takes in the cleaned DataFrame and the aggregated one with the paths where they are going to be stored
def load(clear_data, clear_data_file_path, agg_data, agg_data_file_path):
#Save the dataframes as csv files, without the index
    clear_data.to_csv(clear_data_file_path, index = False)
    agg_data.to_csv(agg_data_file_path, index = False)

# Call the load() function and pass the cleaned and aggregated DataFrames with their paths
load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")

# Create the validation() function with one parameter: file_path - to check whether the previous function was correctly executed
def validation(file_path):
#Use the "os" package to check whether a path exists
    file_exists = os.path.exists(file_path)
    #An exception if the path doesn't exist, hence, if there is no file found on a given path
    if not file_exists:
        raise Exception(f"There is no file at the path {file_path}") 

# Call the validation() function and pass first, the cleaned DataFrame path, and then the aggregated DataFrame path
validation("clean_data.csv")
validation("agg_data.csv")
