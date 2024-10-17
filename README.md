# Building an E-Commerce Data Pipeline

This project focuses on building a data pipeline to analyze Walmart's e-commerce supply and demand patterns around public holidays.

## Project Overview
In this project, the goal was to create a data pipeline for the analysis of supply and demand around the holidays, along with conducting a preliminary analysis of the data. Worked with two data sources: grocery sales and complementary data. 

## About Data
The `grocery_sales` was imported table in the PostgreSQL database with the following features:

**grocery_sales**
- `"index"` - unique ID of the row
- `"Store_ID"` - the store number
- `"Date"` - the week of sales
- `"Weekly_Sales"` - sales for the given store

Also, the `extra_data.parquet` file that contains complementary data:

**extra_data.parquet**
- `"IsHoliday"` - Whether the week contains a public holiday - 1 if yes, 0 if no.
- `"Temperature"` - Temperature on the day of sale
- `"Fuel_Price"` - Cost of fuel in the region
- `"CPI"` – Prevailing consumer price index
- `"Unemployment"` - The prevailing unemployment rate
- `"MarkDown1"`, `"MarkDown2"`, `"MarkDown3"`, `"MarkDown4"` - number of promotional markdowns
- `"Dept"` - Department Number in each store
- `"Size"` - the size of the store
- `"Type"` - type of the store (depends on `Size` column)
- 
### Key Steps:

1. **Data Extraction**:
   - Extracted sales data from the `grocery_sales` table in PostgreSQL.
   - Loaded complementary data from a `parquet` file.

2. **Data Transformation**:
   - Merged the files on index.
   - Filled missing values with mean value,
   - Added a "Month" column from the Date using `dt.month`,
   - Filtered sales over $10,000 using groupby and agg(),
   - Dropped unnecessary columns to create a cleaned dataset.
   - Key columns in the transformed data: `Store_ID`, `Month`, `Dept`, `IsHoliday`, `Weekly_Sales`, `CPI`, and `Unemployment`.

3. **Data Aggregation**:
   - Calculated the average weekly sales per month and stored the results.

4. **Data Loading**:
   - Exported the cleaned data and aggregated sales data to CSV files.

## Key Features

- **Technologies**: PostgreSQL, pandas
- **Analysis**: Monthly aggregation of weekly sales, focusing on public holiday effects.
