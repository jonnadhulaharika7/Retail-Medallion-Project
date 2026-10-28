# Retail Medallion Architecture Using PySpark and Databricks

## Project Overview
This project demonstrates an end-to-end retail data engineering pipeline using PySpark, Databricks, Delta Lake, and Medallion Architecture. It transforms raw retail data into clean, reliable datasets and business-ready analytics.

## Technologies Used
- Python
- PySpark
- Databricks Free Edition
- Delta Lake
- SQL
- Unity Catalog

## Architecture
The project follows the Medallion Architecture pattern:

- **Bronze Layer:** Stores ingested source datasets in Delta tables.
- **Silver Layer:** Cleans data, removes duplicate records, standardizes columns, and performs basic data-quality checks.
- **Gold Layer:** Creates aggregated datasets for business reporting and analysis.

## Datasets
The project uses synthetic retail datasets:

| Dataset | Records |
|---|---:|
| Customers | 10,000 |
| Products | 1,000 |
| Stores | 100 |
| Sales | 100,000 |

## Slowly Changing Dimensions (SCD)
This project demonstrates three approaches to managing changes in dimension data:

- **SCD Type 1:** Updates existing values without preserving previous values.
- **SCD Type 2:** Preserves historical records using effective dates and a current-record flag.
- **SCD Type 3:** Maintains the current value and the previous value in separate columns.

## Gold Analytics Tables
The project produces four business-oriented summary tables:

1. `customer_sales_summary` — sales and order metrics by customer.
2. `product_sales_summary` — sales and order metrics by product.
3. `store_sales_summary` — sales and order metrics by store.
4. `monthly_sales_summary` — monthly sales and order metrics.

Metrics include total orders, total quantity, total sales, and average order value.

## Key Data Engineering Concepts
- PySpark DataFrame transformations and actions
- Data cleansing and duplicate removal
- Delta table creation and management
- Joins between transactional and dimension datasets
- Aggregations and business metrics
- Slowly Changing Dimensions Types 1, 2, and 3
- Medallion Architecture and layered data processing

## Project Structure
```text
Retail_Medallion_Project/
├── README.md
└── notebooks/
    ├── 01_Project_Setup
    ├── 02_Dataset_Creation
    ├── 03_Bronze_Layer
    ├── 04_Silver_Layer
    ├── 05_SCD_Type_1
    ├── 06_SCD_Type_2
    ├── 07_SCD_Type_3
    └── 08_Gold_Layer_Analytics
```

## How to Explore
1. Review `02_Dataset_Creation` to understand the synthetic datasets.
2. Follow `03_Bronze_Layer` and `04_Silver_Layer` to explore ingestion and cleansing.
3. Review the SCD notebooks to understand historical data management.
4. Explore `08_Gold_Layer_Analytics` to see how the Gold summary tables are built.

## Outcome
Built a complete practice project demonstrating layered data processing, data-quality checks, historical dimension management, and retail analytics using PySpark and Databricks.
