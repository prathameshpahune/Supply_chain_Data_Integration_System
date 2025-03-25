# How to Run the Project

## 1. Run the ETL Process

To run the ETL pipeline, use the following command: python main.py


This will:
- Extract raw data from **train.csv**
- Convert the data into a DataFrame
- Perform **data cleaning**
- Create a **Star Schema**:
  - Dimension Tables
  - Fact Table
- Add the dimension and fact tables into **MySQL** with appropriate **constraints**
- Insert cleaned data into the MySQL tables

---

## 2. Run the Dashboard

To launch the dashboard, use the following command: streamlit run dashboard/app.py


This will:
- Fetch data from **MySQL**
- Perform **KPIs and aggregations** using the fetched data
- Display **graphs, tables, and insights** on an interactive dashboard
