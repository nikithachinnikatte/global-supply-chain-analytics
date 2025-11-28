# global-supply-chain-analytics
End-to-end Supply Chain Analytics System using Python ETL, Neon PostgreSQL, and Power BI dashboards.
This project is an end-to-end Supply Chain Analytics System built using:

1.Python (Pandas) for ETL
Neon PostgreSQL as a cloud data warehouse
Star Schema dimensional modeling
Power BI for dashboarding & business insights
The project analyzes 180K+ orders across customers, products, suppliers, categories, and regions to help businesses monitor:
Revenue & Profit
Lead Time & Delivery Performance
Supplier Efficiency
Customer Segments & Geography
Product Profitability

2.ETL Pipeline (Python)
The ETL pipeline:
Cleans raw CSV data
Handles missing values & inconsistent dates
Converts order/ship dates and calculates lead_time_days
Calculates profit, revenue, delivery risk, etc.
Creates dimension tables:
a.dim_customer
b.dim_product
c.dim_supplier
d.dim_date
e.Creates fact_orders
Loads all tables into Neon PostgreSQL using SQLAlchemy

📁 Files:
etl/etl_to_neon.py
etl/check_columns.py

3.Data Warehouse
fact_orders: Revenue, profit, lead time, delivery status
dim_customer: Segment, location, demographics
dim_product: Category, price, attributes
dim_supplier: Supplier-related metrics
dim_date: Calendar-level hierarchy

3.Power BI Dashboards
Two interactive Power BI pages were created:

🟦 PAGE 1 — Executive Summary
Total Revenue, Profit, Orders
Avg Lead Time, On-Time Delivery %
Revenue Trend (line chart)
Profit by Category (bar chart)
Global Revenue Map (bubble map)
Slicers: Year, Category, Country, Segment

🟩 PAGE 2 — Operations Analytics
Lead Time Distribution (histogram)
Avg Lead Time by Category
On-Time vs Late Deliveries
Supplier Performance
Shipping Mode Analysis

4.DAX measures used-
Total Revenue = SUM('public fact_orders'[total_revenue])
Total Profit = SUM('public fact_orders'[profit])
Total Orders = COUNT('public fact_orders'[order_id])
Avg Lead Time = AVERAGE('public fact_orders'[lead_time_days])
On-Time % = DIVIDE(CALCULATE(COUNTROWS('public fact_orders'), 'public fact_orders'[on_time_flag] = TRUE()), COUNTROWS('public fact_orders'))


5. CLONE THE PROJECT
   git clone https://github.com/<your-username>/global-supply-chain-analytics
6.INSTALL DEPENDENCIES-
   pip install -r requirements.txt
7.UPDATE NEON CONNECTION STRING-
   DATABASE_URL = "postgresql://...<your-neon-url>"
8.Author

Nikitha CR
