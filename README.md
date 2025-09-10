# Funiture-Sales-Performance
This project analyzes the furniture sales performance for 2014, 2015, 2016 and 2017 using an interactive power-bi dashboard. It explores sales  trends, top-selling states and cities, shipping modes, and product categories, uncovering insights to drive data‑driven business decisions.

---

## Table of Contents
1. [Overview](#overview)
2. [Raw Data](#raw-data)
3. [Dashboard & Dashboard Features](#dashboard-features)
4. [Data Cleaning Process](Data--cleaning--process&preprocessing)
5. [Key Metrics](#key-metrics)
6. [Insights & Conclusions](#insights--conclusions)

---

## Overview
This interactive power-bi dashboard analyzes U.S. furniture sales performance . It helps stakeholders such as sales leaders, category managers, operations, and finance understand sales, profit, product mix, customer segments, geographies, and shipping modes to support data-driven decisions.

---

## Raw Data
The raw data for this dashboard includes a comprehensive dataset of the furniture sales dataset. It consists of the following fields:

1. **Order ID**: Unique identifier for each order.
2. **Order Date**: Date the order was placed.
3. **Ship Date**: Date the order was shipped.
4. **Ship Mode**: Fulfillment method (e.g., First Class, Second Class).
5. **Customer ID**: Unique identifier for each customer.
6. **Customer Name**: Customer’s full name.
7. **Segment**: Customer segment (Consumer, Corporate, Home Office).
8. **Country**: Country of the order (U.S.).
9. **City**: City where the order was delivered.
10. **State**: State where the order was delivered.
11. **Region**: U.S. region (e.g., West, East, Central, South).
12. **Category**: Top-level product category (e.g., Furniture).
13. **Sub-Category**: Product sub-category (e.g., Chairs, Bookcases, Tables).
14. **Product Name**: Descriptive product name.
15. **Sales**: Revenue from the line item.
16. **Quantity**: Units sold in the line item.
17. **Product ID**: Unique identifier for each product.
18. **Profit**: Profit from the line item (can be negative).
19. **Month**: Month label derived from Order Date for time analysis.
 - <a href="https://github.com/Femi-Valor/furniture-sales"> Raw Data
    
---

### Data Cleaning Process in SQL
- **Removed Duplicates**: Ensured the uniqueness of records by checking against Order ID and Product ID.
- **Data Types**: Converted columns such as Order Date and Ship Date to proper Date format, and ensured Sales, Quantity, and Profit were numeric.
- **Null Handling**: Replaced missing values in categorical fields (e.g., City, State, Region) with “NULL”.
- **Derived Columns**: Extracted Month and Year from Order Date for time-series analysis, and also duration using power query in power-bi
- **Text Standardization**: Cleaned up inconsistent entries in Category and Sub-Category fields for accurate grouping.

---

### Data Exploration in SQL
- **Sales By State**
- **Shipping Mode**
- **Monthly Sales Trend**
- **Top 5 Cities**
- **Duraton Table**
- **Category Sales**

---

### Data Modelling

<img width="724" height="527" alt="Screenshot 2025-09-10 212147" src="https://github.com/user-attachments/assets/6ac10b50-a851-4d32-9d96-43756eeedf4b" />

---

## Dashboard Features
- **Interactive Filters**: Users can explore data by Region, State, City, Category, Sub-Category, and Ship Mode for flexible analysis.
- **Visualizations**: The dashboard includes trend lines, bar charts, pie charts, and maps to display sales distribution, profit patterns, and shipping insights.
- **KPI Highlight Cards**: At-a-glance metrics for Total Sales, Total Profit, Quantity Sold, and YoY comparisons.
- **Category & Product Analysis**: Breakdowns by category and sub-category reveal top and bottom performers, including loss-making products.
- **Geographic Insights**:State and city level maps highlight sales hotspots and underperforming regions.
- **Customer Segments**: Analysis by Consumer, Corporate, and Home Office segments to uncover profitability trends.
- **Shipping Performance**: Comparison of Ship Modes (First Class, Second Class, Standard Class, Same Day) to evaluate cost vs. margin trade-offs.
- **Time-Based Trends**: Monthly sales and profit charts highlight seasonal patterns and growth opportunities.
- **Shipping Duration**: ship date minus order date to get the number of days it took to be delivered.
- **User-Centric Design**: Clean layout, consistent formatting, and Excel slicers ensure clarity and easy exploration.
- **Dynamic Insights**: All visuals update instantly based on user selections, supporting scenario-based decision making.
<img width="997" height="553" alt="Screenshot 2025-09-10 212036" src="https://github.com/user-attachments/assets/5ea1a710-f1ac-4c72-83cc-9becf6f377cf" />

  - <a href="https://github.com/Femi-Valor/furniture-sales-power-bi/blob/main/funiture%20sales%20c.xlsx"> Live Interaction


---

## Key Metrics
1. **Total Sales**: $742.0k
2. **Total Profit**: $18.5k
   - Profit Margin: ~57%
3. **Total Quantity Sold**: 8.0k units
4. **Top Sub-Category by Sales: Chairs** (highest revenue contributor)
5. **Shippng Duration**: 4-days (~28%)
6. **Top Region by Sales**: West (largest revenue and profit contributor)
7. **Top State by Sales**: California
8. **Most Used Ship Mode**: Standard Class (~59% of all orders)
9. **Customer Segment with Highest Sales**: Consumer
10. **Seasonal Pattern**: Sales peak in November & December (holiday effect), lowest in February.

---
## Insights & Conclusions
1. **Category Performance**:
   - Furniture and Technology drive the bulk of revenue, but Office Supplies contributes steady profits with less risk.
   - Tables stand out as a loss-making sub-category, suggesting pricing or discount issues.
2. **Regional Performance**:
   - The West region leads both in sales and profits, driven by California.
   - The South lags in profitability, indicating possible cost or discounting challenges.
3. **Shipping Mode Impact**:
   - Standard Class dominates order volume but drags profitability.
   - First Class and Same Day orders, though fewer, generate higher profit margins per unit.
4. **Customer Segments**:
   - Consumer segment contributes the most revenue.
   - Corporate customers show stronger profitability despite lower overall sales.
5. **Seasonal Trends**:
   - Strong spikes in November–December highlight seasonal opportunities (holiday promotions).
   - Early months (Jan–Feb) are consistently weak, suggesting room for off-season sales strategies.

---


