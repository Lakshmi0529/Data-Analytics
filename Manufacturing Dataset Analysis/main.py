from sqlalchemy import create_engine,MetaData,Table,select,func,case,between,over
import pandas as pd

# Create engine
engine = create_engine('sqlite:///sales_analysis.db')
metadata = MetaData()
metadata.reflect(bind=engine)

excel_files = {
    r"D:\Data Analytics\Manufacturing data\customers.xlsx": 'customers',
    r"D:\Data Analytics\Manufacturing data\date.xlsx": 'date',
    r"D:\Data Analytics\Manufacturing data\product.xlsx": 'product',
    r"D:\Data Analytics\Manufacturing data\productcategory.xlsx": 'product_category',
    r"D:\Data Analytics\Manufacturing data\productsubcategory.xlsx": 'product_subcategory',
    r"D:\Data Analytics\Manufacturing data\salesterritory.xlsx": 'sales_territory',
    r"D:\Data Analytics\Manufacturing data\sales_new.xlsx": 'sales'
}

for file_path,table_name in excel_files.items():
    df = pd.read_excel(file_path)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=1000
    )


# Create table objects for querying
customers = Table('customers', metadata, autoload_with=engine)
date = Table('date', metadata, autoload_with=engine)
product = Table('product', metadata, autoload_with=engine)
product_category = Table('product_category', metadata, autoload_with=engine)
product_subcategory = Table('product_subcategory', metadata, autoload_with=engine)
sales_territory = Table('sales_territory', metadata, autoload_with=engine)
sales = Table('sales', metadata, autoload_with=engine)

# Handling missing data and data exploration
sales_df = pd.read_sql_table('sales',con=engine)
sales_df=sales_df.drop(columns=['CarrierTrackingNumber','CustomerPONumber'],axis=1)
sales_territory_df = pd.read_sql_table('sales_territory',con=engine)

customers_df = pd.read_sql_table('customers',con=engine)
customers_df = customers_df.drop(columns=['Title','MiddleName','Suffix','EmailAddress',
                                          'SpanishEducation','FrenchEducation','SpanishOccupation',
                                          'FrenchOccupation','AddressLine1','AddressLine2','Phone'],axis=1)
date_df = pd.read_sql_table('date',con=engine)
product_df = pd.read_sql_table('product',con=engine)
product_df = product_df.drop(columns=['WeightUnitMeasureCode','SizeUnitMeasureCode','SpanishProductName',
                                      'FrenchProductName','Size','SizeRange','Weight',
                                      'Class','Style','FrenchDescription','ChineseDescription','ArabicDescription',        
                                    'HebrewDescription','ThaiDescription','GermanDescription','JapaneseDescription',      
                                    'TurkishDescription'],axis=1)
product_category_df = pd.read_sql_table('product_category',con=engine)
product_subcategory_df = pd.read_sql_table('product_subcategory',con=engine)

# Merging the data for analysis
merged = sales_df.merge(date_df,how='left',left_on='OrderDateKey',right_on='DateKey')
merged = merged.merge(product_df, on='ProductKey')
merged = merged.merge(product_subcategory_df, on='ProductSubcategoryKey')
merged = merged.merge(product_category_df, on='ProductCategoryKey')
merged = merged.merge(customers_df, on='CustomerKey')
data = merged.merge(sales_territory_df, on='SalesTerritoryKey')
data['FullDateAlternateKey'] = pd.to_datetime(data['FullDateAlternateKey']).dt.date

# Converting the merged dataframe to a SQL table for querying
data.to_sql('full_sales_data',con=engine,if_exists='replace',index=False,chunksize=1000)
full_sales = Table('full_sales_data',metadata,autoload_with=engine)

# Sales Performance Analysis
    # Monthly sales trends
monthly_sales = select(
    full_sales.c.CalendarYear,
    full_sales.c.EnglishMonthName,
    func.sum(full_sales.c.SalesAmount).label('total_sales'),
    func.sum(full_sales.c.OrderQuantity).label('total_units')
).select_from(full_sales).group_by(full_sales.c.CalendarYear,full_sales.c.EnglishMonthName,full_sales.c.MonthNumberOfYear)\
.order_by(full_sales.c.CalendarYear,full_sales.c.MonthNumberOfYear)

monthly_sales_df = pd.read_sql(monthly_sales,engine)
monthly_sales_df.to_csv('monthly_sales.csv')

    # Product Category Performance
category_performance = select(
    full_sales.c.EnglishProductCategoryName,
    full_sales.c.EnglishProductSubcategoryName,
    func.sum(full_sales.c.SalesAmount).label('total_revenue'),
    func.sum(full_sales.c.OrderQuantity).label('total_units'),
    (func.sum(full_sales.c.SalesAmount)/func.sum(full_sales.c.OrderQuantity)).label('avg_price')
).select_from(full_sales).group_by(full_sales.c.EnglishProductCategoryName,full_sales.c.EnglishProductSubcategoryName)\
    .order_by(func.sum(full_sales.c.SalesAmount).desc())

category_df = pd.read_sql(category_performance,engine)
category_df.to_csv('category_sales.csv')

    # Sales by Territory
territory_sales = select(
    full_sales.c.SalesTerritoryCountry,
    full_sales.c.SalesTerritoryRegion,
    func.sum(full_sales.c.SalesAmount).label('total_sales'),
    func.count(full_sales.c.SalesOrderNumber.distinct()).label('order_count'),
    (func.sum(full_sales.c.SalesAmount)/func.count(full_sales.c.SalesOrderNumber.distinct())).label('avg_order_value')
).select_from(full_sales).group_by(full_sales.c.SalesTerritoryCountry,full_sales.c.SalesTerritoryRegion)\
                         .order_by(func.sum(full_sales.c.SalesAmount).desc())

territory_df = pd.read_sql(territory_sales,engine)
territory_df.to_csv('sales_territory.csv')

# Recency, Frequency, Monetary Value (RFM Analysis)
    # getting the most recent order date
max_date_query = select(func.max(full_sales.c.FullDateAlternateKey))
with engine.connect() as conn:
    max_date = conn.execute(max_date_query).scalar()

rfm_query = select(
    full_sales.c.CustomerKey,
    full_sales.c.FirstName,
    full_sales.c.LastName,
    (func.julianday(max_date)-func.julianday(func.max(full_sales.c.FullDateAlternateKey))).label('recency_days'),
    func.count(full_sales.c.SalesOrderNumber.distinct()).label('frequency'),
    func.sum(full_sales.c.SalesAmount).label('monetary_value')
).select_from(full_sales).group_by(full_sales.c.CustomerKey,full_sales.c.FirstName,full_sales.c.LastName).order_by(func.sum(full_sales.c.SalesAmount).desc())

rfm_df = pd.read_sql(rfm_query,engine)

# Segmenting Customers based on RFM Scores
rfm_df['r_score'] = pd.qcut(rfm_df['recency_days'],q=4,labels=[4,3,2,1])
rfm_df['f_score'] = pd.qcut(rfm_df['frequency'].rank(method='first'),q=4,labels=[1,2,3,4])
rfm_df['m_score'] = pd.qcut(rfm_df['monetary_value'],q=4,labels=[1,2,3,4])
rfm_df['rfm_score'] = rfm_df['r_score'].astype(str) + rfm_df['f_score'].astype(str) + rfm_df['m_score'].astype(str)

    # Defining Customer Segments
segment_map = {
    r'111|112|121|131|141|151|113|114': 'Lost',
    r'134|142|143|144': 'At Risk',
    r'332|322|233|232|223|222|132|123|122|212|211|213|214|221|231|234|241|242|243|244': 'Hibernating',
    r'311|411|331|421|341|321|412|342|422|432|332|312|313|314|334|343|423': 'Loyal',
    r'433|434|443|444|344|413|414|431|441|442': 'Champions',
    r'533|543|542|544|552|553|554': 'Super Champions'
}

rfm_df['segment'] = rfm_df['rfm_score'].replace(segment_map,regex=True)
rfm_df.to_csv('rfm.csv')

# Customer Demographic Insights
income_cte = select(
    case(
        (full_sales.c.YearlyIncome < 30000, 'Low Income'),
        (between(full_sales.c.YearlyIncome, 30000, 60000), 'Middle Income'),
        (full_sales.c.YearlyIncome > 60000, 'High Income'),).label('income_group'),
func.count(full_sales.c.CustomerKey.distinct()).label('customer_count'),
func.sum(full_sales.c.SalesAmount).label('total_spend'),
(func.sum(full_sales.c.SalesAmount)/func.count(full_sales.c.CustomerKey.distinct()))\
    .label('spend_per_customer')).select_from(full_sales).group_by(case(
        (full_sales.c.YearlyIncome < 30000, 'Low Income'),
        (between(full_sales.c.YearlyIncome, 30000, 60000), 'Middle Income'),
        (full_sales.c.YearlyIncome > 60000, 'High Income'),).label('income_group')).cte('income_data')

income_spending = select(
    income_cte.c.income_group,
    income_cte.c.customer_count,
    income_cte.c.total_spend,
    income_cte.c.spend_per_customer,
).select_from(income_cte).order_by(income_cte.c.total_spend.desc())

income_df = pd.read_sql(income_spending,engine)
income_df.to_csv('customer_spend.csv')

# Product Profitability Analysis
product_profitability = select(
    full_sales.c.EnglishProductName,
    full_sales.c.EnglishProductSubcategoryName,
    func.sum(full_sales.c.SalesAmount).label('total_revenue'),
    func.sum(full_sales.c.TotalProductCost).label('total_cost'),
    (func.sum(full_sales.c.SalesAmount)-func.sum(full_sales.c.TotalProductCost)).label('total_profit'),
    ((func.sum(full_sales.c.SalesAmount)-func.sum(full_sales.c.TotalProductCost))/func.sum(full_sales.c.SalesAmount)).label('profit_margin'),
    func.sum(full_sales.c.OrderQuantity).label('total_units')
).select_from(full_sales).group_by(full_sales.c.EnglishProductName,full_sales.c.EnglishProductSubcategoryName)\
                         .having(func.sum(full_sales.c.SalesAmount) > 1000)\
                         .order_by(((func.sum(full_sales.c.SalesAmount)-func.sum(full_sales.c.TotalProductCost))/func.sum(full_sales.c.SalesAmount)).desc())

profitability_df = pd.read_sql(product_profitability,engine)
profitability_df.to_csv('profits.csv')

# Time-Based Analysis

yoy_growth = select(
    full_sales.c.CalendarYear,
    full_sales.c.CalendarQuarter,
    func.sum(full_sales.c.SalesAmount).label('quarterly_sales'),
    (func.sum(full_sales.c.SalesAmount)-func.lag(func.sum(full_sales.c.SalesAmount)).over(partition_by=full_sales.c.CalendarQuarter,order_by=full_sales.c.CalendarYear))\
        /(func.lag(func.sum(full_sales.c.SalesAmount)).over(partition_by=full_sales.c.CalendarQuarter,order_by=full_sales.c.CalendarYear)*100).label('ypy_growth_pct')
).select_from(full_sales).group_by(full_sales.c.CalendarYear,full_sales.c.CalendarQuarter)\
                         .order_by(full_sales.c.CalendarQuarter,full_sales.c.CalendarYear)

yoy_df = pd.read_sql(yoy_growth,engine)
yoy_df.to_csv('sales_growth.csv')
