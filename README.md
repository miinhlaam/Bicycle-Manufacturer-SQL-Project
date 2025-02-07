### Bicycle Manufacturer SQL Project

## **I. INTRODUCTION**
The goal of this project is to design and develop an SQL-based database system to manage the operations of a bicycle manufacturing company. The system will help manage the production, sales, inventory, and employee-related data efficiently and effectively. It will allow the manufacturer to track raw materials, finished products, sales, suppliers, and customers.
## **II. DATASET**
- The AdventureWorks2019 dataset is stored in a public Google BigQuery dataset.
  
- Dataset Dictionary: [https://support.google.com/analytics/answer/3437719?hl=en](https://drive.google.com/file/d/1bwwsS3cRJYOg1cvNppc1K_8dQLELN16T/view)
  

## **III. EXPLORE THE DATASET**
### **Query 1: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M**
```SQL
SELECT FORMAT_DATETIME('%b %Y', a.ModifiedDate) month
      ,c.Name
      ,sum(a.OrderQty) qty_item
      ,sum(a.LineTotal) total_sales
      ,count(distinct a.SalesOrderID) order_cnt
FROM `adventureworks2019.Sales.SalesOrderDetail` a 
LEFT JOIN `adventureworks2019.Production.Product` b
  ON a.ProductID = b.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c
  ON b.ProductSubcategoryID = cast(c.ProductSubcategoryID as string)

WHERE DATE(a.ModifiedDate) >=  (SELECT DATE_SUB(date(max(a.ModifiedDate)), INTERVAL 12 month)
                                FROM `adventureworks2019.Sales.SalesOrderDetail` )--2013-06-30
GROUP BY 1,2
ORDER BY 2,1;
```
| month	| visits	| pageviews	| transactions| 
| --- | --- | --- | --- |
|201701	|64694	|257708	|713|
| 201702	| 62192	| 233373	| 733| 
| 201703	| 69931	| 259522	| 993| 



### **Query 2: Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Can use metric: quantity_item. Round results to 2 decimal**

```SQL
WITH info AS (
  SELECT
    sub.Name Name
    ,extract(YEAR FROM DATE(sales.ModifiedDate)) yr
    ,sum(sales.OrderQty) qty
    FROM `adventureworks2019.Sales.SalesOrderDetail` sales
  LEFT JOIN `adventureworks2019.Production.Product` product ON sales.ProductID = product.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` sub ON cast(product.ProductSubcategoryID AS INT) = sub.ProductSubcategoryID
  GROUP BY 1,2
),
get_diff AS (
  SELECT
    Name
    ,yr
    ,qty
    ,lag(qty) OVER(PARTITION BY Name ORDER BY yr) prv_qty
    ,round(qty/lag(qty) OVER(PARTITION BY Name ORDER BY yr)-1,2) qty_diff
  FROM info
  ORDER BY 2
),
get_rnk AS (
SELECT
  Name 
  ,qty 
  ,prv_qty
  ,coalesce(qty_diff,0) qty_diff
  ,dense_rank() OVER(ORDER BY qty_diff DESC) rnk
FROM get_diff
)
SELECT
  Name 
  ,qty qty_item
  ,prv_qty
  ,qty_diff
  FROM get_rnk
  WHERE rnk IN (1,2,3)
  ORDER BY rnk;
```
| source	| total_visits	| total_no_of_bounces	| bounce_rate| 
| --- | --- | --- | --- |
| google	| 38400	| 19798	| 51.557| 
| (direct)	| 19891	| 8606	| 43.266| 
| youtube.com	| 6351	| 4238	| 66.73| 
| analytics.google.com	| 1972	| 1064	| 53.955| 
| Partners	| 1788	| 936	| 52.349| 
| m.facebook.com	| 669	| 430	| 64.275| 
| google.com	| 368	| 183	| 49.728| 
| dfa	| 302	| 124	| 41.06| 
| sites.google.com	| 230	| 97	| 42.174| 
| facebook.com	| 191	| 102	| 53.403| 
| reddit.com	| 189	| 54	| 28.571| 
| qiita.com	| 146	| 72	| 49.315| 


### **Query 3: Ranking Top 3 TeritoryID with biggest Order quantity of every year. If there's TerritoryID with same quantity in a year, do not skip the rank number**
```SQL
WITH oder_cnt_tab AS (
  SELECT
    extract(YEAR FROM DATE(a.ModifiedDate)) AS yr
    ,b.TerritoryID TerritoryID
    ,sum(a.OrderQty) AS order_cnt
  FROM `adventureworks2019.Sales.SalesOrderDetail` a 
  LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` b ON a.SalesOrderID = b.SalesOrderID
  GROUP BY 1,2
),
rnk_tab AS (
  SELECT 
    yr
    ,TerritoryID
    ,order_cnt
    ,dense_rank() OVER(PARTITION BY yr ORDER BY order_cnt DESC) rnk
  FROM oder_cnt_tab
)
SELECT 
  yr
  ,TerritoryID
  ,order_cnt
  ,rnk
FROM rnk_tab
WHERE rnk IN (1,2,3)
RDER BY yr DESC, rnk;
```
| time_type	| time	| source	| revenue| 
| --- | --- | --- | --- |
| Month	| 201706	| (direct)	| 97333.6197| 
| Month	| 201706	| google	| 18757.1799| 
| Month	| 201706	| dfa	| 8862.23| 
| Month| 	201706|	mail.google.com| 	2563.13| 
| Month	| 201706| 	search.myway.com| 	105.94| 
| Month| 	201706| 	groups.google.com| 	101.96| 
| Month| 	201706| 	chat.google.com| 	74.03| 
| Month| 	201706| 	dealspotr.com| 	72.95| 
| Month| 	201706| 	mail.aol.com| 	64.85| 
| Month| 	201706| 	phandroid.com| 	52.95| 
| Month| 	201706| 	sites.google.com| 	39.17| 
| Month| 	201706| 	google.com| 	23.99| 
| Month| 	201706| 	yahoo| 	20.39| 
| Month| 	201706| 	youtube.com| 	16.99| 
| Month| 	201706| 	bing| 	13.98| 
| Month	| 201706| 	l.facebook.com| 	12.48| 
| Week	| 201722	| (direct)	| 6888.9| 
| Week	| 201722	| google| 	2119.39| 
| Week	| 201722	| dfa	| 1670.65| 
| Week	| 201722	| sites.google.com	| 13.98| 
| Week	| 201723	| (direct)	| 17325.6799| 
| Week| 	201723	| dfa| 	1145.28| 
| Week| 	201723	| google	| 1083.95| 
| Week	| 201723| 	search.myway.com| 	105.94| 
| Week| 	201723	| chat.google.com	| 74.03| 
| Week	| 201723	| youtube.com	| 16.99| 
| Week	| 201724	| (direct)	| 30908.9099| 
| Week	| 201724	| google| 	9217.17| 
| Week	| 201724| 	mail.google.com	| 2486.86| 
| Week	| 201724	| dfa	| 2341.56| 
| Week	| 201724	| dealspotr.com	| 72.95| 
| Week	| 201724	| bing	| 13.98| 
| Week| 	201724	| l.facebook.com	| 12.48| 
| Week	| 201725	| (direct)	| 27295.3199| 
| Week	| 201725	| google	| 1006.1| 
| Week	| 201725| 	mail.google.com	| 76.27| 
| Week	| 201725| 	mail.aol.com| 	64.85| 
| Week	| 201725	| phandroid.com	| 52.95| 
| Week	| 201725	| groups.google.com	| 38.59| 
| Week	| 201725	| sites.google.com	| 25.19| 
| Week	| 201725	| google.com| 	23.99| 
| Week	| 201726	| (direct)	| 14914.81| 
| Week	| 201726	| google	| 5330.57| 
| Week	| 201726	| dfa	|3704.74| 
| Week	| 201726	| groups.google.com	| 63.37| 
| Week	| 201726	| yahoo	| 20.39| 


### **Query 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017**
```SQL
SELECT
  extract(YEAR FROM DATE(sales.ModifiedDate))
  ,sub.Name Name
  ,sum(sales.OrderQty * sales.UnitPrice * offer.DiscountPct) total_cost
FROM `adventureworks2019.Sales.SalesOrderDetail` sales
LEFT JOIN `adventureworks2019.Sales.SpecialOffer` offer ON sales.SpecialOfferID = offer.SpecialOfferID
LEFT JOIN `adventureworks2019.Production.Product` product ON sales.ProductID = product.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` sub ON cast(product.ProductSubcategoryID AS INT) = sub.ProductSubcategoryID
WHERE lower(offer.Type) LIKE '%seasonal discount%' 
GROUP BY 1,2
ORDER BY 1,2;

--> mình nên luôn tách aggregate function và field*field ra, cho dễ nhìn, dễ kiểm soát output 
SELECT 
    FORMAT_TIMESTAMP("%Y", ModifiedDate)
    , Name
    , sum(disc_cost) AS total_cost
FROM (
      SELECT DISTINCT a.ModifiedDate
      , c.Name
      , d.DiscountPct, d.Type
      , a.OrderQty * d.DiscountPct * UnitPrice AS disc_cost 
      FROM `adventureworks2019.Sales.SalesOrderDetail` a
      LEFT JOIN `adventureworks2019.Production.Product` b ON a.ProductID = b.ProductID
      LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c ON cast(b.ProductSubcategoryID AS INT) = c.ProductSubcategoryID
      LEFT JOIN `adventureworks2019.Sales.SpecialOffer` d ON a.SpecialOfferID = d.SpecialOfferID
      WHERE lower(d.Type) LIKE '%seasonal discount%' 
)
GROUP BY 1,2;
```
| month	| avg_pageviews_purchase| avg_pageviews_non_purchase
| --- | --- | --- |
| 201706	| 94.02050113895217	| 316.86558846341671| 
| 201707	| 124.23755186721992	| 334.05655979568053| 


### **Query 5: Average number of transactions per user that made a purchase in July 2017**
```SQL
WITH 
info AS (
  SELECT  
      extract(MONTH FROM ModifiedDate) AS month_no
      , extract(YEAR FROM ModifiedDate) AS year_no
      , CustomerID
      , count(Distinct SalesOrderID) AS order_cnt
  FROM `adventureworks2019.Sales.SalesOrderHeader`
  WHERE FORMAT_TIMESTAMP("%Y", ModifiedDate) = '2014'
  AND Status = 5
  GROUP BY 1,2,3
  ORDER BY 3,1 
),

row_num AS (--đánh số thứ tự các tháng họ mua hàng
  SELECT *
      , row_number() OVER(PARTITION BY CustomerID ORDER BY month_no) AS row_numb
  FROM info 
), 

first_order AS (   --lấy ra tháng đầu tiên của từng khách
  SELECT *
  FROM row_num
  WHERE row_numb = 1
), 

month_gap AS (
  SELECT 
      a.CustomerID
      , b.month_no AS month_join
      , a.month_no AS month_order
      , a.order_cnt
      , concat('M - ',a.month_no - b.month_no) AS month_diff
  FROM info a 
  LEFT JOIN first_order b 
  ON a.CustomerID = b.CustomerID
  ORDER BY 1,3
)

SELECT month_join
      , month_diff 
      , count(DISTINCT CustomerID) AS customer_cnt
FROM month_gap
GROUP BY 1,2
ORDER BY 1,2;

```
| month	| avg_total_transactions_per_user| 
| --- | --- |
| 201707	| 4.16390041493776| 



### **Query 6: Average amount of money spent per session. Only include purchaser data in July 2017**
```SQL
SELECT  
  format_date('%Y%m', cast(date as date format 'YYYYMMDD')) month
  ,round((sum(product.productRevenue)/1000000)/count(totals.visits),2) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) hits,
      UNNEST(hits.product) product
WHERE 
  product.productRevenue is not null
  and totals.transactions is not null
  and extract(month from cast(date as date format 'YYYYMMDD')) = 07
GROUP BY month;
```
| month	| avg_revenue_by_user_per_visit| 
| --- | --- |
| 201707	| 43.86| 



### **Query 7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017**
```SQL
WITH v1 AS (
  SELECT
    distinct fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
        UNNEST(hits) hits,
        UNNEST(hits.product) product
  WHERE product.productRevenue IS NOT NULL 
        AND extract(month from cast(date as date format 'YYYYMMDD')) = 07
        AND product.v2ProductName = "YouTube Men's Vintage Henley"
),
purchased AS (
  SELECT
    fullVisitorId
    ,product.v2ProductName name
    ,product.productQuantity quantity
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
              UNNEST(hits) hits,
              UNNEST(hits.product) product
  WHERE product.productRevenue is not null 
    AND extract(month from cast(date as date format 'YYYYMMDD')) = 07
)
SELECT
  p.name other_purchased_products
  ,sum(p.quantity) quantity
FROM v1 LEFT JOIN purchased p
ON v1.fullVisitorId = p.fullVisitorId
WHERE p.name != "YouTube Men's Vintage Henley"
GROUP BY p.name
ODER BY quantity desc;
```
| other_purchased_products	| quantity| 
| --- | --- |
| Google Sunglasses	| 20| 
| Google Women's Vintage Hero Tee Black	| 7| 
| SPF-15 Slim & Slender Lip Balm	| 6| 
| Google Women's Short Sleeve Hero Tee Red Heather	| 4| 
| Google Men's Short Sleeve Badge Tee Charcoal	| 3| 
| YouTube Men's Fleece Hoodie Black	| 3| 
| YouTube Twill Cap	| 2| 
| Google Men's Short Sleeve Hero Tee Charcoal	| 2| 
| Red Shine 15 oz Mug	| 2| 
| Crunch Noise Dog Toy	| 2| 


### **Query 8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. Output calculated in product level**
- Add_to_cart_rate = number product add to cart / number product view.
- Purchase_rate = number product purchase/number product view.

```SQL
WITH total_cohort AS (
  SELECT
    format_date('%Y%m', cast(date as date format 'YYYYMMDD')) month
    ,SUM(CASE WHEN eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) num_product_view
    ,SUM(CASE WHEN eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) num_addtocart
    ,SUM(CASE WHEN eCommerceAction.action_type = '6' 
              AND product.productRevenue IS NOT NULL 
              THEN 1 ELSE 0 END) num_purchase
  FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) hits,
      UNNEST(hits.product) product
  WHERE EXTRACT(month from cast(date as date format 'YYYYMMDD')) IN (1,2,3)
  GROPU BY month
)
SELECT
  month
  ,num_product_view
  ,num_addtocart
  ,num_purchase
  ,round(100.0*num_addtocart/num_product_view,2) add_to_cart_rate
  ,round(100.0*num_purchase/num_product_view,2) purchase_rate
FROM total_cohort
ORDER BY month;
```
| month	| num_product_view	| num_addtocart	| num_purchase	| add_to_cart_rate	| purchase_rate| 
| --- | --- | --- | --- | --- | --- |
| 201701	| 25787	| 7342	| 2143	| 28.47	| 8.31| 
| 201702	| 21489	| 7360	| 2060	| 34.25	| 9.59| 
| 201703	| 23549	| 8782	| 2977	| 37.29	| 12.64| 



## **V. CONCLUSION**
In conclusion, my analysis of the eCommerce dataset using SQL on Google BigQuery has uncovered valuable **_insights into total visits, pageviews, transactions, bounce rates, and revenue per traffic source_**, which can inform future business decisions. The **_next step_** will involve **_visualizing_** these insights and key trends using software such as Power BI or Tableau. Overall, this project demonstrates the **_effectiveness of employing SQL and big data tools_** like Google BigQuery to **_derive meaningful insights from large datasets_**.
