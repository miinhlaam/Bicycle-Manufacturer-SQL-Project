### Bicycle Manufacturer SQL Project

## **I. INTRODUCTION**
This project aims to practice SQL syntax, problem solving skill apply for a bicycle manufacturing company using Google BigQuery. The system will efficiently manage large volumes of data related to bicycle production, inventory, sales, and customer management, taking advantage of BigQuery’s powerful capabilities in handling large datasets and performing complex analytics in real-time.
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


### **Query 4: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory**
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


### **Query 5: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)**
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



### **Query 6: Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal**
```SQL
WITH stock_info AS (
  SELECT
    product.Name name
    ,extract(MONTH FROM DATE(work.ModifiedDate)) AS MONTH
    ,sum(work.StockedQty) stock_qty
  FROM `adventureworks2019.Production.Product` product 
  LEFT JOIN `adventureworks2019.Production.WorkOrder` work on product.ProductID = work.ProductID
  WHERE extract(YEAR FROM DATE(work.ModifiedDate)) = 2011
  GROUP BY 1,2
),
get_diff AS (
SELECT
  *
  ,lead(stock_qty) OVER(PARTITION BY name ORDER BY month DESC) stock_prv
  ,round(100.0*(stock_qty/lead(stock_qty) OVER(PARTITION BY name ORDER BY month DESC)-1),1) cal_diff
 FROM stock_info
)
SELECT
  name
  ,month
  ,stock_qty
  ,stock_prv
  ,coalesce(cal_diff, 0)
FROM get_diff
ORDER BY name, month DESC;
```
| month	| avg_revenue_by_user_per_visit| 
| --- | --- |
| 201707	| 43.86| 



### **Query 7:  Calc Ratio of Stock / Sales in 2011 by product name, by month. Order results by month desc, ratio desc. Round Ratio to 1 decimal, mom yoy**
```SQL
WITH sales_info AS (
  SELECT  
    extract(MONTH FROM DATE(sales.ModifiedDate)) sales_mth
    ,extract(YEAR FROM DATE(sales.ModifiedDate)) yr
    ,sales.productID
    ,product.Name name
    ,sum(sales.OrderQty) sales_qty
  FROM `adventureworks2019.Sales.SalesOrderDetail` sales
  LEFT JOIN `adventureworks2019.Production.Product` product ON sales.ProductID =  product.ProductID
    WHERE extract(YEAR FROM DATE(sales.ModifiedDate)) = 2011
  GROUP BY 1,2,3,4
),
stock_info AS (
  SELECT
    extract(MONTH FROM DATE(ModifiedDate)) stock_mth
    ,productID
    ,sum(StockedQty) stock_qty
  FROM `adventureworks2019.Production.WorkOrder`
  WHERE extract(YEAR FROM DATE(ModifiedDate)) = 2011
  GROUP BY 1,2
)
SELECT 
  sales_mth
  ,yr
  ,sales_info.productID
  ,name
  ,sales_qty
  ,stock_qty
  ,round(stock_qty/sales_qty,1) ratio
FROM sales_info LEFT JOIN stock_info 
  ON sales_info.ProductID = stock_info.ProductID AND sales_info.sales_mth = stock_info.stock_mth
ORDER BY 1 DESC, 7 DESC;
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


### **Query 8: No of order and value at Pending status in 2014**
- Add_to_cart_rate = number product add to cart / number product view.
- Purchase_rate = number product purchase/number product view.

```SQL
SELECT  
  extract(YEAR FROM DATE(ModifiedDate)) yr
  ,Status
  ,count(DISTINCT PurchaseOrderID) order_Cnt
  ,sum(TotalDue) value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
WHERE Status = 1 AND extract(YEAR FROM DATE(ModifiedDate)) = 2014
GROUP BY 1,2;
```
| month	| num_product_view	| num_addtocart	| num_purchase	| add_to_cart_rate	| purchase_rate| 
| --- | --- | --- | --- | --- | --- |
| 201701	| 25787	| 7342	| 2143	| 28.47	| 8.31| 
| 201702	| 21489	| 7360	| 2060	| 34.25	| 9.59| 
| 201703	| 23549	| 8782	| 2977	| 37.29	| 12.64| 



## **V. CONCLUSION**
The Bicycle Manufacturer Database System designed using Google BigQuery provides a robust and scalable solution for managing the complexities of bicycle manufacturing, sales, and operations. By leveraging BigQuery’s powerful analytics and cloud-based capabilities, the system enables the manufacturer to efficiently track and analyze key business processes, from raw material procurement to sales transactions and production management.
