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
![Q1](https://github.com/user-attachments/assets/151bc038-5503-4c6f-bbe6-8371c982c2f6)



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
![Q2](https://github.com/user-attachments/assets/a3a62333-6eee-48d8-9a61-61b9a4a1af95)



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
![Q3](https://github.com/user-attachments/assets/900e72fe-2f59-4212-b1fc-cf5ba93b8b98)


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
```
![Q4](https://github.com/user-attachments/assets/890350d2-46c1-4f17-b62a-d99bd85f835b)


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
![Q5](https://github.com/user-attachments/assets/358f5603-eb2b-4b99-b322-4142ba8a317c)




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
![Q6](https://github.com/user-attachments/assets/3f86d638-1f74-4577-994b-5daf421713f8)



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
![Q7](https://github.com/user-attachments/assets/1d2765a0-5b38-418f-a06d-a551eb9067d8)



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
![Q8](https://github.com/user-attachments/assets/e352233f-0dc8-47d9-ae92-63eaf9bba365)



## **V. CONCLUSION**
The Bicycle Manufacturer Database System designed using Google BigQuery provides a robust and scalable solution for managing the complexities of bicycle manufacturing, sales, and operations. By leveraging BigQuery’s powerful analytics and cloud-based capabilities, the system enables the manufacturer to efficiently track and analyze key business processes, from raw material procurement to sales transactions and production management.
