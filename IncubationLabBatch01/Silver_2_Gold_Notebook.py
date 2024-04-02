# Databricks notebook source
# DBTITLE 1,Execute necessary functions
# MAGIC %run 
# MAGIC "./FunctionsNotebook"

# COMMAND ----------

# DBTITLE 1,Creation of Widgets and GoldSchemaPath
dbutils.widgets.text("SchemaName", "SalesLT")
v_schemaName = dbutils.widgets.get("SchemaName")
goldSchemaPath= goldPath+v_schemaName+'/'

# COMMAND ----------

# MAGIC %md
# MAGIC Consolidated Customer Table Creation:

# COMMAND ----------

# DBTITLE 1,Creation of ConsolidatedCustomerDf/Table
CustomerDf = spark.sql('''select c.CustomerID as CustomerID,a.AddressID as AddressID,CONCAT_WS(' ',c.FirstName,case when c.Middlename = 'NA' THEN  '' ELSE c.Middlename END,c.LastName) as CustomerName
,(case when c.Title='Mr.' then 'Male' when c.Title='Ms.' then 'Female' else 'Unkown' end) as Gender,c.EmailAddress,c.Phone as PhoneNumber, a.AddressLine1,a.AddressLine2,a.City,a.StateProvince,a.CountryRegion,a.PostalCode,ca.AddressType,c.CompanyName, c.SalesPerson as SalesPerson
from silver_saleslt.Customer c
join silver_saleslt.CustomerAddress ca on  c.CustomerID=ca.CustomerID
join silver_saleslt.Address a on a.AddressID=ca.AddressID''')
CustomerDf.write.format("delta").mode("OVERWRITE").option("mergeSchema", "true").save(goldSchemaPath+'Customer/')

# COMMAND ----------

# MAGIC %md
# MAGIC Consolidated Product Table Creation:

# COMMAND ----------

# DBTITLE 1,Creation of ProductModel & ProductDescription Df/Table
pmpdDf= spark.sql('''select PM.ProductModelID,PD.ProductDescriptionID,PM.Name AS ProductModelName,PD.Description,PMPD.Culture from silver_saleslt.ProductDescription PD
join silver_saleslt.ProductModelProductDescription PMPD ON PD.ProductDescriptionID=PMPD.ProductDescriptionID
JOIN silver_saleslt.ProductModel PM ON PM.ProductModelID=PMPD.ProductModelID''')
pmpdDf.createOrReplaceTempView("PM")

# COMMAND ----------

# DBTITLE 1,Creation of ProductCategory Df/Table
pcDf= spark.sql('''SELECT PPC.ProductCategoryID, PC.Name AS ParentProductCategory, PPC.Name as ProductCategory FROM silver_saleslt.ProductCategory PC JOIN silver_saleslt.ProductCategory PPC ON PC.ProductCategoryID = PPC.ParentProductCategoryID''')
pcDf.createOrReplaceTempView("PC")

# COMMAND ----------

# DBTITLE 1,Creation of Product Df/Table
productDf = spark.sql('''SELECT PC.*,PM.*,P.ProductID,P.Name as ProductName,P.ProductNumber,P.Color as ProductColor,P.StandardCost,P.ListPrice,P.Size,P.Weight,P.SellStartDate,P.SellEndDate,P.DiscontinuedDate FROM silver_saleslt.Product P
JOIN PM ON PM.ProductModelID= P.ProductModelID
JOIN PC ON PC.ProductCategoryID =P.ProductCategoryID
ORDER BY ProductID,PC.ProductCategoryID,PM.ProductModelID''')
productDf.write.format("delta").mode("OVERWRITE").option("mergeSchema", "true").save(goldSchemaPath+'Product/')

# COMMAND ----------

# MAGIC %md
# MAGIC Consolidated Sales Table Creation

# COMMAND ----------

# DBTITLE 1,Creation of SalesOrder Df/Table
salesDf = spark.sql('''select SH.SalesOrderID,SH.RevisionNumber,SO.SalesOrderDetailID,CASE WHEN SH.OnlineOrderFlag= 1 THEN 'OnlineOrder'  else 'InStoreOrder' end as OrderType,SO.ProductID,SH.OrderDate,SH.DueDate,SH.ShipDate, SO.OrderQty,SO.UnitPrice,SO.UnitPriceDiscount,SO.LineTotal,
CASE WHEN SH.Status= 1 THEN 'In_process' 
	WHEN SH.Status= 2 THEN 'Approved'
	WHEN SH.Status= 3 THEN 'Backordered'
	WHEN SH.Status= 4 THEN 'Rejected'
	WHEN SH.Status= 5 THEN 'Shipped'
	WHEN SH.Status= 6 THEN 'Cancelled' ELSE 'UNKOWN' END AS OrderStatus,
SH.SalesOrderNumber,SH.PurchaseOrderNumber,SH.AccountNumber,SH.CustomerID,SH.ShipToAddressID,SH.BillToAddressID,SH.ShipMethod,SH.SubTotal,SH.TaxAmt,SH.Freight,SH.TotalDue from silver_saleslt.SalesOrderHeader SH
JOIN silver_saleslt.SalesOrderDetail SO ON SO.SalesOrderID = SH.SalesOrderID
''')
salesDf.write.format("delta").mode("OVERWRITE").option("mergeSchema", "true").save(goldSchemaPath+'SalesOrder/')

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of ReportingTable

# COMMAND ----------

reportingDf = spark.sql('''SELECT so.SalesOrderID, so.SalesOrderDetailID, so.OrderType,so.OrderDate,so.DueDate, so.ShipDate, so.OrderQty, so.UnitPrice,
so.UnitPriceDiscount, so.LineTotal, so.OrderStatus, so.SalesOrderNumber, so.PurchaseOrderNumber, so.AccountNumber, so.ShipToAddressID, 
so.BillToAddressID, so.ShipMethod, so.Subtotal, so.TaxAmt ,so.Freight, so.TotalDue,p.ProductID, p.ParentProductCategory, 
p.ProductCategory,p.ProductModelName, p.Description,p.Culture, p.ProductName,p.ProductNumber,p.ProductColor,p.StandardCost, p.ListPrice, p.Size, p.Weight,p.SellStartDate, p.SellEndDate, p.DiscontinuedDate,c.CustomerID,c.CustomerName, c.Gender, c.EmailAddress, 
c.PhoneNumber,c.AddressLine1, c.AddressLine2,c.City,c.StateProvince, c.Countryregion, c.PostalCode, c.AddressType,c.CompanyName,c.SalesPerson
FROM gold_saleslt.SalesOrder  so
join gold_saleslt.Product  p on so.ProductID=p.ProductID
join gold_saleslt.Customer  c on so.CustomerID = c.CustomerID
''')
reportingDf.write.format("delta").mode("OVERWRITE").option("mergeSchema", "true").save(goldSchemaPath+'ReportingTable/')
