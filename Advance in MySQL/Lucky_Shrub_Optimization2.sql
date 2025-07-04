
-- =====================================
-- Create the database and use it
-- =====================================
CREATE DATABASE IF NOT EXISTS Lucky_Shrub;
USE Lucky_Shrub;

-- =====================================
-- Create tables
-- =====================================

CREATE TABLE IF NOT EXISTS Orders (
  OrderID INT NOT NULL PRIMARY KEY,
  ClientID VARCHAR(10),
  ProductID VARCHAR(10),
  Quantity INT,
  Cost DECIMAL(6,2),
  Date DATE
);

CREATE TABLE IF NOT EXISTS Products (
  ProductID VARCHAR(10),
  ProductName VARCHAR(100),
  BuyPrice DECIMAL(6,2),
  SellPrice DECIMAL(6,2),
  NumberOfItems INT
);

CREATE TABLE IF NOT EXISTS Activity (
  ActivityID INT PRIMARY KEY,
  Properties JSON
);

-- =====================================
-- Populate Orders table
-- =====================================

INSERT INTO Orders (OrderID, ClientID, ProductID , Quantity, Cost, Date) VALUES 
(1, "Cl1", "P1", 10, 500, "2020-09-01"),
(2, "Cl2", "P2", 5, 100, "2020-09-05"),
(3, "Cl3", "P3", 20, 800, "2020-09-03"),
(4, "Cl4", "P4", 15, 150, "2020-09-07"),
(5, "Cl3", "P3", 10, 450, "2020-09-08"),
(6, "Cl2", "P2", 5, 800, "2020-09-09"),
(7, "Cl1", "P4", 22, 1200, "2020-09-10"),
(8, "Cl3", "P1", 15, 150, "2020-09-10"),
(9, "Cl1", "P1", 10, 500, "2020-09-12"),
(10, "Cl2", "P2", 5, 100, "2020-09-13"),
(11, "Cl4", "P5", 5, 100, "2020-09-15"),
(12, "Cl1", "P1", 10, 500, "2022-09-01"),
(13, "Cl2", "P2", 5, 100, "2022-09-05"),
(14, "Cl3", "P3", 20, 800, "2022-09-03"),
(15, "Cl4", "P4", 15, 150, "2022-09-07"),
(16, "Cl3", "P3", 10, 450, "2022-09-08"),
(17, "Cl2", "P2", 5, 800, "2022-09-09"),
(18, "Cl1", "P4", 22, 1200, "2022-09-10"),
(19, "Cl3", "P1", 15, 150, "2022-09-10"),
(20, "Cl1", "P1", 10, 500, "2022-09-12"),
(21, "Cl2", "P2", 5, 100, "2022-09-13"),
(22, "Cl2", "P1", 10, 500, "2021-09-01"),
(23, "Cl2", "P2", 5, 100, "2021-09-05"),
(24, "Cl3", "P3", 20, 800, "2021-09-03"),
(25, "Cl4", "P4", 15, 150, "2021-09-07"),
(26, "Cl1", "P3", 10, 450, "2021-09-08"),
(27, "Cl2", "P1", 20, 1000, "2022-09-01"),
(28, "Cl2", "P2", 10, 200, "2022-09-05"),
(29, "Cl3", "P3", 20, 800, "2021-09-03"),
(30, "Cl1", "P1", 10, 500, "2022-09-01");

-- =====================================
-- Populate Products table
-- =====================================

INSERT INTO Products (ProductID, ProductName, BuyPrice, SellPrice, NumberOfItems) VALUES 
("P1", "Artificial grass bags", 40, 50, 100),
("P2", "Wood panels", 15, 20, 250),
("P3", "Patio slates", 35, 40, 60),
("P4", "Sycamore trees", 7, 10, 50),
("P5", "Trees and Shrubs", 35, 50, 75),
("P6", "Water fountain", 65, 80, 15);

-- =====================================
-- Populate Activity table
-- =====================================

INSERT INTO Activity (ActivityID, Properties) VALUES 
(1, '{ "ClientID": "Cl1", "ProductID": "P1", "Order": "True" }'),
(2, '{ "ClientID": "Cl2", "ProductID": "P4", "Order": "False" }'),
(3, '{ "ClientID": "Cl5", "ProductID": "P5", "Order": "True" }');

-- =====================================
-- Task 1: CTE to get number of orders in 2022 for Cl1, Cl2, Cl3
-- =====================================

WITH ClientOrders AS (
    SELECT ClientID, COUNT(OrderID) AS OrderCount
    FROM Orders
    WHERE YEAR(Date) = 2022 AND ClientID IN ('Cl1', 'Cl2', 'Cl3')
    GROUP BY ClientID
)
SELECT CONCAT(ClientID, ": ", OrderCount, " orders") AS "Total number of orders"
FROM ClientOrders;

-- =====================================
-- Task 2: Prepared Statement to get order details
-- =====================================

PREPARE GetOrderDetail FROM 
'SELECT OrderID, Quantity, Cost, Date 
 FROM Orders 
 WHERE ClientID = ? AND YEAR(Date) = ?';

SET @client_id = 'Cl1';
SET @order_year = 2020;

EXECUTE GetOrderDetail USING @client_id, @order_year;

DEALLOCATE PREPARE GetOrderDetail;

-- =====================================
-- Task 3: Extract product details using JSON column
-- =====================================

SELECT 
    p.ProductID, 
    p.ProductName, 
    p.BuyPrice, 
    p.SellPrice
FROM 
    Activity a
JOIN 
    Products p ON a.Properties->>'$.ProductID' = p.ProductID
WHERE 
    a.Properties->>'$.Order' = 'True';
