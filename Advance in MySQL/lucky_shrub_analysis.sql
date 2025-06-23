
-- Lucky Shrub Database Setup and Analysis Tasks

-- 1. Create Database and Use It
CREATE DATABASE IF NOT EXISTS Lucky_Shrub;
USE Lucky_Shrub;

-- 2. Table Creation
CREATE TABLE Clients (
    ClientID VARCHAR(10) PRIMARY KEY,
    FullName VARCHAR(100),
    ContactNumber INT,
    AddressID INT
);

CREATE TABLE Products (
    ProductID VARCHAR(10) PRIMARY KEY,
    ProductName VARCHAR(100),
    BuyPrice DECIMAL(6,2),
    SellPrice DECIMAL(6,2),
    NumberOfItems INT
);

CREATE TABLE Addresses (
    AddressID INT PRIMARY KEY,
    Street VARCHAR(255),
    County VARCHAR(100)
);

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FullName VARCHAR(100),
    JobTitle VARCHAR(50),
    Department VARCHAR(200),
    AddressID INT
);

CREATE TABLE Activity (
    ActivityID INT PRIMARY KEY,
    Properties JSON
);

CREATE TABLE Audit (
    AuditID INT AUTO_INCREMENT PRIMARY KEY,
    OrderDateTime TIMESTAMP NOT NULL
);

CREATE TABLE Orders (
    OrderID INT NOT NULL PRIMARY KEY,
    ClientID VARCHAR(10),
    ProductID VARCHAR(10),
    Quantity INT,
    Cost DECIMAL(6,2),
    Date DATE,
    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

CREATE TABLE Notifications (
    NotificationID INT AUTO_INCREMENT PRIMARY KEY,
    Notification VARCHAR(256),
    DateTime TIMESTAMP NOT NULL
);

-- 4. Task 1: Function - Find Average Cost
DELIMITER //
CREATE FUNCTION FindAverageCost(year_input INT)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
  DECLARE avg_cost DECIMAL(10,2);
  SELECT AVG(Cost) INTO avg_cost FROM Orders WHERE YEAR(Date) = year_input;
  RETURN avg_cost;
END //
DELIMITER ;

-- 5. Task 2: Stored Procedure - Evaluate Product Sales
DELIMITER //
CREATE PROCEDURE EvaluateProduct(IN prod_id VARCHAR(10))
BEGIN
  DECLARE total_sold INT;
  SELECT SUM(Quantity)
  INTO total_sold
  FROM Orders
  WHERE ProductID = prod_id AND YEAR(Date) BETWEEN 2020 AND 2022;
  SELECT total_sold AS TotalQuantitySold;
END //
DELIMITER ;

-- 6. Task 3: Trigger - Insert Audit
DELIMITER //
CREATE TRIGGER UpdateAudit
AFTER INSERT ON Orders
FOR EACH ROW
BEGIN
  INSERT INTO Audit(OrderDateTime) VALUES (NOW());
END //
DELIMITER ;

-- 7. Task 4: Full Names with Addresses
SELECT
  FullName,
  Street,
  County
FROM Clients
JOIN Addresses ON Clients.AddressID = Addresses.AddressID
UNION
SELECT
  FullName,
  Street,
  County
FROM Employees
JOIN Addresses ON Employees.AddressID = Addresses.AddressID
ORDER BY Street;

-- 8. Task 5: CTE - Total Sales for Product P2
WITH YearlySales AS (
  SELECT YEAR(Date) AS SaleYear, SUM(Cost) AS Total
  FROM Orders
  WHERE ProductID = 'P2'
  GROUP BY YEAR(Date)
)
SELECT CONCAT(Total, ' (', SaleYear, ')') AS "Total sum of P2 Product"
FROM YearlySales;

-- 9. Task 6: JSON Extraction from Activity Table
SELECT
  c.FullName,
  c.ContactNumber,
  a.Properties ->> '$.ProductID' AS ProductID
FROM Activity a
JOIN Clients c
ON c.ClientID = a.Properties ->> '$.ClientID';

-- 10. Task 7: Procedure - Get Profit
DELIMITER //
CREATE PROCEDURE GetProfit(IN product_id VARCHAR(10), IN YearInput INT)
BEGIN
  DECLARE profit DECIMAL(10,2) DEFAULT 0;
  DECLARE sold_quantity INT DEFAULT 0;
  DECLARE buy_price DECIMAL(6,2);
  DECLARE sell_price DECIMAL(6,2);

  SELECT SUM(Quantity) INTO sold_quantity FROM Orders WHERE ProductID = product_id AND YEAR(Date) = YearInput;
  SELECT BuyPrice, SellPrice INTO buy_price, sell_price FROM Products WHERE ProductID = product_id;

  SET profit = (sell_price * sold_quantity) - (buy_price * sold_quantity);

  SELECT profit;
END //
DELIMITER ;

-- 11. Task 8: Virtual Table (View)
CREATE OR REPLACE VIEW DataSummary AS
SELECT
  c.FullName,
  c.ContactNumber,
  a.County,
  p.ProductName,
  o.ProductID,
  o.Cost,
  o.Date
FROM Clients c
JOIN Addresses a ON c.AddressID = a.AddressID
JOIN Orders o ON c.ClientID = o.ClientID
JOIN Products p ON o.ProductID = p.ProductID
WHERE YEAR(o.Date) = 2022
ORDER BY o.Cost DESC;
