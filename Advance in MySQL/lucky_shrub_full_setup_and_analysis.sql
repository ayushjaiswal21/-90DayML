
-- Create database
CREATE DATABASE IF NOT EXISTS Lucky_Shrub;

-- Use the database
USE Lucky_Shrub;

-- Create tables
CREATE TABLE Clients (
    ClientID VARCHAR(10),
    FullName VARCHAR(100),
    ContactNumber INT,
    AddressID INT
);

CREATE TABLE Orders (
    OrderID INT NOT NULL PRIMARY KEY,
    ClientID VARCHAR(10),
    ProductID VARCHAR(10),
    Quantity INT,
    Cost DECIMAL(6,2),
    Date DATE
);

CREATE TABLE Products (
    ProductID VARCHAR(10),
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

-- Insert data into Clients
INSERT INTO Clients(ClientID, FullName, ContactNumber, AddressID) VALUES
("Cl1", "Takashi Ito", 351786345, 1),
("Cl2", "Jane Murphy", 351567243, 2),
("Cl3", "Laurina Delgado", 351342597, 3),
("Cl4", "Benjamin Clauss", 351342509, 4),
("Cl5", "Altay Ayhan", 351208983, 5),
("Cl6", "Greta Galkina", 351298755, 6);

-- Insert data into Orders
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

-- Insert data into Products
INSERT INTO Products (ProductID, ProductName, BuyPrice, SellPrice, NumberOfItems) VALUES
("P1", "Artificial grass bags ", 40, 50, 100),
("P2", "Wood panels", 15, 20, 250),
("P3", "Patio slates", 35, 40, 60),
("P4", "Sycamore trees ", 7, 10, 50),
("P5", "Trees and Shrubs", 35, 50, 75),
("P6", "Water fountain", 65, 80, 15);

-- Insert data into Addresses
INSERT INTO Addresses (AddressID, Street, County) VALUES
(1, ",291 Oak Wood Avenue", "Graham County"),
(2, "724 Greenway Drive", "Pinal County"),
(3, "102 Sycamore Lane", "Santa Cruz County"),
(4, "125 Roselawn Close", "Gila County"),
(5, "831 Beechwood Terrace", "Cochise County"),
(6, "755 Palm Tree Hills", "Mohave County"),
(7, "751 Waterfall Hills", "Tucson County"),
(8, "878 Riverside Lane", "Tucson County"),
(9, "908 Seaview Hills", "Tucson County"),
(10, "243 Waterview Terrace", "Tucson County"),
(11, "148 Riverview Lane", "Tucson County"),
(12, "178 Seaview Avenue", "Tucson County");

-- Task 1: Total quantity of Sycamore trees (P4) sold
SELECT
    SUM(Quantity) AS TotalSycamoreTreesSold
FROM Orders
WHERE ProductID = 'P4' AND YEAR(Date) IN (2020, 2021, 2022);

-- Task 2: Client orders in 2021 and 2022
SELECT
    c.ClientID,
    c.ContactNumber,
    a.Street,
    a.County,
    o.OrderID,
    o.Cost,
    o.Date,
    p.ProductName
FROM Clients c
JOIN Orders o ON c.ClientID = o.ClientID
JOIN Addresses a ON c.AddressID = a.AddressID
JOIN Products p ON o.ProductID = p.ProductID
WHERE YEAR(o.Date) IN (2021, 2022)
ORDER BY o.Date;

-- Task 3: Create function to find sold quantity of product by year
DELIMITER //
CREATE FUNCTION FindSoldQuantity(prod_id VARCHAR(10), order_year INT)
RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE soldQty INT;
    SELECT SUM(Quantity) INTO soldQty
    FROM Orders
    WHERE ProductID = prod_id AND YEAR(Date) = order_year;
    RETURN soldQty;
END;
//
DELIMITER ;
