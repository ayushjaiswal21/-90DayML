
-- Step 1: Create the database
CREATE DATABASE Lucky_Shrub;

-- Step 2: Use the database
USE Lucky_Shrub;

-- Step 3: Create the Orders table
CREATE TABLE Orders (
    OrderID INT NOT NULL,
    ClientID VARCHAR(10) DEFAULT NULL,
    ProductID VARCHAR(10) DEFAULT NULL,
    Quantity INT DEFAULT NULL,
    Cost DECIMAL(6,2) DEFAULT NULL,
    Date DATE DEFAULT NULL,
    PRIMARY KEY (OrderID)
);

-- Step 4: Create the Employees table
CREATE TABLE Employees (
    EmployeeID INT DEFAULT NULL,
    FullName VARCHAR(100) DEFAULT NULL,
    Role VARCHAR(50) DEFAULT NULL,
    Department VARCHAR(255) DEFAULT NULL
);

-- Step 5: Insert data into Orders
INSERT INTO Orders (OrderID, ClientID, ProductID , Quantity, Cost, Date)  
VALUES  
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
(11, "Cl1", "P2", 15, 80, "2020-09-12"), 
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
(29, "Cl3", "P3", 20, 800, "2021-09-03");

-- Step 6: Insert data into Employees
INSERT INTO Employees (EmployeeID, FullName, Role, Department)  
VALUES    
(1, "Seamus Hogan", "Manager", "Management"),    
(2, "Thomas Eriksson", "Assistant ", "Sales"),   
(3, "Simon Tolo", "Executive", "Management"),   
(4, "Francesca Soffia", "Assistant  ", "Human Resources"),   
(5, "Emily Sierra", "Accountant", "Finance"),    
(6, "Greta Galkina", "Accountant", "Finance"), 
(7, "Maria Carter", "Executive", "Human Resources"), 
(8, "Rick Griffin", "Manager", "Marketing");

-- Task 1: Optimized SELECT query
SELECT OrderID, ProductID, Quantity, Date FROM Orders;

-- Task 2: Create index and optimize query
CREATE INDEX IdxClientID ON Orders(ClientID);
EXPLAIN SELECT * FROM Orders WHERE ClientID = 'Cl1';

-- Task 3: Optimize LIKE search using reversed full name
ALTER TABLE Employees ADD ReverseFullName VARCHAR(100);
UPDATE Employees SET ReverseFullName = REVERSE(FullName);
CREATE INDEX IdxReverseFullName ON Employees(ReverseFullName);
SELECT * FROM Employees WHERE ReverseFullName LIKE 'oloT%';
