-- 1. Setup the database
DROP DATABASE IF EXISTS Lucky_Shrub;
CREATE DATABASE Lucky_Shrub;
USE Lucky_Shrub;

-- 2. Create the Orders table
CREATE TABLE IF NOT EXISTS Orders (
    OrderID INT NOT NULL PRIMARY KEY,
    ClientID VARCHAR(10),
    ProductID VARCHAR(10),
    Quantity INT,
    Cost DECIMAL(6,2),
    Date DATE
);

-- 3. Insert data
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

-- 4. Create the GetOrderCost function
DELIMITER //

CREATE FUNCTION GetOrderCost(Order_Id INT)
RETURNS DECIMAL(6,2)
DETERMINISTIC
BEGIN
    DECLARE orderCost DECIMAL(6,2);

    SELECT Cost INTO orderCost
    FROM Orders
    WHERE OrderID = Order_Id;

    RETURN orderCost;
END;
//

DELIMITER ;

-- 5. Create the GetDiscount stored procedure
DELIMITER //

CREATE PROCEDURE GetDiscount(IN Order_Id INT)
BEGIN
    DECLARE originalCost DECIMAL(6,2);
    DECLARE quantity INT;
    DECLARE finalCost DECIMAL(6,2);

    SELECT Quantity, Cost INTO quantity, originalCost
    FROM Orders
    WHERE OrderID = Order_Id;

    IF quantity >= 20 THEN
        SET finalCost = originalCost * 0.80; -- 20% discount
    ELSEIF quantity >= 10 THEN
        SET finalCost = originalCost * 0.90; -- 10% discount
    ELSE
        SET finalCost = originalCost; -- No discount
    END IF;

    SELECT finalCost AS Discounted_Cost;
END;
//

DELIMITER ;

-- 6. Test the function
SELECT GetOrderCost(5) AS Cost;

-- 7. Test the procedure
CALL GetDiscount(5);
