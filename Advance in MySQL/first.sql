-- Create the database
CREATE DATABASE data_first;

-- Select the database to use
USE data_first;

-- Create the Orders table
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    ClientID VARCHAR(10),
    ProductID VARCHAR(10),
    Quantity INT,
    Cost DECIMAL(10, 2),
    Date DATE
);

-- Insert data into Orders table
INSERT INTO Orders (OrderID, ClientID, ProductID, Quantity, Cost, Date) VALUES
(1, 'CL1', 'P1', 10, 500.00, '2020-09-01'),
(2, 'CL2', 'P2', 5, 100.00, '2020-09-05'),
(3, 'CL1', 'P3', 12, 180.00, '2020-09-06'),
(4, 'CL3', 'P1', 8, 150.00, '2020-09-07'),
(5, 'CL3', 'P4', 16, 150.00, '2020-09-08'),
(6, 'CL1', 'P4', 15, 140.00, '2020-09-08'),
(7, 'CL2', 'P2', 11, 125.00, '2020-09-09'),
(8, 'CL3', 'P1', 22, 120.00, '2020-09-10'),
(9, 'CL1', 'P3', 15, 150.00, '2020-09-10'),
(10, 'CL2', 'P2', 19, 160.00, '2020-09-12');

SELECT * From Orders;
DELIMITER //

CREATE FUNCTION GetTotalCost(Cost DECIMAL(5,2)) 
RETURNS DECIMAL(5,2)
DETERMINISTIC
BEGIN
    IF (Cost >= 100 AND Cost < 500) THEN
        SET Cost = Cost - (Cost * 0.1);  -- 10% discount
    ELSE
        SET Cost = Cost - (Cost * 0.2);  -- 20% discount for all other cases
    END IF;
    RETURN Cost;
END;
//
DELIMITER ;
select GetTotalCost(500);
