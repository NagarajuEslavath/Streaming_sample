CREATE DATABASE InventoryDB;
GO

USE InventoryDB;
GO

CREATE TABLE inventory(
    id INT PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    last_updated DATETIME DEFAULT GETDATE()
);
GO
