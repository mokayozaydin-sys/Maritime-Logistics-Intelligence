-- ==========================================================
-- LOGISTICS DATA WAREHOUSE SCHEMA & ANALYSIS QUERIES
-- Author: [Adın Soyadın]
-- Field: Industrial Engineering - Logistics Analytics
-- ==========================================================

-- 1. DATABASE STRUCTURE (DDL)
-- Creating relational tables with integrity constraints

-- Port Information Table
CREATE TABLE IF NOT EXISTS Ports (
    port_id INTEGER PRIMARY KEY,
    port_name TEXT NOT NULL,
    country TEXT NOT NULL
);

-- Main Shipments Table
CREATE TABLE IF NOT EXISTS Shipments (
    shipment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    origin_port_id INTEGER,
    dest_port_id INTEGER,
    departure_date TEXT, -- Stored in ISO 8601 format (YYYY-MM-DD)
    arrival_date TEXT,
    status TEXT,
    FOREIGN KEY (origin_port_id) REFERENCES Ports (port_id),
    FOREIGN KEY (dest_port_id) REFERENCES Ports (port_id)
);

-- Operational Costs Table
CREATE TABLE IF NOT EXISTS Costs (
    cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
    shipment_id INTEGER,
    freight_cost REAL,
    custom_fee REAL,
    inland_transport REAL,
    FOREIGN KEY (shipment_id) REFERENCES Shipments (shipment_id)
);

-- 2. OPERATIONAL KPI ANALYSIS (DQL)
-- This query calculates Average Lead Time and Total Cost per Origin Port.
-- It demonstrates the use of JOINs, Aggregate Functions, and Date Arithmetic.

SELECT 
    p.port_name AS Origin_Port,
    COUNT(s.shipment_id) AS Total_Shipments,
    -- Lead Time Calculation: Difference between Arrival and Departure
    ROUND(AVG(JULIANDAY(s.arrival_date) - JULIANDAY(s.departure_date)), 2) AS Avg_Lead_Time_Days,
    -- Total Cost Calculation: Sum of Freight, Customs, and Inland charges
    ROUND(AVG(c.freight_cost + c.custom_fee + c.inland_transport), 2) AS Avg_Total_Cost_USD
FROM Shipments s
JOIN Ports p ON s.origin_port_id = p.port_id
JOIN Costs c ON s.shipment_id = c.shipment_id
GROUP BY p.port_name
ORDER BY Avg_Total_Cost_USD DESC;
