import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')

# Step 1
q = """
SELECT firstName, lastName
FROM employees
JOIN offices
    USING(officeCode)
WHERE city = 'Boston'
;
"""
pd.read_sql(q, conn)
# print(pd.read_sql(q, conn))

# Step 2
# Note that COUNT(*) is not appropriate here because
# we are trying to count the _employees_ in each group.
# So instead we count by some attribute of an employee
# record. The primary key (employeeNumber) is a 
# conventional way to do this
q = """
SELECT
    o.officeCode,
    o.city,
    COUNT(e.employeeNumber) AS n_employees
FROM offices AS o
LEFT JOIN employees AS e
    USING(officeCode)
GROUP BY officeCode
HAVING n_employees = 0
;
"""
pd.read_sql(q, conn)
# print(pd.read_sql(q, conn))

# Step 3
q = """
SELECT
    o.officeCode,
    o.city,
    COUNT(c.customerNumber) AS n_customers
FROM offices AS o
JOIN employees AS e
    USING(officeCode)
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY officeCode;
"""
pd.read_sql(q, conn)
# print(pd.read_sql(q, conn))

# Step 4
# We don't need to use aliases for the columns since they
# are conveniently already labeled as different kinds of
# names (firstName, lastName, productName)
q = """
SELECT firstName, lastName, productName
FROM employees AS e
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders
    USING(customerNumber)
JOIN orderdetails
    USING(orderNumber)
JOIN products
    USING(productCode)
;
"""
pd.read_sql(q, conn)
# print(pd.read_sql(q, conn))

# Step 5
q = """
SELECT firstName, lastName, SUM(quantityOrdered) as total_products_sold
FROM employees AS e
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders
    USING(customerNumber)
JOIN orderdetails
    USING(orderNumber)
GROUP BY firstName, lastName
ORDER BY lastName
;
"""
pd.read_sql(q, conn)
# print(pd.read_sql(q, conn))

# Step 6
# Recall that HAVING is used for filtering after an aggregation
q = """
SELECT firstName, lastName, COUNT(productCode) as different_products_sold
FROM employees AS e
JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders
    USING(customerNumber)
JOIN orderdetails
    USING(orderNumber)
GROUP BY firstName, lastName
HAVING different_products_sold > 200
ORDER BY lastName
;
"""
pd.read_sql(q, conn)
print(pd.read_sql(q, conn))

conn.close()