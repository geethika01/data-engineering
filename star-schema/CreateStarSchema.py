# -*- coding: utf-8 -*-
"""
Title: Create Fact and Dimention tables using start schema in Postgre database for a 
music store
Details:
    Fact Table: customer_transactions(customer_id int, store_id int, spent numeric(4,2))
    Dimension Tables: 
        customer(customer_id int, name varchar, rewards char(1))
        items_purchased (customer_id int, item_number int, item_name varchar)
        store (store_id int, state char(3))
    
    
Created on Thu Feb 18 22:39:48 2021

@author: Geethika.Wijewardena
------------------------------------------------------------------------------
"""

import psycopg2

#----------------------------------------------------------------------------
# Create connection to the database

try:
    conn = psycopg2.connect("host=192.168.193.159 dbname=studentdb\
                            user=geethika password=*******")
except psycopg2.Error as e:
    print("Error: Could not connect to the database")
    print(e)
        
# Get a cursor to execute queries

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the database")
    print(e)
    
# Set Automatic commit to be true
conn.set_session(autocommit=True)

#----------------------------------------------------------------------------
# Create Fact Table

try:
    cur.execute("CREATE TABLE IF NOT EXISTS customer_transactions \
                (customer_id int, store_id int, spent numeric(4,2),\
                PRIMARY KEY (customer_id, store_id))")
except psycopg2.Error as e:
    print("Issue creating Fact table")
    print(e)
    
# Insert data into Fact Table
    
try:
    cur.execute("INSERT INTO customer_transactions \
                (customer_id, store_id, spent) \
                VALUES(%s, %s, %s)",\
                (1,1, 20.50)) 
except psycopg2.Error as e:
    print("Error: Could not insert data")
    print(e)

try:
    cur.execute("INSERT INTO customer_transactions \
                (customer_id, store_id, spent) \
                VALUES(%s, %s, %s)",\
                (2,1, 35.21)) 
except psycopg2.Error as e:
    print("Error: Could not insert data")
    print(e)                

# Check for the rows
try:
    cur.execute("SELECT * FROM customer_transactions") 
except psycopg2.Error as e:
    print("Error: Could not read data in the customer_transactions table")
    print(e)         

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

#-----------------------------------------------------------------------------
# Create Dimension tables

# customer table
try:
    cur.execute("CREATE TABLE IF NOT EXISTS customer \
                (customer_id int PRIMARY KEY, name varchar, rewards char(1))")
except psycopg2.Error as e:
    print ("Error: Could not create customer table")
    print (e)

# store table    
try:
    cur.execute("CREATE TABLE IF NOT EXISTS store \
                (store_id int PRIMARY KEY, state char(2))")
except psycopg2.Error as e:
    print ("Error: Could not create store table")
    print (e)    
        
# items_purchased table
try:
    cur.execute("CREATE TABLE IF NOT EXISTS items_purchased \
                (customer_id int, item_number int, item_name varchar)")
               
except psycopg2.Error as e:
    print ("Error: Could not create items_purchased table")
    print (e)
    
#-----------------------------------------
# Insert data into Dimension tables
# Customer table
    
try:
    cur.execute("INSERT INTO customer \
                (customer_id, name, rewards) \
                VALUES(%s, %s, %s)",\
                (1, 'Amanda', 'Y'))     
except psycopg2.Error as e:
    print("Error: Could not insert data into customer table")
    print(e)

try:
    cur.execute("INSERT INTO customer \
                (customer_id, name, rewards) \
                VALUES(%s, %s, %s)",\
                (2, 'Toby', 'N'))     
except psycopg2.Error as e:
    print("Error: Could not insert data into customer table")
    print(e)
    
try:
    cur.execute("SELECT * FROM customer") 
except psycopg2.Error as e:
    print("Error: Could not read data in the customer table")
    print(e)         

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

#----------------------------------------------    
# Store table
try:
    cur.execute("INSERT INTO store (store_id, state) \
                 VALUES(%s, %s)",\
                (1, "CA"))
except psycopg2.Error as e:
    print ("Error: Could not insert data into store table")
    print (e)

try:
    cur.execute("INSERT INTO store (store_id, state) \
                 VALUES(%s, %s)",\
                (2, "WA"))
except psycopg2.Error as e:
    print ("Error: Could not insert data into store table")
    print (e)

try:
    cur.execute("SELECT * FROM store") 
except psycopg2.Error as e:
    print("Error: Could not read data in the store table")
    print(e)         

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

#----------------------------------------------
# Items purchased

try:
    cur.execute("INSERT INTO items_purchased \
                (customer_id, item_number, item_name) \
                VALUES(%s, %s, %s)",\
                (1, 1, 'Rubber Soul'))     
except psycopg2.Error as e:
    print("Error: Could not insert data into items_purchased table")
    print(e)

try:
    cur.execute("INSERT INTO items_purchased \
                (customer_id, item_number, item_name) \
                VALUES(%s, %s, %s)",\
                (2, 3, 'Let It Be'))     
except psycopg2.Error as e:
    print("Error: Could not insert data into items_purchased table")
    print(e)

try:
    cur.execute("SELECT * FROM items_purchased") 
except psycopg2.Error as e:
    print("Error: Could not read data in the items_purchased table")
    print(e)         

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()
    
#------------------------------------------------------------------------------
# Queries
# 1.  Find all the customers that spent more than 30 dollars, who are they, 
# which store they bought it from, location of the store, what they bought and 
# if they are a rewards member.
    
try:
    cur.execute("SELECT customer.name, store.store_id, store.state,\
                items_purchased.item_name, customer.rewards \
                FROM (((customer_transactions \
                JOIN customer ON customer_transactions.customer_id = customer.customer_id) \
                JOIN store ON customer_transactions.store_id = store.store_id)\
                JOIN items_purchased ON customer_transactions.customer_id = items_purchased.customer_id) \
                WHERE spent > 30.00;")
except psycopg2.Error as e:
    print("Error: cannot execute query 1")
    print (e)


row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

#-----------------------------------------    
# 2. How much did Customer 2 spend?

try:
    cur.execute("SELECT customer.name, sum(customer_transactions.spent)\
                FROM (customer_transactions \
                JOIN customer ON customer_transactions.customer_id = customer.customer_id) \
                WHERE customer.customer_id = 2 \
                GROUP BY customer.name;")
except psycopg2.Error as e:
    print("Error: cannot execute query 1")
    print (e)


row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()
    
#----------------------------------------------------------------------------
# Update Customer table - Try to update row where on conflict do nothing

try:
    cur.execute("INSERT INTO customer (customer_id, name, rewards) \
                VALUES(%s, %s, %s) \
                ON CONFLICT (customer_id) \
                DO NOTHING;", (1, "Amanda", "N"))
except psycopg2.Error as e:
    print("Error: when inserting new record")
    print (e)
    
try:
    cur.execute("SELECT * FROM customer")
except psycopg2.Error as e:
    print("Error: cannot execute select * for customer table")
    print (e)

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()
    
# Update Customer table - Try to update row where on conflict do update

try:
    cur.execute("INSERT INTO customer (customer_id, name, rewards) \
                VALUES(%s, %s, %s) \
                ON CONFLICT (customer_id) \
                DO UPDATE \
                    SET rewards = 'N'; " , (1, "Amanda", "Y"))
    
except psycopg2.Error as e:
    print("Error: when inserting new record")
    print (e)
    
try:
    cur.execute("SELECT * FROM customer")
except psycopg2.Error as e:
    print("Error: cannot execute select * for customer table")
    print (e)

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

#----------------------------------------------------------------------    
# Drop tables
table_names = ["customer_transactions", "customer", "store", "items_purchased"]

for tbl in table_names:
    try:
        cur.execute("DROP TABLE " + tbl)
    except psycopg2.Error as e:
        print("Error dropping table " + tbl)
        print (e)
