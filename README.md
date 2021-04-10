# large-data-processor
A Python code for parallel insert of csv data to transactional database table.

## 1. Steps to run code
### Installation dependencies
  - Language used: Python v3.8.7
  - Libraries used: Pandas, Multiprocessing, SQLAlchemy, Time, Hashlib (The libraries can be installed using package installer - pip e.g. 'pip install (package name)')
  - Database used: MYSQL v8.0.23
  - MySQL Workbench(optional) v8.0.22
### Steps to run
  - Install all the dependencies in the Installation dependencies section above
  - Clone the large-data-processor repo
  - Place the products.csv file in Data folder
  - Create a schema and products table under the schema in the database
  - Provide the database and table information in the driver file.
  - Run the insert or update function in the my_driver.py file 

## 2. MySQL Database details
  - Schema: my_schema
  - Schema creation query
    CREATE SCHEMA `my_schema` ;
    
  - Table: products
  - Table creation query - 
   CREATE TABLE `my_schema`.`products` (
  `id` VARCHAR(50) NOT NULL,
  `name` VARCHAR(50) NULL,
  `sku` VARCHAR(100) NULL,
  `description` VARCHAR(1000) NULL,
  PRIMARY KEY (`id`));
  
  - Table: Aggregate
  - Aggregated query for rows with `name` and `no. of products` as the columns and load them to aggregate table
   
  CREATE TABLE my_schema.aggregate SELECT name, COUNT(*) AS 'no of products' FROM my_schema.products GROUP BY name ORDER BY name; 

## 3. Completed points in "Points to achieve"
  1. OOPS - The code follows OOPS and uses the concepts of classes, objects, dynamic attributes and Inheritance
  
  2. Regular non-blocking parallel ingestion - Achieved by Multiprocessing module. The code uses async and parallel execution to load the data to table.
     The load of 500,000 rows in products.csv is taking an average of 43 ± 3 secs
     
  3. Updation of existing products in the table based on `sku` as the primary key - The code allows for updation of the name, sku and description 
     based on one or all of the three columns.
     Total no of tables in the database - 2 (products, aggregate)
     
     No of rows in products table - 500,000
     
     No of rows in aggregate table - 222,024
     
     Products table sample
     <img width="1162" alt="Screenshot 2021-04-10 at 9 47 58 PM" src="https://user-images.githubusercontent.com/66643226/114276993-cfd28280-9a46-11eb-869f-157956dec6c7.png">
     
     Aggregate table sample
    
     <img width="210" alt="Screenshot 2021-04-10 at 9 48 53 PM" src="https://user-images.githubusercontent.com/66643226/114277016-f7c1e600-9a46-11eb-86fa-6395e9b95ad6.png">
       
  4. All product details are ingested into a single table
  
  5. Created aggregated query for rows with `name` and `no. of products` as the columns and loaded them to aggregate table
  
## 4. Points not done from "Points to achieve"
      None

## 5. Improvement that can be made with time:
  - The core logic of the code relies on hash values to create a unique id which is the primary key in the table. This had to be done since name and sku columns had duplicates.
    A primary key was needed to avoid duplicate inserts when the code is run for the same file multiple times. 
    The collision probability of md5 hash used is 1.47*10-29. A logic may be needed to handle the collision when the data grows.
    
  - Another logic will be to check if the data already exists in the table for each insert. 
    This was slowing down the code because of the additional I/O involved. 
    I would like to implement this logic if given time and see how to improve the efficiency.
    
  - The code can be made more generic to handle any type of file or database insert. Currently the code works for csv file to table.
  
  - Support the code for NoSQL databases as well.
  
  
  
  
  
  
