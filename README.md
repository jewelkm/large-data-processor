# Large-data-processor
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
  - Aggregated query for rows with `name` and `no. of products` as the columns and load them to aggregate table - 
  CREATE TABLE my_schema.aggregate SELECT name, COUNT(*) AS 'no of products' FROM my_schema.products GROUP BY name ORDER BY name;   
  
  
  
