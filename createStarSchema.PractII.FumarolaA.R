# createStarSchema
# Andrew Fumarola
# CS5200 Summer 2025
# Practicum II

# package install method: 
# https://statsandr.com/blog/an-efficient-way-to-install-and-load-r-packages/
packages <- c("RMySQL")
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}
invisible(lapply(packages, library, character.only = TRUE))

# Part B-2: connect to Aiven cloud-hosted database
db_user <- 'avnadmin' 
db_password <- '***'
db_name <- 'defaultdb'
db_host <- 'mysql-376f2d16-andrewfumarola-cs5200.f.aivencloud.com'
db_port <- 26836
mydb <-  dbConnect(RMySQL::MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

# Part B-3: create fact table(s)

# The goal is to create two star schemas with their own separate fact table
# FactSales will address temporal and spatial revenue and sales data
# SummarySales will pre-calculate quarterly and yearly revenue averages
# FactCustomers will address customer habits

# Star Schema 1: FactSales

dbExecute(mydb, "DROP TABLE IF EXISTS FactCustomers")
dbExecute(mydb, "DROP TABLE IF EXISTS FactSales")
dbExecute(mydb, "DROP TABLE IF EXISTS SummarySales")
dbExecute(mydb, "DROP TABLE IF EXISTS DimDate")
dbExecute(mydb, "DROP TABLE IF EXISTS DimCountry")
dbExecute(mydb, "DROP TABLE IF EXISTS DimType")

dbExecute(mydb, "CREATE TABLE IF NOT EXISTS DimDate (
          date_id INT PRIMARY KEY,
          month INT NOT NULL,
          quarter INT NOT NULL,
          year INT NOT NULL
          );")

dbExecute(mydb, "CREATE TABLE IF NOT EXISTS DimCountry (
          country_id INT PRIMARY KEY AUTO_INCREMENT,
          country_name VARCHAR(64) NOT NULL
          );")

dbExecute(mydb, "CREATE TABLE IF NOT EXISTS DimType (
          type_id INT PRIMARY KEY,
          type_name VARCHAR(16)
          );")

dbExecute(mydb, "CREATE TABLE IF NOT EXISTS FactSales (
          transaction_id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
          date_id INT NOT NULL,
          country_id INT NOT NULL,
          type_id INT NOT NULL,
          revenue DECIMAL(10,2) NOT NULL,
          FOREIGN KEY (date_id) REFERENCES DimDate(date_id),
          FOREIGN KEY (country_id) REFERENCES DimCountry(country_id),
          FOREIGN KEY (type_id) REFERENCES DimType(type_id)
          );")

dbExecute(mydb, "CREATE TABLE IF NOT EXISTS SummarySales (
          country VARCHAR(64) NOT NULL,
          year INT NOT NULL,
          quarter INT NOT NULL,
          total_revenue DECIMAL(10,2),
          PRIMARY KEY (country, year, quarter)
          );")

# Star Schema 2: FactCustomer:
# Will reference the same dim tables as above for FactSales

dbExecute(mydb, "CREATE TABLE IF NOT EXISTS FactCustomers (
          customer_id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
          country_id INT NOT NULL,
          type_id INT NOT NULL,
          FOREIGN KEY (country_id) REFERENCES DimCountry(country_id),
          FOREIGN KEY (type_id) REFERENCES DimType(type_id)
          );")

dbDisconnect(mydb)
