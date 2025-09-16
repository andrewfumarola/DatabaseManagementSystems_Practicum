# loadAnalyticsDB
# Andrew Fumarola
# CS5200 Summer 2025
# Practicum II

# package install method: 
# https://statsandr.com/blog/an-efficient-way-to-install-and-load-r-packages/
packages <- c("RMySQL", "DBI")
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}
invisible(lapply(packages, library, character.only = TRUE))

# Part B-2: connect to Aiven cloud-hosted database
db_user <- 'avnadmin' 
db_password <- 'AVNS_2RI4V25IYkODzXuVRMT'
db_name <- 'defaultdb'
db_host <- 'mysql-376f2d16-andrewfumarola-cs5200.f.aivencloud.com'
db_port <- 26836
mydb <-  dbConnect(RMySQL::MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

suppressWarnings({
# Connect to sqlite databases
filmdb <- dbConnect(RSQLite::SQLite(), 'film-sales.db')
musicdb <- dbConnect(RSQLite::SQLite(), 'music-sales.db')

# DIM tables -------------
# DimDate:

# Create sequence of applicable dates, turn into dataframe
begin <- as.Date("2005-01-01")
end <- as.Date("2025-01-01")
s <- seq(begin, end, by = "day")
date_id <- as.integer(format(s, "%Y%m%d"))
year <- as.integer(format(s, "%Y"))
month <- as.integer(format(s, "%m"))
quarter <- as.integer((month - 1) %/% 3 + 1) # ChatGPT provided this formula

dateDF <- data.frame(date_id, month, quarter, year)

# Batch upload to DimDate table
batch_size  <- 100
n <- nrow(dateDF)
for (start in seq(1, n, by = batch_size)) {
  end <- min(start + batch_size - 1, n)
  values_vec <- character()
   for (i in start:end) {
    dID <- date_id[i]
    y <- year[i]
    m <- month[i]
    q <- quarter[i]
    # build one row of values string for SQL
    values_vec <- c(values_vec, 
                    paste0("(", paste(c(dID, m, q, y), collapse = ", "), ")"))
  }
  # combine all values into one insert statement
  query <- paste0(
    "INSERT INTO DimDate (date_id, month, quarter, year) VALUES ",
    paste(values_vec, collapse = ", "),
    ";"
  )
  dbExecute(mydb, query)
}

# DimCountry
# Get all countries in both sqlite databases and get their union
# Create dimcountry with list of unique countries

countryDF_film <- dbGetQuery(filmdb, "SELECT DISTINCT country 
                              FROM Country")
countryDF_music <- dbGetQuery(musicdb, "SELECT DISTINCT country
                              FROM customers")
colnames(countryDF_film) <- colnames(countryDF_music)
countryDF <- unique(rbind(countryDF_film, countryDF_music))

batch_size  <- 100
n <- nrow(countryDF)
for (start in seq(1, n, by = batch_size)) {
  end <- min(start + batch_size - 1, n)
  values_vec <- character()
  for (i in start:end) {
    c <- countryDF$Country[i]
    n_escaped <- gsub("'", "''", c)
    # build one row of values string for SQL
    values_vec <- c(values_vec, paste0("('", n_escaped, "')"))
  }
  # combine all values into one insert statement
  query <- paste0(
    "INSERT INTO DimCountry (country_name) VALUES ",
    paste(values_vec, collapse = ", "),
    ";"
  )
  dbExecute(mydb, query)
}

dbGetQuery(mydb, "SELECT * FROM DimCountry")

# DimType

query <- "INSERT INTO DimType (type_id, type_name) 
          VALUES (1, 'film')"
dbExecute(mydb, query)
query <- "INSERT INTO DimType (type_id, type_name) 
          VALUES (2, 'music')"
dbExecute(mydb, query)


# FACT TABLES -----------------
# FactSales

# Load necessary information from each sqlite db and load to fact table
# film DB

film_raw <- dbGetQuery(filmdb, "SELECT p.payment_date, co. country, p.amount
                    FROM payment p
                    JOIN customer cu on cu.customer_id = p.customer_id
                    JOIN address a on a.address_id = cu.address_id
                    JOIN city ci on ci.city_id = a.city_id
                    JOIN country co on co.country_id = ci.country_id")

film_raw$payment_date <- paste0(substr(film_raw$payment_date, 1,4),
                                substr(film_raw$payment_date, 6,7),
                                substr(film_raw$payment_date, 9,10))

dimCountryDF <- dbGetQuery(mydb, "SELECT * FROM DimCountry")

film_raw$country <- dimCountryDF$country_id[match(film_raw$country, 
                                                dimCountryDF$country_name)]
batch_size  <- 100
n <- nrow(film_raw)
for (start in seq(1, n, by = batch_size)) {
  end <- min(start + batch_size - 1, n)
  values_vec <- character()
  for (i in start:end) {
    dID <- film_raw$payment_date[i]
    c <- film_raw$country[i]
    r <- film_raw$amount[i]
    # build one row of values string for SQL
    values_vec <- c(values_vec, 
                    paste0("(", paste(c(dID, c, "1", r), collapse = ", "), ")"))
  }
  # combine all values into one insert statement
  query <- paste0(
    "INSERT INTO FactSales (date_id, country_id, type_id, revenue) VALUES ",
    paste(values_vec, collapse = ", "),
    ";"
  )
  dbExecute(mydb, query)
}

# music DB

music_raw <- dbGetQuery(musicdb, "SELECT i.InvoiceDate, ii.UnitPrice, c.Country
                    FROM Invoice_items ii
                    JOIN Invoices i on i.invoiceID = ii.invoiceID
                    JOIN Customers c on c.customerID = i.customerID")

music_raw$InvoiceDate <- paste0(substr(music_raw$InvoiceDate, 1,4),
                                substr(music_raw$InvoiceDate, 6,7),
                                substr(music_raw$InvoiceDate, 9,10))

music_raw$Country <- dimCountryDF$country_id[match(music_raw$Country, 
                                                  dimCountryDF$country_name)]

batch_size  <- 100
n <- nrow(music_raw)
for (start in seq(1, n, by = batch_size)) {
  end <- min(start + batch_size - 1, n)
  values_vec <- character()
  for (i in start:end) {
    dID <- music_raw$InvoiceDate[i]
    c <- music_raw$Country[i]
    r <- music_raw$UnitPrice[i]
    # build one row of values string for SQL
    values_vec <- c(values_vec, 
                    paste0("(", paste(c(dID, c, "2", r), collapse = ", "), ")"))
  }
  # combine all values into one insert statement
  query <- paste0(
    "INSERT INTO FactSales (date_id, country_id, type_id, revenue) VALUES ",
    paste(values_vec, collapse = ", "),
    ";"
  )
  dbExecute(mydb, query)
}

# SummarySales
# This will use information from FactSales to pre-calculate revenue by quarter

dbExecute(mydb, 
          "INSERT INTO SummarySales (country, year, quarter, total_revenue)
          SELECT 
            CASE
              WHEN dc.country_name IN ('USA', 'United States') THEN 'United States'
              ELSE dc.country_name
            END AS country,
              d.year, d.quarter, SUM(f.revenue) AS tot_rev
          FROM FactSales f
          JOIN DimDate d ON f.date_id = d.date_id
          JOIN DimCountry dc ON dc.country_id = f.country_id
          GROUP BY country, d.year, d.quarter
          ;")

# FactCustomer

# film db

film_raw_c <- dbGetQuery(filmdb, "SELECT cu.customer_id, co.country
                                  FROM Customer cu
                                  JOIN Address a ON a.address_id = cu.address_id
                                  JOIN City ci ON ci.city_id = a.city_id
                                  JOIN Country co 
                                    ON co.country_id = ci.country_id")

film_raw_c$country <- dimCountryDF$country_id[match(film_raw_c$country, 
                                                  dimCountryDF$country_name)]
batch_size  <- 100
n <- nrow(film_raw_c)
for (start in seq(1, n, by = batch_size)) {
  end <- min(start + batch_size - 1, n)
  values_vec <- character()
  for (i in start:end) {
    c <- film_raw_c$country[i]
    # build one row of values string for SQL
    values_vec <- c(values_vec, 
                    paste0("(", paste(c(c, "1"), collapse = ", "), ")"))
  }
  # combine all values into one insert statement
  query <- paste0(
    "INSERT INTO FactCustomers (country_id, type_id) VALUES ",
    paste(values_vec, collapse = ", "),
    ";"
  )
  dbExecute(mydb, query)
}

# music db

music_raw_c <- dbGetQuery(musicdb, "SELECT CustomerID, country
                                    FROM customers
                                    ;")

music_raw_c$Country <- dimCountryDF$country_id[match(music_raw_c$Country, 
                                                    dimCountryDF$country_name)]
batch_size  <- 100
n <- nrow(music_raw_c)
for (start in seq(1, n, by = batch_size)) {
  end <- min(start + batch_size - 1, n)
  values_vec <- character()
  for (i in start:end) {
    c <- music_raw_c$Country[i]
    # build one row of values string for SQL
    values_vec <- c(values_vec, 
                    paste0("(", paste(c(c, "2"), collapse = ", "), ")"))
  }
  # combine all values into one insert statement
  query <- paste0(
    "INSERT INTO FactCustomers (country_id, type_id) VALUES ",
    paste(values_vec, collapse = ", "),
    ";"
  )
  dbExecute(mydb, query)
}

# Testing Cloud Database --------------------

success_test = 0 # This will count number of successful tests
total_test = 0

# Comparing customer statistics by country (film type)
n_US_filmdb <- dbGetQuery(filmdb, "SELECT count(*)
                           FROM Customer cu
                           JOIN Address a ON a.address_id = cu.address_id
                           JOIN City ci ON ci.city_id = a.city_id
                           JOIN Country co 
                                ON co.country_id = ci.country_id
                           WHERE co.country = 'United States';")[[1]]

n_US_mydb <- dbGetQuery(mydb, "SELECT count(*) FROM FactCustomers f
                            JOIN DimCountry d on d.country_id = f.country_id
                            WHERE d.country_name = 'United States'
                            AND f.type_id = 1")[[1]]


print("Checking count of film customers from 'United States' matches new db")
if(n_US_filmdb == n_US_mydb) {
  print("Success")
  success_test = success_test + 1
}else print("ERROR in FactCustomers")
total_test = total_test + 1

# Comparing customer statistics by country (music type)

n_US_musicdb <- dbGetQuery(musicdb, "SELECT count(*)
                            FROM customers 
                            WHERE country = 'USA';")[[1]]

n_US_mydb <- dbGetQuery(mydb, "SELECT count(*) FROM FactCustomers f
                            JOIN DimCountry d on d.country_id = f.country_id
                            WHERE d.country_name = 'USA'
                            AND f.type_id = 2")[[1]]


print("Checking count of music customers from 'USA' matches new db")
if(n_US_musicdb == n_US_mydb) {
  print("Success")
  success_test = success_test + 1
}else print("ERROR in FactCustomers")
total_test = total_test + 1

# Comparing total amount of sales (film)

total_sales_film <- dbGetQuery(filmdb, "SELECT SUM(amount)
                                        FROM payment")[[1]]

total_sales_film_mydb <- dbGetQuery(mydb, "SELECT SUM(revenue)
                                          FROM FactSales
                                          WHERE type_id = 1")[[1]]

print("Checking revenue from film db matches FactSales table")
if(round(total_sales_film, 2) == round(total_sales_film_mydb, 2)) {
  print("Success")
  success_test = success_test + 1
}else print("ERROR in FactSales")
total_test = total_test + 1

# Comparing total amount of sales (music)

total_sales_music <- dbGetQuery(musicdb, "SELECT SUM(unitprice)
                                        FROM Invoice_items")[[1]]

total_sales_music_mydb <- dbGetQuery(mydb, "SELECT SUM(revenue)
                                          FROM FactSales
                                          WHERE type_id = 2")[[1]]

print("Checking revenue from music db matches FactSales table")
if(round(total_sales_music, 2) == round(total_sales_music_mydb, 2)) {
  print("Success")
  success_test = success_test + 1
}else print("ERROR in FactSales")
total_test = total_test + 1

# Test Summary
print(paste0("Ran ", total_test, " unique tests"))
print(paste0(success_test, " were successful"))
print(paste0("Success Rate = ", (success_test*100/total_test), "%"))

dbDisconnect(mydb)
})
