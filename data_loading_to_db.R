#Testing the functions

library("readxl")
library(glue)

#Function to fill NA by MA of 24 hours average

# Specify the columns you want to fill NAs in
columns_to_fill <- c("CO", "O3", "NO2", "PM10", "SO2","PM25")  # Replace with your column names

# Function to replace NAs with the column mean
replace_na_with_mean <- function(x) {
  if (is.numeric(x)) {
    x[is.na(x)] <- mean(x, na.rm = TRUE)
  }
  return(x)
}


# Apply the function to selected columns
filled_data <- dfkiyovu %>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))



dfkiyovu <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 1) 
# Apply the function to selected columns
dfkiyovu1 <- dfkiyovu %>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))

dfkimihurura <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 2) 

dfkimihurura1 <- dfkimihurura %>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))

dfgacuriro <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 3) 

dfgacuriro1 <- dfgacuriro %>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))

dfgikomero <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 4) 

dfgikomero1 <- dfgikomero %>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))

dfMtkgl <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 5)

dfMtkgl1 <- dfMtkgl%>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))


dfJali <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 6) 

dfJali1 <- dfJali %>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))

dfrusororo <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 7) 

dfrusororo1 <- dfrusororo%>% 
  mutate(across(all_of(columns_to_fill), ~ replace_na_with_mean(.)))

dfrebero <- read_excel(here::here('data/DataWorkbook.xlsx'), sheet = 8) 



#Load the data into db
conn <- db_connect(db='postgres')
#Note: Every task loack up the db, to perform another task you run connect!!
db_data_load(conn, data = dfkiyovu1, table_name = "Kiyovu", overwrite = TRUE)

db_data_load(conn, data = dfkimihurura1, table_name = "Kimihurura", overwrite = TRUE)

db_data_load(conn, data = dfgacuriro1, table_name = "Gacuriro", overwrite = TRUE)

db_data_load(conn, data = dfgikomero1 , table_name = "Gikomero", overwrite = TRUE)

db_data_load(conn, data = dfMtkgl1 , table_name = "MtKigali", overwrite = TRUE)

db_data_load(conn, data = dfrusororo1 , table_name = "Rusororo", overwrite = TRUE)

db_data_load(conn, data = dfJali1, table_name = "Jali", overwrite = TRUE)

db_data_load(conn, data = dfrebero, table_name = "Rebero", overwrite = TRUE)

#Check the written tables
dbListTables(db_connect(db="postgres"))

#Read on table (ex: Kiyovu)
dbReadTable(conn, "Kiyovu") %>% collect()

