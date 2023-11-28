#Creating a function for DB connection and checking the packages installation
#You will need the following libraries to be connected to db
#'RMySQL','DBI', 'RPostgreSQL', 'odbc'
#'--------------Check whether they installed in your PC if not install them automatically 
#' run check_installed_packages_and_load_them()
#_______________________________________________________

#1. Checking the package installation and network connection

check_installed_packages_and_load_them <- function() {
  # Define a vector of package names to check and load
  packages_to_load <- c('RMySQL','DBI', 'RPostgreSQL', 'odbc', 'glue','tidyverse')
 
  tryCatch({
    # Check and load packages
    for (pkg in packages_to_load) {
      # Check if the package is already loaded or installed
      if (!(pkg %in% installed.packages()[,"Package"])) {
        # If not loaded or installed, install it
        if (!requireNamespace(pkg, quietly = TRUE)) {
          
          install.packages(pkg)
        }
        
        library(pkg, character.only = TRUE)
        
      } else {
        # Package is already loaded, no need to install it
        library(pkg, character.only = TRUE)
      }
    }
    
    # If packages are loaded, check the connection here
    # You can add your connection checks here
    
    # If everything is fine, print the message
    cat("The required Libraries are already installed.\n")
  },
  error = function(e) {
    # Check for specific error conditions and print corresponding messages
    if (inherits(e, "packageError")) {
      cat("It is time to install them.\n")
    } else {
      cat("We encountered connection issues or system dependencies. Check the issues.\n")
    }
  })
}
# Usage example:
# Call the function to check packages and connection
#check_installed_packages_and_load_them()
#=====================================================

# 2. DB Connection function
# 
db_connect <- function(db = c("mysql_mysql", "postgres_AQKigali")[1]) {
  
  tryCatch({
    print("Connecting to Database..........")
    
    if (grepl("mysql", db)) {
      db <- gsub("mysql_", "", db)
      out <- DBI::dbConnect(
        RMySQL::MySQL(),
        host = Sys.getenv("mysql_host"),
        port =  as.integer(Sys.getenv("mysql_port")),  # Ensure port is an integer,
        user = Sys.getenv("mysql_user"),
        password = Sys.getenv("mysql_passwd"),
        dbname = db,
        timeout = 10
      )
    } else if (grepl("postgres", db)) {
      db <- gsub("postgres_", "", db)
      out <- DBI::dbConnect(
        RPostgres::Postgres(),
        user = Sys.getenv("post_user"),
        password = Sys.getenv("post_passwd"),
        host = Sys.getenv("post_host"),
        port = as.integer(Sys.getenv("post_port")),  # Ensure port is an integer
        dbname = db
      )
    } else {
      stop("Invalid database type. Please specify 'mysql' or 'postgres'.")
    }
    
    print("Database Connected!")
    return(out)
  },
  error = function(cond) {
    print("Disable to connect to Database. Plz try again ")
  })
}

# Usage example:
# To connect to MySQL, use db_connect(db = "mysql_mysql")
# To connect to PostgreSQL, use db_connect(db = "postgres_AQKigali")
#===================================================================

#3.DB write 

# Define a function to load data into a database at first time

# Define a function to load data into a database
db_data_load <- function(conn, data, table_name, overwrite = FALSE) {
  # Create a temporary log file
  tmp_name <- tempfile(fileext = ".log")
  
  # Log information about the data loading operation
  message(glue("Data loading started for {table_name} at {Sys.time()}, \n"), appendLF = FALSE)
  
  # Check if the table already exists
  if (dbExistsTable(conn, table_name) && !overwrite) {
    stop("Table already exists. Use overwrite = TRUE to replace it.")
  }
  
  # Create or overwrite the table
  if (overwrite) {
    dbExecute(conn, paste0("DROP TABLE IF EXISTS ", table_name))
  }
  
  # Determine the data source type (e.g., data frame, CSV file) and load the data
  if (is.data.frame(data)) {
    # If 'data' is a data frame, use dbWriteTable to insert it into the database
    dbWriteTable(conn, table_name, data, overwrite = overwrite)
  } else if (is.character(data) && file.exists(data)) {
    # If 'data' is a file path (e.g., CSV), load the data from the file into the database
    # Modify the following based on your database system (e.g., using COPY in PostgreSQL)
    # Example for SQLite:
    query <- paste0("CREATE TABLE ", table_name, " AS SELECT * FROM csvread('", data, "')")
    dbExecute(conn, query)
  } else {
    stop("Unsupported data source type. Provide a data frame or a valid file path.")
  }
  
  # Disconnect from the database and print a farewell message on exit
  on.exit({
    dbDisconnect(conn)
    cat("\nBye database disconnected\n")
    
    # Log information about the data loading completion
    message(glue("\n, Data loading completed for {table_name} at {Sys.time()}"))
    
    # Delete the temporary log file
    unlink(tmp_name)
  })
  
  cat("Data loaded into the database table:", table_name, "\n")
}

# Example usage:
# Replace 'your_data' with your data source (data frame or file path)
# Replace 'your_table_name' with the desired table name
# Set 'overwrite = TRUE' to replace the table if it already exists
# conn <- db_connect()  # Use db_connect to establish a database connection
# db_data_load(conn, your_data, "your_table_name", overwrite = TRUE)


#===================================================================

#4. Load multiple tables at once

# Define a list of data files and corresponding table names to load
data_to_load <- list(
  list(data = "file1.csv", table_name = "table1"),
  list(data = "file2.csv", table_name = "table2"),
  list(data = "file3.csv", table_name = "table3")
  # Add more data files and table names as needed
)

# Wrapper function to load multiple data files
load_multiple_tables <- function(conn, data_to_load) {
  
  for (item in data_to_load) {
    data_file <- item$data
    table_name <- item$table_name
    db_data_load(conn, data_file, table_name, overwrite = TRUE)
  }
  # Disconnect from the database and print a farewell message on exit
  on.exit({
    dbDisconnect(conn)
    cat("\nBye database disconnected\n") 
    
    # Log information about the data loading completion
    message(glue(" - Data loading completed for {item$table_name} at {Sys.time()}"))
    })
}

# Example to use:
# Call the wrapper function to load multiple tables
#load_multiple_tables(conn, data_to_load)
#=====================================

# 5. DB write and append the data to update table

# Define a function to append data to an existing table in the database
db_data_append <- function(conn, data, table_name) {
  # Create a temporary log file
  tmp_name <- tempfile(fileext = ".log")
  
  # Log information about the append operation
  log_info(glue("Data append started for {table_name} at {Sys.time()}"), file = tmp_name)
  
  # Check if the table exists in the database
  if (!dbExistsTable(conn, table_name)) {
    stop("Table does not exist. Cannot append data.")
  }
  
  # Append the data to the existing table
  dbWriteTable(conn, table_name, data, append = TRUE)
  
  # Disconnect from the database and print a farewell message on exit
  on.exit({
    dbDisconnect(conn)
    cat("Bye database disconnected\n")
    
    # Log information about the append operation completion
    log_info(glue("Data append completed for {table_name} at {Sys.time()}"), file = tmp_name, append = TRUE)
    
    # Delete the temporary log file
    unlink(tmp_name)
  })
  
  cat("Data appended to the database table:", table_name, "\n")
}

# Example usage:
# Replace 'your_data' with your data source (data frame)
# Replace 'your_table_name' with the existing table name in the database
# conn <- db_connect()  # Use db_connect to establish a database connection
# db_data_append(conn, your_data, "your_table_name")
#==================================================

#6. Querrying the db

# Function to perform a database query and log messages

db_query <- function(conn, SQLQuery, tbl = TRUE) {
  
  # Create a temporary log file
  
  tmp_name <- tempfile(fileext = ".log")
  
  # Log information about db connection
  cat("Connected to ", dbGetInfo(conn)$dbname, "database", "hosted on", dbGetInfo(conn)$host, "\n")
  
  # Log information about the query
  cat("SQL Query Performed:", SQLQuery, "\n")
  cat("Query started at", format(Sys.time(), format="%Y-%m-%d %H:%M:%S"), "\n")
  
  # Perform the database query
  result <- dbGetQuery(conn, SQLQuery)
  
  # Check if the query was successful
  if (!is.data.frame(result)) {
    cat("Query did not return a data frame\n")
  } else {
    # Fetch the query result as a tibble or data frame
    if (isTRUE(tbl)) {
      query_data <- result %>% as_tibble()
    } else {
      query_data <- result
    }
    
    # Check if the result is not empty
    if (nrow(query_data) > 0) {
      message(glue("Rows retrieved: {nrow(query_data)}"))
      
    } else {
      cat("Query result is empty\n")
    }
    
  }
  
  #Log information on completion of tb querry
  if (!nrow(query_data) == 0){
    cat("Query Completed at", format(Sys.time(), format="%Y-%m-%d %H:%M:%S"), "\n")
  } else { 
    cat("Query result is empty\n")}
  
  # Disconnect from the database and print a farewell message
  on.exit({
    # Disconnect from the database
    dbDisconnect(conn)
    
    # Log information about the query completion
    message(glue("Done! Database is disconnected at {Sys.time()}"))
    # Delete the temporary log file
    unlink(tmp_name)
  })
  
  # Return the query result
  return(query_data)  
}

# Example usage of the db_query function

#conn <- db_connect(db='postgres')
#SQLQuery <- 'SELECT * FROM "Gacuriro" LIMIT 10;'
#result_df <- db_query(conn, SQLQuery) 
#Or you can emmbed the row query in function for example:
#result_df <- db_query(conn, 'SELECT * FROM "Gacuriro" LIMIT 10;')

# Print the query result as a tibble
#print(result_df)


