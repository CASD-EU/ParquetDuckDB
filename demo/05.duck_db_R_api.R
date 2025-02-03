# install.packages("/home/pliu/r_packages/duckdb_1.1.3.tar.gz", repos = NULL, type = "binary")


library("duckdb")
# to start an in-memory database
con <- dbConnect(duckdb())

# create a table
dbExecute(con, "CREATE TABLE sample_table (item VARCHAR, value DECIMAL(10, 2), count INTEGER)")
# insert two items into the table
dbExecute(con, "INSERT INTO sample_table VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")

# retrieve the items again
res <- dbGetQuery(con, "SELECT * FROM sample_table")

print(res)

# read the parquet file
table_name <- "fr_immo"

# Construct and execute the query to create the table from the Parquet file
query <- paste("CREATE TABLE", table_name, "AS SELECT * FROM parquet_scan('/home/pliu/git/ParquetDuckDB/data/fr_immo_transactions.parquet')", sep = " ")

# Execute the query
dbExecute(con, query)

# retrieve the items again
fr_immo_df <- dbGetQuery(con, "SELECT * FROM fr_immo;")


# count column and row number
column_count <- ncol(fr_immo_df)
row_count <- nrow(fr_immo_df)

cat("Number of columns:", column_count, "\n")
cat("Number of rows:", row_count, "\n")

print(head(fr_immo_df, 5))

# disconnect the duckdb
dbDisconnect(con)