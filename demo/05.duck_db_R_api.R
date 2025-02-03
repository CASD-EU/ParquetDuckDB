# install.packages("/home/pliu/r_packages/duckdb_1.1.3.tar.gz", repos = NULL, type = "binary")


library("duckdb")
# to start an in-memory database
con <- dbConnect(duckdb())

# create a table
dbExecute(con, "CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)")
# insert two items into the table
dbExecute(con, "INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")

# retrieve the items again
res <- dbGetQuery(con, "SELECT * FROM items")
print(res)