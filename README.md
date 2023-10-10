# Hudi Basics

This code demonstrates the basics of using Apache Hudi with PySpark. It includes functions for writing data, querying
data, time travel querying, updating data, and performing incremental queries.
I created this repo to complement the https://hudi.apache.org/docs/quick-start-guide @ 0.13.1, but to run it
interactively in the IDE.

## Prerequisites

- PySpark 3.3.x or below. I used PySpark 3.3.3
- Python 3.10.x or below. I used Python 3.9.18

## Functions

To work with these functions, just add the function name under

    `if __name__ == '__main__':`

### spark basics - create_df()

This function creates a DataFrame by reading JSON data.

### spark basics - write_data()

This function writes data to the Hudi table in COW format.

### spark basics - query_data()

This function queries the Hudi table and displays the results.

### spark basics - time_travel_query()

This function demonstrates time travel querying capability by reading data at different points in time.

### spark basics - update_data()

This function generates updates and appends them to the Hudi table.

### spark basics - incremental_query()

This function performs an incremental query on the Hudi table.

### spark streaming - create_df()

This function creates a DataFrame by reading JSON data.

### spark streaming - create_table()

This function writes data to the Hudi table in COW format.

### spark streaming - query_data()

This function queries the streaming Hudi table and displays the results.

### spark streaming - streaming_write()

This function reads the streaming data and writes it to persistent storage (local in this case) and to the console.

### spark streaming - point_in_time_query()

This function shows how to query for data points within given time.

### spark streaming - soft_delete()

This function shows how to soft delete data from the Hudi table.

### spark streaming - hard_delete()

This function shows how to permanently delete data from the Hudi table.