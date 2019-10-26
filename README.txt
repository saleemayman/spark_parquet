# input data
All CSV data to be loaded to PostgreSQL is located in the data/ folder with in the root folder.

# compiling
Use:
    'sbt clean compile'

# running
Use:
    'sbt run "<DB Name> <DB user Name> <User Pass> <Server IP> <Port> <Target Table Name>"'

# TODO:
#   - Implement data read (from flat file) and load (to PostgreSQL database) using Spark instead of Scala only.
#   - Clean schema in DB.
#   - Normalize data into header table (primary key) and data table (foreign key).
