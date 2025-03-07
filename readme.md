# MariaDB Cron Parser & Scheduler

A collection of MariaDB stored functions that parse cron expressions (with seconds and complex intervals) into JSON and calculate the next execution timestamp directly in SQL. Additionally, a UDF written in C is provided for even faster performance in computing the next execution time.

## Features

- Parse cron expressions with seconds.
- Supports complex interval definitions (e.g., `5-29/2,31-59/4`).
- Calculates the next execution time for a given cron schedule.
- Easily integrated into database-based job scheduling.
- **Optional UDF Variant:** An implementation in C for improved performance.

## Usage

### Using Stored Functions

To get the next execution timestamp using the stored functions, simply run:

```sql
SELECT get_next_execution('30 5-29/2,31-59/4 */3 4/5 * 1-3') AS next_execution;
```

### Using the UDF Variant

The UDF variant is implemented in C (in `cron_next.c`). It directly computes the next execution timestamp based on the cron expression with improved performance.

#### Building and Installing the UDF

1. **Compile the UDF:**

   ```bash
   gcc -shared -fPIC -o cron_next.so cron_next.c $(mysql_config --cflags --libs)
   ```

2. **Copy the Shared Object to the Plugin Directory:**

   ```bash
   sudo cp cron_next.so /usr/lib/mysql/plugin/
   ```

3. **Restart MariaDB:**

   ```bash
   systemctl restart mariadb
   ```

4. **Register the UDF in MariaDB:**

   ```sql
   CREATE FUNCTION cron_next_execution RETURNS INTEGER SONAME 'cron_next.so';
   ```

#### Explanation of the UDF Variant

The UDF `cron_next_execution` accepts a cron expression as input and returns the next execution time as a Unix timestamp. Written in C, the UDF bypasses the overhead of SQL-based string parsing and recursion by performing the parsing and calculation in compiled code. This results in faster performance, especially when processing many cron expressions.

#### Query Example for the UDF Variant

Once the UDF is installed and registered, you can call it in your SQL queries. For example, to compute the next execution timestamp for a cron expression, run:

```sql
SELECT cron_next_execution('30 5-29/2,31-59/4 */3 4/5 * 1-3') AS next_execution;
```

This will return the next execution timestamp as an integer (Unix timestamp). You can convert it to a readable datetime format using MariaDB's `FROM_UNIXTIME()` function if desired:

```sql
SELECT FROM_UNIXTIME(cron_next_execution('30 5-29/2,31-59/4 */3 4/5 * 1-3')) AS next_execution_datetime;
```

## Summary

- **Stored Functions:** Provide a pure SQL solution for parsing and scheduling cron jobs.
- **UDF Variant:** Offers improved performance by implementing the logic in C. Follow the steps above to compile, install, and register the UDF, then query it as shown.

Feel free to adjust the instructions as needed for your environment.
