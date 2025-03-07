# MariaDB Cron Parser & Scheduler

A collection of MariaDB stored functions that parse cron expressions (with seconds and complex intervals) into JSON and calculate the next execution timestamp directly in SQL.

## Features

- Parse cron expressions with seconds.
- Supports complex interval definitions (e.g., `5-29/2,31-59/4`).
- Calculates the next execution time for a given cron schedule.
- Easily integrated into database-based job scheduling.

## Usage

To get the next execution timestamp, simply run:

```sql
SELECT get_next_execution('30 5-29/2,31-59/4 */3 4/5 * 1-3') AS next_execution;
