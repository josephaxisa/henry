# Henry - A Looker Cleanup Tool

Henry can be used to navigate and manages Spaces, Looks,
and Dashboards via a simple command line tool.

## Status and Support

Henry is **NOT** supported or warranted by Looker in any way. Please do not contact Looker support
for issues with Gazer. Issues can be logged via https://github.com/josephaxisa/henry/issues

## Setting up Henry

You can install this gem by simply typing:

    $ pip install henry

Alternately you can follow the Development setup below, typing `bundle exec rake install` to install it
locally

## Usage

Display help information...

    $ henry --help

### Storing Credentials
Store login information by creating the file `config.yml` in the home directory of your script with the api3 credentials

```
hosts:
  host_alias:
    access_token: ''
    host: foo.bar.companyname.com
    id: AbCdEfGhIjKlMnOp
    secret: QrStUvWxYz1234567890
```

Make sure that the `config.yml` file has restricted permissions by running `chmod 600 config.yml`. The tool will also ensure that this is the case every time it writes to the file.

### Global Options that apply to many commands

#### Suppressing Formatted Output

Many commands provide tabular output. For tables the option `--plain` will suppress the table headers and format lines, making it easier to use tools like grep, awk, etc. to retrieve values from the output of these commands.

#### CSV Output

Many commands provide tabular output. For tables the option `--csv` will output tabular data in
csv format. When combined with `--plain` the header will also be suppressed.

### Pulse Information

The command `henry pulse` runs a number of tests that help determine the overall instance health. A healthy Looker instance should pass all the tests. Below is a list of tests currently implemented.

#### Connection Checks
Runs specific tests for each connection to make sure the connection is in working order. If any tests fail, the output will show which tests passed or failed for that particular connection.
```
+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| thelook          | Can connect                                                                                                                                                                                  |
|                  | Can cancel queries                                                                                                                                                                           |
|                  | Can find temp schema "tmp"                                                                                                                                                                   |
|                  | Can create temporary tables                                                                                                                                                                  |
|                  | Can run simple select query                                                                                                                                                                  |
|                  | Compatible mysql version (5.7.21-log)                                                                                                                                                        |
|                  | Failed to create or write to pdt connection registration table tmp.connection_reg_r3 : Connection registration error for thelook: max registrations reached for connection thelook           |
| thelook_redshift | OK                                 																													                                      |
| bq_publicdata    | Driver cannot be initialized: Connection "bq_publicdata" with no cert on disk or in db!                                                                                                      |
| looker_bq        | OK                                                                                                                                                                    					   |
| rds_postgres     | OK                                                                                                                                                                                           |
+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

#### Query Stats
Checks how many queries were run over the past 30 days and how many of them had to queue, errored or got killed as well as some statistics around runtimes and queuing times.

#### Scheduled Plans
Determines the number of scheduled jobs that ran in the past 30 days, how many were successful, how many ran but did not deliver or failed to run altogether.

#### Legacy Features
Outputs a list of legacy features that are still in use if any. These are features that have been replaced with improved ones and should be moved away from.

#### Version
Checks if the latest Looker version is being used. Looker supports only up to 3 releases back.


### Analyze Information
The `analyze` command is meant to help identify models and explores that have become bloated and use `vacuum` on them in order to trim them.

### analyze projects
The `analyze projects` command scans projects for their content as well as checks for the status of quintessential features for success such as the git connection status and validation requirements.
```
+-----------+---------------+--------------+-----------------------------+-----------------+-----------------------+
| project   |  model_count  |  view_count  | Git Connection              | Pull Requests   | Validation Required   |
|-----------+---------------+--------------+-----------------------------+-----------------+-----------------------|
| MySQL     |       1       |      13      | OK                          | off             | True                  |
| BigQuery  |       1       |      1       | OK                          | off             | True                  |
| redshift  |       2       |      8       | verify_remote  (PASS)       | links           | True                  |
|           |               |              | git_hostname_resolves (PASS)|                 |                       |
|           |               |              | can_reach (FAIL)            |                 |                       |
|           |               |              | read_access (FAIL)          |                 |                       |
|           |               |              | write_access (FAIL)         |                 |                       |
| postgres  |       2       |      5       | OK                          | required        | False                 |
+-----------+---------------+--------------+-----------------------------+-----------------+-----------------------+
```

### analyze models
Shows the number of explores in each model as well as the number of queries against that model.
```
+-----------+----------------+-----------------+-------------------+
| project   | model          |  explore_count  |  query_run_count  |
|-----------+----------------+-----------------+-------------------|
| MySQL     | thelook        |        0        |         0         |
| BigQuery  | sports_tracker |        1        |         4         |
| redshift  | ecommerce      |        6        |         3         |
| postgres  | postgres       |        2        |        23         |
| postgres  | ML             |        3        |        40         |
| redshift  | empty_model    |        0        |         0         |
+-----------+----------------+-----------------+-------------------+
```

### analyze explores
Shows explores and their usage. If the `--min_queries` argument is passed, joins and fields that have been used less than the threshold specified will be considered as unused.
```
+----------------+-----------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------+
| model          | explore               | is_hidden   | has_description   |  join_count  |  unused_joins  |  field_count  |  unused_fields  |  query_count  |
|----------------+-----------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------|
| ecommerce      | distribution_centers  | False       | No                |      0       |       0        |       5       |        5        |       0       |
| ecommerce      | events                | False       | No                |      1       |       0        |      44       |       43        |       1       |
| ecommerce      | inventory_items       | False       | Yes               |      2       |       2        |      39       |       39        |       0       |
| ecommerce      | order_items           | False       | No                |      4       |       4        |      103      |       100       |       2       |
| ecommerce      | products              | False       | No                |      1       |       1        |      16       |       16        |       0       |
| ecommerce      | users                 | False       | No                |      0       |       0        |      21       |       21        |       0       |
| ML             | prediction            | False       | No                |      0       |       0        |      110      |       103       |      38       |
| ML             | historical_analysis   | False       | No                |      0       |       0        |      110      |       110       |       1       |
+----------------+-----------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------+
```

### Vacuum Information
The `vacuum` command outputs a list of unused content based on predefined criteria that a developer can then use to cleanup models and explores.

### vacuum models
The `vacuum models` command exposes models and the number of queries against them over a predefined period of time. Explores that are listed here have not had the minimum number of queries against them in the timeframe specified. As a result it is safe to hide them and later delete them.
```
+----------------+----------------------+-------------------------+
| model          | unused_explores      |  model_query_run_count  |
|----------------+----------------------+-------------------------|
| thelook        | None                 |            0            |
| sports_tracker | None                 |            4            |
| ecommerce      | products             |            3            |
|                | distribution_centers |                         |
|                | users                |                         |
|                | inventory_items      |                         |
| postgres       | None                 |           23            |
| ML             | None                 |           40            |
| empty_model    | None                 |            0            |
+----------------+----------------------+-------------------------+
```

### vacuum explores
The `vacuum explores` command exposes joins and exposes fields that have not are below the minimum number of queries threshold (default =0, can be changed using the `--min_queries` argument) over the specified timeframe (default: 90, can be changed using the `--timeframe` argument).
```
+-----------+----------------------+----------------------+------------------------------------------------+
| model     | explore              | unused_joins         | unused_fields                                  |
|-----------+----------------------+----------------------+------------------------------------------------|
| ecommerce | distribution_centers | N/A                  | distribution_centers.count                     |
|           |                      |                      | distribution_centers.id                        |
|           |                      |                      | distribution_centers.latitude                  |
|           |                      |                      | distribution_centers.longitude                 |
|           |                      |                      | distribution_centers.name                      |
| ecommerce | inventory_items      | distribution_centers | inventory_items.cost                           |
|           |                      | products             | inventory_items.count                          |
|           |                      |                      | inventory_items.created_date                   |
|           |                      |                      | inventory_items.created_month                  |
|           |                      |                      | inventory_items.created_quarter                |
|           |                      |                      | inventory_items.created_time                   |
|           |                      |                      | inventory_items.created_week                   |
|           |                      |                      | inventory_items.created_year                   |
|           |                      |                      | inventory_items.id                             |
|           |                      |                      | inventory_items.product_brand                  |
|           |                      |                      | inventory_items.product_category               |
|           |                      |                      | inventory_items.product_department             |
|           |                      |                      | inventory_items.product_distribution_center_id |
|           |                      |                      | inventory_items.product_id                     |
|           |                      |                      | inventory_items.product_name                   |
|           |                      |                      | inventory_items.product_retail_price           |
|           |                      |                      | inventory_items.product_sku                    |
|           |                      |                      | inventory_items.sold_date                      |
|           |                      |                      | inventory_items.sold_month                     |
|           |                      |                      | inventory_items.sold_quarter                   |
|           |                      |                      | inventory_items.sold_time                      |
|           |                      |                      | inventory_items.sold_week                      |
|           |                      |                      | inventory_items.sold_year                      |
+-----------+----------------------+----------------------+------------------------------------------------+
```
It is very important to note that fields vacuumed fields in one explore are not meant to be completely removed from view files altogether because they might be used in other explores. Instead, one should either hide those fields (if they're not used anywhere else) or exclude them from the explore using the _fields_ LookML parameter.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/deangelo-llooker/gzr. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## Code of Conduct

Everyone interacting in the Henry project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/looker/content_util/blob/master/CODE_OF_CONDUCT.md).

## Copyright

Copyright (c) 2018 Joseph Axisa for Looker Data Sciences. See [MIT License](LICENSE.txt) for further details.
