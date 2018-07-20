<div align="center">
  <img src="https://github.com/llooker/lookml_field_usage/blob/master/doc/logo/logo.png"><br>
</div>

-----------------
# Henry: A Looker Cleanup Tool
Henry is a tool with a command line interface (CLI) that helps determine model bloat in your Looker instance and identify unused content in models and explores. The results are meant to help developers cleanup models from unused explores and explores from unused joins and fields

## Table of Contents
- [Status and Support](#status_and_support)
- [Installation](#where_to_get_it)
- [Usage](#usage)
  - [Storing Credentials](#storing_credentials)
  - [Global Options](#global_options)
    - [Suppressing Formatted Output](#supressed_output)
    - [CSV Output](#csv_output)
  - [The Pulse Command](#pulse_nformation)
  - [The Analyze Command](#analyze_information)
    - [Analyzing Projects](#analyze_projects)
    - [Analyzing Models](#analyze_models)
    - [Analyzing Explores](#analyze_explores)
  - [The Vacuum Command](#vacuum_information)
    - [Vacuuming Models](#vacuum_models)
    - [Vacuuming Explores](#vacuum_explores)
- [Dependencies](#dependencies)
- [Development](#development)
- [Contributing](#contributing)
- [Code of Conduct](#code_of_conduct)
- [Copyright](#copyright)

## Status and Support <a name="status_and_support"></a>
Henry is **NOT** supported or warranted by Looker in any way. Please do not contact Looker support
for issues with Henry. Issues can be logged via https://github.com/josephaxisa/henry/issues

## Where to get it <a name="where_to_get_it"></a>
The source code is currently hosted on GitHub at https://github.com/josephaxisa/henry/henry. The latest released version can be found on [PyPI](https://pypi.org/project/henry/) and can be installed using:

    $ pip install henry

For development setup, follow the Development setup below.

## Usage <a name="usage"></a>
In order to display usage information, use:

    $ henry --help


### Storing Credentials <a name="storing_credentials"></a>
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

### Global Options that apply to many commands <a name="global_options"></a>
#### Suppressing Formatted Output  <a name="suppressed_output"></a>
Many commands provide tabular output. For tables the option `--plain` will suppress the table headers and format lines, making it easier to use tools like grep, awk, etc. to retrieve values from the output of these commands.

#### CSV Output <a name="csv_output"></a>
Many commands provide tabular output. For tables the option `--csv` will output tabular data in
csv format. When combined with `--plain` the header will also be suppressed.

### Pulse Information <a name="pulse_information"></a>
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


### Analyze Information <a name="analyze_output"></a>
The `analyze` command is meant to help identify models and explores that have become bloated and use `vacuum` on them in order to trim them.

### analyze projects <a name="analyze_projects"></a>
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

### analyze models <a name="analyze_models"></a>
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

### analyze explores <a name="analyze_explores"></a>
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

### Vacuum Information <a name="vacuum_information"></a>
The `vacuum` command outputs a list of unused content based on predefined criteria that a developer can then use to cleanup models and explores.

### vacuum models <a name="vacuum_models"></a>
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

### vacuum explores <a name="vacuum_explores"></a>
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

## Dependencies <a name="dependencies"></a>
- [PyYAML](https://pyyaml.org/): 3.12 or higher
- [requests](http://docs.python-requests.org/en/master/): 2.18.4 or higher
- [tabulate](https://bitbucket.org/astanin/python-tabulate): 0.8.2 or higher
- [tqdm](https://tqdm.github.io/): 4.23.4 or higher

## Development <a name="development"></a>

To install henry in development mode need clone the repo and install the dependencies above.

You can then install using:

    $ python setup.py develop

Alternatively, you can use `pip` if you want all the dependencies pulled in automatically (the -e option is for installig it in [development mode](https://pip.pypa.io/en/latest/reference/pip_install/#editable-installs)).

    $ pip install -e .

## Contributing <a name="contributing></a>

Bug reports and pull requests are welcome on GitHub at https://github.com/josephaxisa/henry/issues. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## Code of Conduct <a name="code_of_conduct"></a>

Everyone interacting in the Henry project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/looker/content_util/blob/master/CODE_OF_CONDUCT.md).

## Copyright <a name="copyright""></a>

Copyright (c) 2018 Joseph Axisa for Looker Data Sciences. See [MIT License](LICENSE.txt) for further details.
