# Data Engineering task
*We want to forecast for each item, what are the sales going to be next week.*

*Expected output is repository that has the following:*

1. *Code to load relevant tables for the task (minimum tables needed), and prepare efficient ETL that builds a dataset on which Data Scientist can continue the work (use pandas)* **(done)**
    1. *The output should be in parquet, well partitioned by product* **(done)**
    2. *The format of output is a single table that can be used for modelling (no need to extract features).* **(done)**
2. *python script to run code, that you can pass arguments to* **(done)**
3. *A couple of simple pytest tests, and run them in github actions at every PR.* **(done)**
4. *Configuration files in yml* **(done)**
5. *Think about the following:*
    1. *Which features would you extract and how from the tables? How would you use the remaining tables?*
        
        Choosing the right features to extract from the tables would need an open discussion with the Data Scientists that will ingest the data.
        Besides the number of sales per product per week, one could extract the following:
        - Average order product price during the week
            1. Retain the column `price` in `olist_order_items_dataset` (in `pipeline.py` line 68).
            2. Add the aggregation function `avg` applied to `price` (in `pipeline.py` line 42).
        - Product category
            1. Load table `input/olist_products_dataset.csv` as `df_products`.
            2. Select `product_id` and `product_category_name`.
            3. Join `df_products` with `df_order_items` by `product_id`.
    2. *How would you turn it into an application in production?*

        To turn the code into an application for production, we should follow these steps:

        - Organize the Code: We should break down the code into smaller functions or classes. This helps make the code easier to understand, maintain, and reuse.

        - Testing: It's important to implement more testing to ensure that any changes or updates to the code do not introduce errors. We can do this by adding more unit tests and integration tests.

        - Error Handling: We need to implement error handling mechanisms, such as exception handling and logging. This helps us handle unexpected situations gracefully and provides meaningful error messages to users.

        - Data Storage: We should choose an appropriate data storage solution based on the specific requirements of the application. This could be a database or a file system, depending on factors like data volume, complexity, and access patterns.

        Performance Optimization: We need to optimize the performance and efficiency of data processing operations. This involves identifying bottlenecks and implementing optimizations to improve the speed and responsiveness of the application.
    3. *How would you design an application if you knew that you would have to build a similar solution for a couple other countries, and the data schema might be different for them, however, you can get the same underlying data?*

        To design an application for multiple countries with potentially different data schemas but the same underlying data, we should follow these steps:

        - Modular Design: Organize the application into modules, with each module responsible for a specific task or functionality. This allows for easy swapping of components to accommodate different data schemas for different countries.

        - Data Normalization: Standardize the data into a common format that the entire application can understand. This involves renaming columns and converting data types to ensure consistency across different data schemas.

        - Data Ingestion Layer: Implement a data ingestion layer capable of fetching data from various sources, such as databases or APIs. This layer should transform the data into the standardized format before further processing.

        - Configuration Management: Use configuration files or database tables to specify the mapping between the source data schema and the common format. This allows for flexibility in adapting to different data schemas without modifying the application code.

## Overview

This repository contains a data processing pipeline implemented in Python using pandas and argparse.

## Pipeline Description

The pipeline performs the following tasks:

1. Reads input data from CSV files (`orders_dataset_path` and `order_items_dataset_path`).
2. Filters orders based on a specified status (`order_status_filter`).
3. Selects relevant columns from the filtered orders.
4. Joins the order items and orders by order ID.
5. Calculates the number of orders per product per week.
6. Saves the results to a Parquet file partitioned by product ID (`output_path`).

## Requirements

To run this pipeline, you need:

- Python 3.x
- pandas
- pyyaml

## Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/paguasmar/epam-task.git
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Prepare your input data:

   - Place the CSV files containing orders and order items in a directory.

4. Create a configuration file (`pipeline_config.yaml`) with the necessary parameters. An example configuration file is provided.

5. Run the pipeline:

   ```bash
   python pipeline.py [arguments]
   ```

## Command Line Arguments
1. `--config`
    - **Type**: String
    - **Default**: pipeline_config.yaml
    - **Description**: Path to the YAML configuration file.
2. `--orders_dataset_path`
    - **Type**: String
    - **Description**: Path to the orders dataset CSV file.
3. `--order_items_dataset_path`
    - **Type**: String
    - **Description**: Path to the order items dataset CSV file.
4. `--output_path`
    - **Type**: String
    - **Description**: Path to save the output Parquet file.
5. `--order_status_filter`
    - **Type**: String
    - **Description**: Filter orders by status.
6. `--output_engine`
    - **Type**: String
    - **Description**: Engine to use for writing the output Parquet file.
7. `--partition_cols`
    - **Type**: List of Strings
    - **Description**: Columns to partition the output Parquet file.
8. `--log_level`
    - **Type**: String
    - **Description**: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
9. `--log_file_path`
    - **Type**: String
    - **Default**: pipeline.log
    - **Description**: Path to the log file.

## Configuration

The pipeline configuration is specified in a YAML file (`pipeline_config.yaml`). You need to provide the following parameters:

- `orders_dataset_path`: Path to the orders dataset CSV file.
- `order_items_dataset_path`: Path to the order items dataset CSV file.
- `output_path`: Path to save the output Parquet file.
- `order_status_filter`: Filter orders by status.
- `output_engine`: Engine to use for writing the output Parquet file.
- `partition_cols`: Columns to partition the output Parquet file.
- `log_level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
- `log_file_path`: Path to the log file.

## Running the Tests
The Pytests in this project are automatically executed using GitHub Actions. Whenever code changes are pushed or pull requests are submitted, GitHub Actions runs the Pytests to validate the functionality of our code. You can view the test results directly in the GitHub Actions tab of this repository.

To run the Pytests locally, follow these steps:

- Run Pytest by executing the command pytest.

## Authors

- [Pedro Águas Marques](https://github.com/paguasmar)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
