# Pipeline README

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
   python pipeline.py --config pipeline_config.yaml
   ```

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