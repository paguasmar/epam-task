import argparse
import logging
import traceback
from typing import Any, Dict, Tuple

import pandas as pd
import yaml
from pandas import DataFrame


def setup_logging(log_file_path: str, log_level: str) -> None:
    """
    Set up logging configuration.

    Args:
        log_file_path (str): Path to the log file.
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Returns:
        None
    """
    # Create logger
    logger: logging.Logger = logging.getLogger()
    logger.setLevel(log_level)

    # Create file handler
    file_handler: logging.FileHandler = logging.FileHandler(log_file_path)
    file_handler.setLevel(log_level)

    # Create console handler
    console_handler: logging.StreamHandler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # Create formatter and add it to the handlers
    formatter: logging.Formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        config_file (str): Path to the YAML configuration file.

    Returns:
        dict: Loaded configuration.
    """
    try:
        with open(config_file, "r") as f:
            config: Dict[str, Any] = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_file}")
        return
    except yaml.YAMLError as e:
        logging.error(f"Error parsing configuration file: {e}")
        return


def update_values(
    original_dict: Dict[str, Any], update_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Update values in the original dictionary with values
    from the update dictionary.

    Args:
        original_dict (dict): Original dictionary.
        update_dict (dict): Dictionary with updated values.

    Returns:
        dict: Updated dictionary.
    """
    for key, value in update_dict.items():
        if value is not None:
            original_dict[key] = value
    return original_dict


def calculate_orders_per_product_per_week(
    df_products_sales: DataFrame,
) -> DataFrame:
    """
    Calculate the number of orders per product per week.

    Args:
        df_products_sales (pandas.DataFrame):
            DataFrame containing product sales data.

    Returns:
        pandas.DataFrame:
            DataFrame with the number of orders per product per week.
    """
    logger: logging.Logger = logging.getLogger(__name__)
    logger.info("Calculating number of orders per product per week...")
    df_copy: DataFrame = df_products_sales.copy(deep=False)
    df_copy["week"] = (
        df_copy["order_purchase_timestamp"]
        .dt.isocalendar()
        .week.astype("uint8")
    )
    df_copy["year"] = df_copy["order_purchase_timestamp"].dt.year.astype(
        "uint16"
    )
    return (
        df_copy.groupby(["product_id", "year", "week"])
        .size()
        .reset_index(name="sales")
        .astype({"sales": "uint32"})
    )


def read_input_data(config: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    logger: logging.Logger = logging.getLogger(__name__)
    # Read input data
    logger.info("Reading input data...")
    try:
        df_orders: DataFrame = pd.read_csv(
            config["orders_dataset_path"],
            index_col="order_id",
            parse_dates=["order_purchase_timestamp"],
            usecols=["order_id", "order_status", "order_purchase_timestamp"],
            dtype={"order_status": "category", "order_id": "str"},
        )
        df_order_items: DataFrame = pd.read_csv(
            config["order_items_dataset_path"],
            usecols=["order_id", "product_id"],
            dtype={"order_id": "str", "product_id": "str"},
            index_col="order_id",
        )
        return df_orders, df_order_items
    except FileNotFoundError as e:
        logger.error("Input data file not found: %s", str(e))
        return
    except Exception as e:
        logger.error("Error occurred while reading input data: %s", str(e))
        logger.error(traceback.format_exc())
        return


def filter_orders_by_status(
    df_orders: DataFrame, order_status_filter: str
) -> DataFrame:
    logger: logging.Logger = logging.getLogger(__name__)
    # Filter df_orders by order_status
    logger.info("Filtering orders by status: %s", order_status_filter)
    return df_orders[df_orders["order_status"] == order_status_filter]


def select_relevant_columns(df_orders_delivered: DataFrame) -> DataFrame:
    logger: logging.Logger = logging.getLogger(__name__)
    logger.info("Selecting relevant columns...")
    return df_orders_delivered[["order_purchase_timestamp"]]


def join_dataframes(
    df_order_items: DataFrame, df_orders_delivered: DataFrame
) -> DataFrame:
    logger: logging.Logger = logging.getLogger(__name__)
    logger.info("Joining dataframes...")
    return df_order_items.merge(
        df_orders_delivered, left_index=True, right_index=True, how="inner"
    )


def save_results(
    df_products_sales_weekly: DataFrame, config: Dict[str, Any]
) -> None:
    logger: logging.Logger = logging.getLogger(__name__)
    logger.info("Saving results to %s...", config["output_path"])
    df_products_sales_weekly.to_parquet(
        config["output_path"],
        partition_cols=config["partition_cols"],
        engine="fastparquet",
        index=False,
    )


def main(args: argparse.Namespace) -> None:
    """
    Main function to execute the pipeline.

    Args:
        args (Namespace): Command-line arguments.

    Returns:
        None
    """
    config: Dict[str, Any] = load_config(args.config)

    # Update configuration with command-line arguments
    config = update_values(config, vars(args))
    setup_logging(config["log_file_path"], config["log_level"])
    logger: logging.Logger = logging.getLogger(__name__)
    logger.info("Starting pipeline execution.")

    df_orders: DataFrame
    df_order_items: DataFrame
    df_orders, df_order_items = read_input_data(config)
    df_orders_delivered: DataFrame = filter_orders_by_status(
        df_orders, config["order_status_filter"]
    )
    df_orders_delivered: DataFrame = select_relevant_columns(
        df_orders_delivered
    )
    df_products_sales: DataFrame = join_dataframes(
        df_order_items, df_orders_delivered
    )
    df_products_sales_weekly: DataFrame = (
        calculate_orders_per_product_per_week(df_products_sales)
    )
    save_results(df_products_sales_weekly, config)

    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Process data and save results."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="pipeline_config.yaml",
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--orders_dataset_path",
        type=str,
        help="Path to the orders dataset CSV file",
    )
    parser.add_argument(
        "--order_items_dataset_path",
        type=str,
        help="Path to the order items dataset CSV file",
    )
    parser.add_argument(
        "--output_path", type=str, help="Path to save the output Parquet file"
    )
    parser.add_argument(
        "--order_status_filter", type=str, help="Filter orders by status"
    )
    parser.add_argument(
        "--output_engine",
        type=str,
        help="Engine to use for writing the output Parquet file",
    )
    parser.add_argument(
        "--partition_cols",
        nargs="+",
        help="Columns to partition the output Parquet file",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "--log_file_path",
        type=str,
        default="pipeline.log",
        help="Path to the log file",
    )
    args: argparse.Namespace = parser.parse_args()
    main(args)
