import argparse
import pandas as pd
import yaml
import logging
from tqdm import tqdm

def setup_logging(log_file_path, log_level):
    logging.basicConfig(filename=log_file_path, level=log_level,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config

def update_values(original_dict, update_dict):
    for key, value in update_dict.items():
        if value is not None:
            original_dict[key] = value
    return original_dict

def main(args):
    # Setup logging
    setup_logging(args.log_file_path, args.log_level)
    logger = logging.getLogger(__name__)

    # Load configuration from YAML file
    config = load_config(args.config)

    print(config['orders_dataset_path'])

    # Update configuration with command-line arguments
    config = update_values(config, args)

    print(config['orders_dataset_path'])

    logger.info("Starting pipeline execution.")

    # Read input data
    logger.info("Reading input data...")
    df_orders = pd.read_csv(config['orders_dataset_path'], index_col="order_id", parse_dates=["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"])
    df_order_items = pd.read_csv(config['order_items_dataset_path'])

    # Filter df_orders by order_status
    logger.info("Filtering orders by status: %s", config['order_status_filter'])
    df_orders_delivered = df_orders[df_orders["order_status"] == config['order_status_filter']]

    # Select the columns order_id, order_purchase_timestamp
    logger.info("Selecting relevant columns...")
    df_orders_delivered = df_orders_delivered[["order_purchase_timestamp"]]

    # Join df_order_items and df_orders by order_id
    logger.info("Joining dataframes...")
    df_products_sales = df_order_items.join(df_orders_delivered, on="order_id", how="inner")

    # Number of orders per product per week
    logger.info("Calculating number of orders per product per week...")
    df_products_sales["week"] = df_products_sales["order_purchase_timestamp"].dt.week
    df_products_sales["year"] = df_products_sales["order_purchase_timestamp"].dt.year
    df_products_sales_weekly = df_products_sales.groupby(["product_id", "year", "week"]).size().reset_index(name="sales")

    # Save df_products_sales_weekly as parquet partitioned by product_id
    logger.info("Saving results to %s...", config['output_path'])
    with tqdm(total=len(df_products_sales_weekly)) as pbar:
        df_products_sales_weekly.progress_apply(lambda x: x.to_parquet(config['output_path'], partition_cols=config['partition_cols'], engine=config['output_engine'], index=False), axis=1)
        pbar.update(1)

    logger.info("Pipeline execution completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data and save results.")
    parser.add_argument("--config", type=str, default="pipeline_config.yaml", help="Path to YAML configuration file", required=True)
    parser.add_argument("--orders_dataset_path", type=str, help="Path to the orders dataset CSV file")
    parser.add_argument("--order_items_dataset_path", type=str, help="Path to the order items dataset CSV file")
    parser.add_argument("--output_path", type=str, help="Path to save the output Parquet file")
    parser.add_argument("--order_status_filter", type=str, help="Filter orders by status")
    parser.add_argument("--output_engine", type=str, help="Engine to use for writing the output Parquet file")
    parser.add_argument("--partition_cols", nargs='+', help="Columns to partition the output Parquet file")
    parser.add_argument("--log_level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    parser.add_argument("--log_file_path", type=str, default="pipeline.log", help="Path to the log file")
    args = parser.parse_args()
    main(args)
