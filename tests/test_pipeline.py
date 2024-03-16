import pandas as pd
import pytest
import argparse
from pipeline import calculate_orders_per_product_per_week, update_values, main, load_config
from pathlib import Path
from unittest.mock import patch

@pytest.fixture
def sample_data():
    # Sample input data
    data = {
        'order_purchase_timestamp': pd.to_datetime(['2023-01-02', '2023-01-03', '2023-01-04']),
        'product_id': [101, 102, 101]
    }
    return pd.DataFrame(data)

def test_calculate_orders_per_product_per_week(sample_data):
    # Call the function with sample data
    result = calculate_orders_per_product_per_week(sample_data)

    # Check if the values are calculated correctly
    expected_values = pd.DataFrame({
        'product_id': [101, 102],
        'year': [2023, 2023],
        'week': [1, 1],
        'sales': [2, 1]
    })
    pd.testing.assert_frame_equal(result, expected_values, check_dtype=False)


def test_update_values():
    # Test case 1: Update all values
    original_dict = {'a': 1, 'b': 2, 'c': 3}
    update_dict = {'a': 10, 'b': 20, 'c': 30}
    expected_result = {'a': 10, 'b': 20, 'c': 30}
    assert update_values(original_dict, update_dict) == expected_result

    # Test case 2: Update some values
    original_dict = {'a': 1, 'b': 2, 'c': 3}
    update_dict = {'b': 20, 'c': None}
    expected_result = {'a': 1, 'b': 20, 'c': 3}
    assert update_values(original_dict, update_dict) == expected_result

    # Test case 3: Update no values
    original_dict = {'a': 1, 'b': 2, 'c': 3}
    update_dict = {}
    expected_result = {'a': 1, 'b': 2, 'c': 3}
    assert update_values(original_dict, update_dict) == expected_result


def test_main(tmp_path, caplog):

    output_path = f"{tmp_path}/products_sales"
    # Mock the call to pipeline.load_config
    with patch('pipeline.load_config') as mock_load_config:
        # Define the dictionary to be returned by pipeline.load_config
        config_dict = {
            "orders_dataset_path": "test-resources/in/olist_orders_dataset.csv",
            "order_items_dataset_path": "test-resources/in/olist_order_items_dataset.csv",
            "output_path": output_path,
            "order_status_filter": "delivered",
            "output_engine": "fastparquet",
            "partition_cols": ["product_id"],
            "log_level": "INFO",
            "log_file_path": f"{tmp_path}/pipeline.log"
            }
        mock_load_config.return_value = config_dict
        
        # Run the main function with the sample configuration file
        args = argparse.Namespace(config="")
        main(args)

    # Check if the output file is created correctly
    expected_values = pd.read_parquet("test-resources/out/products_sales")
    result = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(result, expected_values, check_dtype=False)

    # Check if the expected log messages are present
    assert "Starting pipeline execution." in caplog.text
    assert "Reading input data..." in caplog.text
    assert "Filtering orders by status: delivered" in caplog.text
    assert "Selecting relevant columns..." in caplog.text
    assert "Joining dataframes..." in caplog.text
    assert "Calculating number of orders per product per week..." in caplog.text
    assert f"Saving results to {output_path}..." in caplog.text
    assert "Pipeline execution completed successfully." in caplog.text

def test_load_config(tmp_path):
    # Create a temporary config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
    orders_dataset_path: "test-resources/in/olist_orders_dataset.csv"
    order_items_dataset_path: "test-resources/in/olist_order_items_dataset.csv"
    output_path: "test-resources/out/products_sales"
    order_status_filter: "delivered"
    output_engine: "fastparquet"
    partition_cols:
      - "product_id"
    log_level: "INFO"
    log_file_path: "pipeline.log"
    """)

    # Call the load_config function
    config = load_config(str(config_file))

    # Check if the loaded configuration matches the expected values
    expected_config = {
        "orders_dataset_path": "test-resources/in/olist_orders_dataset.csv",
        "order_items_dataset_path": "test-resources/in/olist_order_items_dataset.csv",
        "output_path": "test-resources/out/products_sales",
        "order_status_filter": "delivered",
        "output_engine": "fastparquet",
        "partition_cols": ["product_id"],
        "log_level": "INFO",
        "log_file_path": "pipeline.log"
    }
    assert config == expected_config