import pandas as pd
import pytest
from pipeline import calculate_orders_per_product_per_week, update_values, main

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
