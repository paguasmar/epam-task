import pandas as pd
import pytest
from pipeline import calculate_orders_per_product_per_week

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


