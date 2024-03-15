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

    # Check if the result is a DataFrame
    assert isinstance(result, pd.DataFrame)

    # Check if the expected columns are present
    assert 'product_id' in result.columns
    assert 'year' in result.columns
    assert 'week' in result.columns
    assert 'sales' in result.columns

    # Check if the result has the correct number of rows
    assert len(result) == 2

    # Check if the values are calculated correctly
    expected_values = pd.DataFrame({
        'product_id': [101, 102],
        'year': [2023, 2023],
        'week': pd.Series([1, 1], dtype="UInt32"),
        'sales': [2, 1]
    })
    pd.testing.assert_frame_equal(result, expected_values)


