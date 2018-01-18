import os

import pytest

from airflow_plugins.operators import CreateTableWithColumns


@pytest.mark.parametrize(
    ['file_name', 'known_columns'],
    [('test_db_columns.csv', [
        "date", "bookings_count", "sectors_count", "passengers_count",
        "seats_count", "bags_count", "booked_at", "partner", "market",
        "airlines", "currency", "nationality", "device_type", "trip_type",
        "src_dst", "src", "dst", "transfers", "booking_channel", '"AT_sales"',
        "insurance_costs", "has_kiwi_interlining", "extras_sales", "refunds",
        '"AT_costs"', "turnover", "margin", '"intra space"'
    ])]
)
def test_create_table_with_columns(file_name, known_columns):
    file = os.path.join(os.path.dirname(os.path.realpath(__file__)), file_name)
    get_columns = CreateTableWithColumns._get_table_columns
    columns = get_columns(file)
    assert len(known_columns) == len(columns)
    for i in range(len(known_columns)):
        assert known_columns[i] == columns[i]
