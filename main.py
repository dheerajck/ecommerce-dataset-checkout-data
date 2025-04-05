import sqlite3
import pandas as pd

from pathlib import Path


def dump_csv_to_db(db_name: str, csv_path: str, table_name: str, chunksize: int = 10000) -> None:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f'CSV file not found: {csv_path}')

    with sqlite3.connect(db_name) as conn:
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk.to_sql(table_name, conn, if_exists='append', index=False)

    print(f"Data from {csv_path} dumped into table '{table_name}' in {db_name}.")

    return None


def read_table_to_df(db_name: str, table_name: str) -> pd.DataFrame:
    with sqlite3.connect(db_name) as conn:
        try:
            df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
        except pd.errors.DatabaseError:
            raise ValueError(f"Table '{table_name}' does not exist in the database '{db_name}'.")

    print(f"Data from table '{table_name}' in {db_name} read into DataFrame.")

    return df


def part1():
    db_name = 'ecommerce_dataset.sqlite'

    # Delete existing database if it exists
    db_path = Path(db_name)
    if db_path.exists():
        db_path.unlink()

    dump_csv_to_db(db_name, 'dataset/customers_dataset.csv', 'customers_dataset')
    dump_csv_to_db(db_name, 'dataset/orders_dataset.csv', 'orders_dataset')
    dump_csv_to_db(db_name, 'dataset/order_reviews_dataset.csv', 'order_reviews_dataset')
    dump_csv_to_db(db_name, 'dataset/product_category_name_translation.csv', 'product_category_name_translation')
    dump_csv_to_db(db_name, 'dataset/geolocation_dataset.csv', 'geolocation_dataset')
    dump_csv_to_db(db_name, 'dataset/order_payments_dataset.csv', 'order_payments_dataset')
    dump_csv_to_db(db_name, 'dataset/sellers_dataset.csv', 'sellers_dataset')
    dump_csv_to_db(db_name, 'dataset/order_items_dataset.csv', 'order_items_dataset')
    dump_csv_to_db(db_name, 'dataset/products_dataset.csv', 'products_dataset')


def part2():
    customers_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'customers_dataset')
    orders_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'orders_dataset')
    order_reviews_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'order_reviews_dataset')
    product_category_name_translation = read_table_to_df('ecommerce_dataset.sqlite', 'product_category_name_translation')
    geolocation_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'geolocation_dataset')
    order_payments_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'order_payments_dataset')
    sellers_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'sellers_dataset')
    order_items_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'order_items_dataset')
    products_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'products_dataset')

    # Step 1: Merge delivered orders with payments customer lifetime value
    delivered_orders = orders_dataset[orders_dataset['order_status'] == 'delivered']
    delivered_orders_with_payments = pd.merge(delivered_orders, order_payments_dataset, on='order_id', how='left')

    # Step 2: Merge with customers to get state
    delivered_orders_with_payments_and_customers = pd.merge(delivered_orders_with_payments, customers_dataset, on='customer_id', how='left')

    # Step 3: Group by customer_state and sum payment_value
    sales_by_region = delivered_orders_with_payments_and_customers.groupby('customer_state')['payment_value'].sum().reset_index()

    # Step 4: Sort by sales
    sales_by_region = sales_by_region.sort_values(by='payment_value', ascending=False)

    # Top 5 state by sales
    print(sales_by_region[:5])

    # Top 5 customers by sales

    customers_and_sales = (
        delivered_orders_with_payments_and_customers.groupby('customer_unique_id')['payment_value']
        .sum()
        .reset_index()
        .rename(columns={'payment_value': 'customer_lifetime_value'})
        .sort_values(by='customer_lifetime_value', ascending=False)
    )

    print(customers_and_sales[:5])

    product_order_status_mapping_data = orders_dataset.merge(order_items_dataset, on='order_id', how='left').merge(
        products_dataset, on='product_id', how='left'
    )

    df = product_order_status_mapping_data

    # Step 1: Add a column to classify orders as 'failed' or 'delivered'
    df['is_failed'] = df['order_status'].isin(['unavailable', 'canceled']).astype(int)
    df['is_delivered'] = (df['order_status'] == 'delivered').astype(int)

    # Step 2: Group by product_id and sum the counts
    product_counts = df.groupby('product_id')[['is_failed', 'is_delivered']].sum().reset_index()

    # Step 3: Calculate failed rate
    product_counts['failed_rate (%)'] = (product_counts['is_failed'] / product_counts['is_delivered']) * 100

    # Handle cases where is_delivered = 0 (avoid division by zero)
    product_counts['failed_rate (%)'] = product_counts['failed_rate (%)'].replace([float('inf')], -1)

    product_counts.loc[(product_counts['is_delivered'] == 0) & (product_counts['is_failed'] == 0), 'failed_rate (%)'] = 0
    product_counts.loc[(product_counts['is_delivered'] == 0) & (product_counts['is_failed'] > 0), 'failed_rate (%)'] = 100

    # Sort by failed rate
    product_counts = product_counts.sort_values(by='failed_rate (%)', ascending=False)

    product_counts.sort_values(by='failed_rate (%)', ascending=False)[:5].reset_index(drop=True)

    # Top 5 products with highest failed rate
    top_failed_products = product_counts.sort_values(by='failed_rate (%)', ascending=False).reset_index(drop=True)[:5]
    print(top_failed_products)


def main():
    # part1()

    # print()
    # print()

    part2()


if __name__ == '__main__':
    main()
