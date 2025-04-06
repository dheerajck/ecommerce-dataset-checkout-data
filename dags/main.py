import sqlite3

import logging
import pandas as pd

from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime


def dump_csv_to_db(db_name: str, csv_path: str, table_name: str, chunksize: int = 10000) -> None:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f'CSV file not found: {csv_path}')

    with sqlite3.connect(db_name) as conn:
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk.to_sql(table_name, conn, if_exists='append', index=False)

    logging.info(f"Data from '{csv_path}' dumped into table '{table_name}' in database '{db_name}'")

    return None


def read_table_to_df(db_name: str, table_name: str) -> pd.DataFrame:
    with sqlite3.connect(db_name) as conn:
        try:
            df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
        except pd.errors.DatabaseError:
            raise ValueError(f"Table '{table_name}' does not exist in the database '{db_name}'")

    logging.info(f"Data from table '{table_name}' in database '{db_name}' read into DataFrame")

    return df


def load_all_data():
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


def compute_kpis_from_data():
    customers_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'customers_dataset')
    orders_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'orders_dataset')
    order_reviews_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'order_reviews_dataset')
    product_category_name_translation_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'product_category_name_translation')
    geolocation_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'geolocation_dataset')
    order_payments_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'order_payments_dataset')
    sellers_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'sellers_dataset')
    order_items_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'order_items_dataset')
    products_dataset = read_table_to_df('ecommerce_dataset.sqlite', 'products_dataset')

    ##########################################################################################################################################################

    delivered_orders = orders_dataset[orders_dataset['order_status'] == 'delivered']

    delivered_orders_with_payments_and_customers = delivered_orders.merge(
        order_payments_dataset,
        on='order_id',
        how='left',
    ).merge(
        customers_dataset,
        on='customer_id',
        how='left',
    )

    sales_by_region = (
        delivered_orders_with_payments_and_customers.groupby('customer_state')['payment_value']
        .sum()
        .reset_index()
        .sort_values(
            by='payment_value',
            ascending=False,
        )
        .reset_index(drop=True)
    )

    plt.figure(figsize=(12, 6))
    sns.barplot(data=sales_by_region, x='customer_state', y='payment_value', hue='payment_value', palette='viridis')
    plt.title('Total Sales by State')
    plt.ylabel('Total Payment Value')
    plt.xlabel('Customer State')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('Sales_by_region.png', dpi=300, bbox_inches='tight')
    plt.close()

    ##########################################################################################################################################################

    # Average Customer Lifetime Value by State: How much a typical customer from each state spends on average
    # Customer Count by State: How many customers you have in each state

    # High-value customers (even if there aren't many of them)
    # Large customer bases (even if they spend less individually)

    customers_and_sales = (
        delivered_orders_with_payments_and_customers.groupby(['customer_state', 'customer_unique_id'])['payment_value']
        .sum()
        .reset_index()
        .rename(columns={'payment_value': 'customer_lifetime_value'})
    )

    customers_and_sales = (
        customers_and_sales.groupby('customer_state')
        .agg({'customer_lifetime_value': 'mean', 'customer_unique_id': 'count'})
        .reset_index()
        .rename(columns={'customer_lifetime_value': 'avg_customer_value', 'customer_unique_id': 'customer_count'})
        .sort_values(by='avg_customer_value', ascending=False)
        .reset_index(drop=True)
    )

    plt.figure(figsize=(12, 6))
    sns.barplot(data=customers_and_sales, x='customer_state', y='avg_customer_value', hue='avg_customer_value', palette='Blues_d')
    plt.title('Average Customer Lifetime Value by State')
    plt.ylabel('Avg Customer Value')
    plt.xlabel('State')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('Avg_Customer_Lifetime_Value_by_State.png', dpi=300, bbox_inches='tight')
    plt.close()

    # Normalize
    norm_data = customers_and_sales.copy()
    norm_data['avg_customer_value_scaled'] = norm_data['avg_customer_value'] / norm_data['avg_customer_value'].max()
    norm_data['customer_count_scaled'] = norm_data['customer_count'] / norm_data['customer_count'].max()

    # Melt into long format for seaborn
    melted = norm_data.melt(
        id_vars='customer_state', value_vars=['avg_customer_value_scaled', 'customer_count_scaled'], var_name='Metric', value_name='Scaled Value'
    )

    # Rename for prettier legend
    melted['Metric'] = melted['Metric'].map(
        {'avg_customer_value_scaled': 'Avg Customer Value (scaled)', 'customer_count_scaled': 'Customer Count (scaled)'}
    )

    plt.figure(figsize=(12, 6))
    sns.barplot(data=melted, x='customer_state', y='Scaled Value', hue='Metric', palette='Paired')
    plt.title('Avg Customer Value and Customer Count by State (Scaled)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('Avg_Customer_Value_and_Customer_Count_by_State_Scaled.png', dpi=300, bbox_inches='tight')
    plt.close()

    ##########################################################################################################################################################

    # Failed order rate / category

    product_order_status_mapping_data = (
        orders_dataset.merge(order_items_dataset, on='order_id', how='left')
        .merge(products_dataset, on='product_id', how='left')
        .merge(product_category_name_translation_dataset, on='product_category_name', how='left')
    )

    # Step 1: Add a column to classify orders as 'failed' or 'delivered'
    product_order_status_mapping_data['is_failed'] = product_order_status_mapping_data['order_status'].isin(['unavailable', 'canceled']).astype(int)
    product_order_status_mapping_data['is_delivered'] = (product_order_status_mapping_data['order_status'] == 'delivered').astype(int)

    # Step 2: Group by product_category_name_english and sum the counts
    product_delivery_data = (
        product_order_status_mapping_data.groupby('product_category_name_english')[['is_failed', 'is_delivered']].sum().reset_index()
    )

    # Step 3: Calculate failed rate
    product_delivery_data['failed_rate (%)'] = (product_delivery_data['is_failed'] / product_delivery_data['is_delivered']) * 100

    # Handle cases where is_delivered = 0 (avoid division by zero)
    product_delivery_data['failed_rate (%)'] = product_delivery_data['failed_rate (%)'].replace([float('inf')], -1)

    product_delivery_data.loc[(product_delivery_data['is_delivered'] == 0) & (product_delivery_data['is_failed'] == 0), 'failed_rate (%)'] = 0
    product_delivery_data.loc[(product_delivery_data['is_delivered'] == 0) & (product_delivery_data['is_failed'] > 0), 'failed_rate (%)'] = 100

    # Sort by failed rate
    product_delivery_data = product_delivery_data.sort_values(by='failed_rate (%)', ascending=False).reset_index(drop=True)

    average_failure_rate = product_delivery_data['failed_rate (%)'].mean()

    # print(average_failure_rate)
    above_avg_failure_categories = product_delivery_data[product_delivery_data['failed_rate (%)'] > average_failure_rate]

    plt.figure(figsize=(10, len(above_avg_failure_categories) * 0.4))
    sns.barplot(
        data=above_avg_failure_categories.sort_values('failed_rate (%)'),
        x='failed_rate (%)',
        y='product_category_name_english',
        hue='failed_rate (%)',
        palette='viridis',
    )

    plt.axvline(average_failure_rate, color='red', linestyle='--', label='Avg Failure Rate')
    plt.title(f'Categories Above Avg Failure Rate ({average_failure_rate:.2f}%)')
    plt.xlabel('Failure Rate (%)')
    plt.ylabel('Product Category')
    plt.legend()
    plt.tight_layout()
    plt.savefig('Above_Avg_Failure_Rate_Categories.png', dpi=300, bbox_inches='tight')
    plt.close()

    ##########################################################################################################################################################


# with DAG('ecommerce_data_thing', schedule=None, catchup=False) as dag1:
#     acquisition_task = PythonOperator(task_id='data_acquisition', python_callable=load_all_data)
#     analysis_task = PythonOperator(task_id='data_analysis', python_callable=compute_kpis_from_data)

#     acquisition_task >> analysis_task


with DAG('ecommerce_data_acquisition', start_date=datetime(2025, 4, 1), schedule=None, catchup=False) as dag1:
    acquisition_task = PythonOperator(task_id='data_acquisition', python_callable=load_all_data)

    trigger_analysis = TriggerDagRunOperator(
        task_id='trigger_data_analysis',
        trigger_dag_id='ecommerce_data_analysis',
        start_date=datetime(2025, 4, 1),
        wait_for_completion=False,
    )

    acquisition_task >> trigger_analysis


with DAG('ecommerce_data_analysis', start_date=datetime(2025, 4, 1), schedule=None, catchup=False) as dag2:
    analysis_task = PythonOperator(task_id='data_analysis', python_callable=compute_kpis_from_data)
