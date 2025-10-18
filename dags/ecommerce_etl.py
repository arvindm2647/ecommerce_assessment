from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import psycopg2
from psycopg2 import extras

default_args = {
    'owner': 'ecommerce',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for e-commerce data',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'etl'],
)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'host.docker.internal'),
    'port': int(os.getenv('DB_PORT', 5434)),
    'database': os.getenv('DB_NAME', 'ecommerce_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '12345')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_csv_to_db(**context):
    print("Starting CSV load task...")
    print(f"Database config: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    csv_files = {
        'users': '/opt/airflow/data/raw/users.csv',
        'products': '/opt/airflow/data/raw/products.csv',
        'orders': '/opt/airflow/data/raw/orders.csv',
        'order_items': '/opt/airflow/data/raw/order_items.csv',
        'events': '/opt/airflow/data/raw/events.csv'
    }
    
    try:
        cursor.execute("TRUNCATE TABLE events, order_items, orders, products, users RESTART IDENTITY CASCADE;")
        conn.commit()
        print("All tables truncated")
        
        for table, file_path in csv_files.items():
            print(f"Loading {table} from {file_path}")
            df = pd.read_csv(file_path)
            
            columns = df.columns.tolist()
            values = [tuple(row) for row in df.values]
            placeholders = ','.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
            
            extras.execute_batch(cursor, insert_query, values)
            conn.commit()
            print(f"Loaded {len(df)} rows into {table}")
        
        print("CSV load completed successfully!")
        return "CSV_LOAD_SUCCESS"
        
    except Exception as e:
        conn.rollback()
        print(f"Error loading CSV: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def build_sql_views(**context):
    print("Building SQL views...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("DROP VIEW IF EXISTS v_daily_revenue CASCADE;")
        
        view_query = """
        CREATE VIEW v_daily_revenue AS
        SELECT 
            DATE(order_date) AS order_day,
            SUM(total_amount) AS daily_revenue,
            COUNT(order_id) AS total_orders
        FROM orders
        WHERE status = 'paid'
        GROUP BY DATE(order_date)
        ORDER BY order_day DESC;
        """
        cursor.execute(view_query)
        
        cursor.execute("DROP VIEW IF EXISTS v_top_products CASCADE;")
        
        view_query2 = """
        CREATE VIEW v_top_products AS
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            SUM(oi.quantity) AS total_units_sold,
            SUM(oi.quantity * oi.unit_price) AS total_revenue
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.status = 'paid'
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_revenue DESC;
        """
        cursor.execute(view_query2)
        
        conn.commit()
        print("SQL views created successfully!")
        return "VIEWS_CREATED"
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating views: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def run_tests(**context):
    print("Running data quality tests...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    tests_passed = True
    
    try:
        cursor.execute("SELECT COUNT(*) FROM users;")
        user_count = cursor.fetchone()[0]
        print(f"Users count: {user_count}")
        if user_count == 0:
            print("FAIL: No users in database")
            tests_passed = False
        
        cursor.execute("SELECT COUNT(*) FROM products;")
        product_count = cursor.fetchone()[0]
        print(f"Products count: {product_count}")
        if product_count == 0:
            print("FAIL: No products in database")
            tests_passed = False
        
        cursor.execute("SELECT COUNT(*) FROM orders WHERE total_amount < 0;")
        negative_orders = cursor.fetchone()[0]
        print(f"Orders with negative amounts: {negative_orders}")
        if negative_orders > 0:
            print("FAIL: Found orders with negative amounts")
            tests_passed = False
        
        cursor.execute("SELECT COUNT(*) FROM events WHERE event_type NOT IN ('page_view', 'add_to_cart', 'purchase');")
        invalid_events = cursor.fetchone()[0]
        print(f"Invalid event types: {invalid_events}")
        if invalid_events > 0:
            print("FAIL: Found invalid event types")
            tests_passed = False
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_items oi
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_id IS NULL;
        """)
        orphan_items = cursor.fetchone()[0]
        print(f"Orphan order items: {orphan_items}")
        if orphan_items > 0:
            print("FAIL: Found orphan order items")
            tests_passed = False
        
        if not tests_passed:
            raise Exception("Data quality tests failed!")
        
        print("All tests passed!")
        return "TESTS_PASSED"
        
    except Exception as e:
        print(f"Test error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def materialize_daily_revenue(**context):
    print("Materializing daily revenue to CSV...")
    
    conn = get_db_connection()
    
    try:
        query = "SELECT * FROM v_daily_revenue ORDER BY order_day;"
        df = pd.read_sql(query, conn)
        
        output_path = '/opt/airflow/output/daily_revenue.csv'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df.to_csv(output_path, index=False)
        
        print(f"Daily revenue materialized to {output_path}")
        print(f"Total rows: {len(df)}")
        if len(df) > 0:
            print(f"Total revenue: ${df['daily_revenue'].sum():.2f}")
        
        return "MATERIALIZATION_SUCCESS"
        
    except Exception as e:
        print(f"Materialization error: {e}")
        raise
    finally:
        conn.close()

task_load_csv = PythonOperator(
    task_id='load_csv_to_database',
    python_callable=load_csv_to_db,
    dag=dag,
)

task_build_views = PythonOperator(
    task_id='build_sql_views',
    python_callable=build_sql_views,
    dag=dag,
)

task_run_tests = PythonOperator(
    task_id='run_data_quality_tests',
    python_callable=run_tests,
    dag=dag,
)

task_materialize = PythonOperator(
    task_id='materialize_daily_revenue',
    python_callable=materialize_daily_revenue,
    dag=dag,
)

task_load_csv >> task_build_views >> task_run_tests >> task_materialize