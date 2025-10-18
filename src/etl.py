import os
import pandas as pd
import psycopg2
from psycopg2 import extras
from datetime import datetime
from dotenv import load_dotenv
import logging


load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'ecommerce_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

# Paths
RAW_DATA_PATH = 'data/raw/'
INVALID_DATA_PATH = 'data/invalid/'


os.makedirs(INVALID_DATA_PATH, exist_ok=True)


class DataValidator:
    """Handles data validation and cleaning"""
    
    @staticmethod
    def validate_timestamp(timestamp_str):
        """Validate ISO8601 timestamp format"""
        try:
            datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return True
        except (ValueError, AttributeError):
            return False
    
    @staticmethod
    def validate_positive_number(value):
        """Check if value is non-negative"""
        try:
            return float(value) >= 0
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_email(email):
        """Basic email validation"""
        return isinstance(email, str) and '@' in email and '.' in email


class ETLPipeline:
    """Main ETL Pipeline"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def close_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def check_table_exists(self, table_name):
        """Check if table exists in database"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchone()[0]
    
    def validate_users(self, df):
        """Validate users data"""
        valid_rows = []
        invalid_rows = []
        
        seen_ids = set()
        seen_emails = set()
        
        for idx, row in df.iterrows():
            errors = []
            
            # Check for duplicate primary key
            if row['user_id'] in seen_ids:
                errors.append("Duplicate user_id")
            seen_ids.add(row['user_id'])
            
            # Check for duplicate email
            if row['email'] in seen_emails:
                errors.append("Duplicate email")
            seen_emails.add(row['email'])
            
            # Validate email format
            if not DataValidator.validate_email(row['email']):
                errors.append("Invalid email format")
            
            # Validate timestamp
            if not DataValidator.validate_timestamp(str(row['signup_date'])):
                errors.append("Invalid signup_date timestamp")
            
            if errors:
                invalid_row = row.to_dict()
                invalid_row['validation_errors'] = '; '.join(errors)
                invalid_rows.append(invalid_row)
            else:
                valid_rows.append(row.to_dict())
        
        return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)
    
    def validate_products(self, df):
        """Validate products data"""
        valid_rows = []
        invalid_rows = []
        
        seen_ids = set()
        
        for idx, row in df.iterrows():
            errors = []
            
            # Check for duplicate primary key
            if row['product_id'] in seen_ids:
                errors.append("Duplicate product_id")
            seen_ids.add(row['product_id'])
            
            # Validate price
            if not DataValidator.validate_positive_number(row['price']):
                errors.append("Invalid price (must be non-negative)")
            
            if errors:
                invalid_row = row.to_dict()
                invalid_row['validation_errors'] = '; '.join(errors)
                invalid_rows.append(invalid_row)
            else:
                valid_rows.append(row.to_dict())
        
        return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)
    
    def validate_orders(self, df):
        """Validate orders data"""
        valid_rows = []
        invalid_rows = []
        
        seen_ids = set()
        
        for idx, row in df.iterrows():
            errors = []
            
            if row['order_id'] in seen_ids:
                errors.append("Duplicate order_id")
            seen_ids.add(row['order_id'])
            
  
            if not DataValidator.validate_timestamp(str(row['order_date'])):
                errors.append("Invalid order_date timestamp")
            
    
            if not DataValidator.validate_positive_number(row['total_amount']):
                errors.append("Invalid total_amount (must be non-negative)")
            
   
            valid_statuses = ['pending', 'paid', 'cancelled', 'shipped']
            if row['status'] not in valid_statuses:
                errors.append(f"Invalid status (must be one of {valid_statuses})")
            
            if errors:
                invalid_row = row.to_dict()
                invalid_row['validation_errors'] = '; '.join(errors)
                invalid_rows.append(invalid_row)
            else:
                valid_rows.append(row.to_dict())
        
        return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)
    
    def validate_order_items(self, df):
        """Validate order_items data"""
        valid_rows = []
        invalid_rows = []
        
        seen_ids = set()
        
        for idx, row in df.iterrows():
            errors = []
            
  
            if row['order_item_id'] in seen_ids:
                errors.append("Duplicate order_item_id")
            seen_ids.add(row['order_item_id'])
            
            if not (isinstance(row['quantity'], (int, float)) and row['quantity'] > 0):
                errors.append("Invalid quantity (must be positive)")
            
            # Validate unit_price
            if not DataValidator.validate_positive_number(row['unit_price']):
                errors.append("Invalid unit_price (must be non-negative)")
            
            if errors:
                invalid_row = row.to_dict()
                invalid_row['validation_errors'] = '; '.join(errors)
                invalid_rows.append(invalid_row)
            else:
                valid_rows.append(row.to_dict())
        
        return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)
    
    def validate_events(self, df):
        """Validate events data"""
        valid_rows = []
        invalid_rows = []
        
        seen_ids = set()
        
        for idx, row in df.iterrows():
            errors = []
            
            # Check for duplicate primary key
            if row['event_id'] in seen_ids:
                errors.append("Duplicate event_id")
            seen_ids.add(row['event_id'])
            
            # Validate timestamp
            if not DataValidator.validate_timestamp(str(row['event_timestamp'])):
                errors.append("Invalid event_timestamp")
            
            # Validate event_type
            valid_types = ['page_view', 'add_to_cart', 'purchase']
            if row['event_type'] not in valid_types:
                errors.append(f"Invalid event_type (must be one of {valid_types})")
            
            if errors:
                invalid_row = row.to_dict()
                invalid_row['validation_errors'] = '; '.join(errors)
                invalid_rows.append(invalid_row)
            else:
                valid_rows.append(row.to_dict())
        
        return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)
    
    def load_csv_to_db(self, table_name, csv_file, validator_func):
        """Load CSV file into database with validation"""
        logger.info(f"Processing {table_name}...")
        
        # Check if table exists
        if not self.check_table_exists(table_name):
            logger.error(f"Table {table_name} does not exist in database")
            return
        
        # Read CSV
        file_path = os.path.join(RAW_DATA_PATH, csv_file)
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return
        
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {csv_file}")
        
        # Validate data
        valid_df, invalid_df = validator_func(df)
        
        # Log invalid rows
        if not invalid_df.empty:
            invalid_file = os.path.join(INVALID_DATA_PATH, f'invalid_{csv_file}')
            invalid_df.to_csv(invalid_file, index=False)
            logger.warning(f"Found {len(invalid_df)} invalid rows. Logged to {invalid_file}")
        
        # Insert valid rows
        if valid_df.empty:
            logger.warning(f"No valid rows to insert for {table_name}")
            return
        
        # Prepare data for insertion
        columns = valid_df.columns.tolist()
        values = [tuple(row) for row in valid_df.values]
        
        # Create insert query
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        
        try:
            extras.execute_batch(self.cursor, insert_query, values)
            self.conn.commit()
            logger.info(f"Successfully inserted {len(valid_df)} rows into {table_name}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting into {table_name}: {e}")
    
    def run_etl(self):
        """Run the complete ETL pipeline"""
        logger.info("Starting ETL pipeline...")
        
        try:
            self.connect_db()
            
            # Load data in correct order (respecting foreign key constraints)
            self.load_csv_to_db('users', 'users.csv', self.validate_users)
            self.load_csv_to_db('products', 'products.csv', self.validate_products)
            self.load_csv_to_db('orders', 'orders.csv', self.validate_orders)
            self.load_csv_to_db('order_items', 'order_items.csv', self.validate_order_items)
            self.load_csv_to_db('events', 'events.csv', self.validate_events)
            
            logger.info("ETL pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.close_db()


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run_etl()