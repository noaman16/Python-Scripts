from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker
import base64
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get encryption key and database credentials from the .env file
encryption_key = os.getenv('ENCRYPTION_KEY')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Decode the base64-encoded encryption key (removing 'base64:' prefix)
key = base64.b64decode(encryption_key.replace('base64:', ''))

# Encryption function using AES
def encrypt_data(data, key):
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
    encryptor = cipher.encryptor()

    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data.encode()) + padder.finalize()

    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    encrypted_b64 = base64.b64encode(encrypted_data).decode('utf-8')

    # Commented out print statements for each encryption
    # print(f"Original: {data} -> Encrypted: {encrypted_b64}")
    return encrypted_b64

# Create a database connection using SQLAlchemy
db_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)

# Metadata object to hold table information
metadata = MetaData()

# Define the tables
finance_reco = Table('finance_reco', metadata, autoload_with=engine)
finance_deals = Table('finance_deals', metadata, autoload_with=engine)

# Function to encrypt specific columns in the finance_reco table
def encrypt_finance_reco_columns():
    with engine.begin() as connection:  # Use engine.begin() for automatic commit/rollback
        session = Session(bind=connection)
        
        # Fetch records
        select_stmt = finance_reco.select()
        results = session.execute(select_stmt).fetchall()

        for row in results:
            update_data = {}

            # Encrypt 'customer_name' if it's not None
            if row._mapping['customer_name']:
                update_data['customer_name'] = encrypt_data(row._mapping['customer_name'], key)

            # Encrypt 'salesperson_name' if it's not None
            if row._mapping['salesperson_name']:
                update_data['salesperson_name'] = encrypt_data(row._mapping['salesperson_name'], key)

            # Encrypt 'submission_name' if it's not None
            if row._mapping['submission_name']:
                update_data['submission_name'] = encrypt_data(row._mapping['submission_name'], key)

            # Update the record if there is any data to update
            if update_data:
                # Commented out print statements for each update
                # print(f"Updating row ID {row._mapping['id']} with data: {update_data}")
                update_stmt = (
                    finance_reco.update()
                    .where(finance_reco.c.id == row._mapping['id'])
                    .values(update_data)
                )
                session.execute(update_stmt)

# Function to encrypt the customer_name column in the finance_deals table
def encrypt_finance_deals_customer_name():
    with engine.begin() as connection:  # Use engine.begin() for automatic commit/rollback
        session = Session(bind=connection)

        # Fetch records
        select_stmt = finance_deals.select()
        results = session.execute(select_stmt).fetchall()

        for row in results:
            if row._mapping['customer_name']:
                encrypted_customer_name = encrypt_data(row._mapping['customer_name'], key)

                # Commented out print statements for each update
                # print(f"Updating row ID {row._mapping['id']} with encrypted customer_name")
                update_stmt = (
                    finance_deals.update()
                    .where(finance_deals.c.id == row._mapping['id'])
                    .values(customer_name=encrypted_customer_name)
                )
                session.execute(update_stmt)

# Run the encryption for both tables
if __name__ == "__main__":
    print("Starting encryption for finance_reco table...")
    encrypt_finance_reco_columns()
    
    print("Starting encryption for finance_deals table...")
    encrypt_finance_deals_customer_name()
    
    print("Encryption process completed.")
