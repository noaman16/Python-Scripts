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

# Decryption function using AES
def decrypt_data(encrypted_data, key):
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
    decryptor = cipher.decryptor()

    # Decode the base64-encoded encrypted data
    encrypted_data_bytes = base64.b64decode(encrypted_data)

    decrypted_padded_data = decryptor.update(encrypted_data_bytes) + decryptor.finalize()

    # Unpad the decrypted data
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    decrypted_data = unpadder.update(decrypted_padded_data) + unpadder.finalize()

    decrypted_string = decrypted_data.decode('utf-8')
    # Commented out print statements for each decryption
    # print(f"Encrypted: {encrypted_data} -> Decrypted: {decrypted_string}")
    return decrypted_string

# Create a database connection using SQLAlchemy
db_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)

# Metadata object to hold table information
metadata = MetaData()

# Define the tables
finance_reco = Table('finance_reco', metadata, autoload_with=engine)
finance_deals = Table('finance_deals', metadata, autoload_with=engine)

# Function to decrypt specific columns in the finance_reco table
def decrypt_finance_reco_columns():
    with engine.begin() as connection:  # Use engine.begin() for automatic commit/rollback
        session = Session(bind=connection)
        
        # Fetch records
        select_stmt = finance_reco.select()
        results = session.execute(select_stmt).fetchall()

        for row in results:
            update_data = {}
            # print(f"Processing row ID {row._mapping['id']}")

            # Decrypt 'customer_name' if it's not None
            if row._mapping['customer_name']:
                update_data['customer_name'] = decrypt_data(row._mapping['customer_name'], key)

            # Decrypt 'salesperson_name' if it's not None
            if row._mapping['salesperson_name']:
                update_data['salesperson_name'] = decrypt_data(row._mapping['salesperson_name'], key)

            # Decrypt 'submission_name' if it's not None
            if row._mapping['submission_name']:
                update_data['submission_name'] = decrypt_data(row._mapping['submission_name'], key)

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

# Function to decrypt the customer_name column in the finance_deals table
def decrypt_finance_deals_customer_name():
    with engine.begin() as connection:  # Use engine.begin() for automatic commit/rollback
        session = Session(bind=connection)

        # Fetch records
        select_stmt = finance_deals.select()
        results = session.execute(select_stmt).fetchall()

        for row in results:
            print(f"Processing row ID {row._mapping['id']}")

            if row._mapping['customer_name']:
                decrypted_customer_name = decrypt_data(row._mapping['customer_name'], key)

                # Commented out the Update the record with decrypted data
                # print(f"Updating row ID {row._mapping['id']} with decrypted customer_name")
                update_stmt = (
                    finance_deals.update()
                    .where(finance_deals.c.id == row._mapping['id'])
                    .values(customer_name=decrypted_customer_name)
                )
                session.execute(update_stmt)

# Run the decryption for both tables
if __name__ == "__main__":
    print("Starting decryption for finance_reco table...")
    decrypt_finance_reco_columns()
    
    print("Starting decryption for finance_deals table...")
    decrypt_finance_deals_customer_name()
    
    print("Decryption process completed.")
