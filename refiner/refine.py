import json
import logging
import os
import zipfile

from refiner.models.offchain_schema import OffChainSchema
from refiner.models.output import Output
from refiner.transformer.activity_transformer import ActivityTransformer
from refiner.config import settings
from refiner.utils.encrypt import encrypt_file
from refiner.utils.ipfs import upload_file_to_ipfs, upload_json_to_ipfs

class Refiner:
    def __init__(self):
        self.db_path = os.path.join(settings.OUTPUT_DIR, 'db.libsql')

    def transform(self) -> Output:
        """Transform ViewingActivity.csv and account.json from a zip input into the database."""
        logging.info("Starting data transformation")
        output = Output()

        # Find a zip file in the input directory
        zip_path = None
        for fname in os.listdir(settings.INPUT_DIR):
            if fname.lower().endswith('.zip'):
                zip_path = os.path.join(settings.INPUT_DIR, fname)
                break
        if not zip_path:
            raise FileNotFoundError("No zip file found in input directory")
        logging.info(f"Found zip file: {zip_path}")

        # Extract the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(settings.INPUT_DIR)
        logging.info(f"Extracted zip file: {zip_path}")
        logging.info(f"Files in input dir after extraction: {os.listdir(settings.INPUT_DIR)}")

        # Find ViewingActivity.csv and account.json
        csv_filename = os.path.join(settings.INPUT_DIR, 'ViewingActivity.csv')
        account_filename = os.path.join(settings.INPUT_DIR, 'account.json')
        logging.info(f"Looking for CSV at: {csv_filename}")
        logging.info(f"Looking for account JSON at: {account_filename}")
        if not os.path.exists(csv_filename):
            logging.error(f"ViewingActivity.csv not found at: {csv_filename}")
            raise FileNotFoundError("ViewingActivity.csv not found in extracted zip")
        if not os.path.exists(account_filename):
            logging.error(f"account.json not found at: {account_filename}")
            raise FileNotFoundError("account.json not found in extracted zip")

        # Read the address from account.json
        with open(account_filename, 'r') as f:
            account_data = json.load(f)
            address = account_data.get('user')
            if not address:
                raise ValueError("No 'user' field found in account.json")

        # Transform activity data
        transformer = ActivityTransformer(self.db_path)
        transformer.process({'csv_path': csv_filename, 'address': address})
        logging.info(f"Transformed {csv_filename} for address {address}")

        # Create a schema based on the SQLAlchemy schema
        schema = OffChainSchema(
            name=settings.SCHEMA_NAME,
            version=settings.SCHEMA_VERSION,
            description=settings.SCHEMA_DESCRIPTION,
            dialect=settings.SCHEMA_DIALECT,
            schema=transformer.get_schema()
        )
        output.schema = schema

        # Upload the schema to IPFS
        schema_file = os.path.join(settings.OUTPUT_DIR, 'schema.json')
        with open(schema_file, 'w') as f:
            json.dump(schema.model_dump(), f, indent=4)
            schema_ipfs_hash = upload_json_to_ipfs(schema.model_dump())
            logging.info(f"Schema uploaded to IPFS with hash: {schema_ipfs_hash}")

        # Encrypt and upload the database to IPFS
        encrypted_path = encrypt_file(settings.REFINEMENT_ENCRYPTION_KEY, self.db_path)
        ipfs_hash = upload_file_to_ipfs(encrypted_path)
        output.refinement_url = f"{settings.IPFS_GATEWAY_URL}/{ipfs_hash}"

        logging.info("Data transformation completed successfully")
        return output