from sqlalchemy import create_engine, MetaData, Table, insert
import os

engine = create_engine(os.environ['SQL_CONTACTS_BOT'])
metadata = MetaData()

table_name = 'persons_full'
table = Table(table_name, metadata, autoload_with=engine, schema='extra')


def write_contacts(krs, pesel, fn, mn, ln, role):
    try:
        with engine.begin() as connection:  # Begin a transaction
            # Prepare the insert statement excluding the id column
            insert_stmt = (
                insert(table)
                .values(krs=krs, pesel=pesel, first_name=fn, middle_name=mn, last_name=ln, role_type=role)
            )
            # Execute the insert statement
            result = connection.execute(insert_stmt)
            print(f"Successfully inserted data for KRS {krs}.")
            # TODO: Update contacts_update column in website_data.spolki

    except Exception as e:
        print(f"Error updating contacts for KRS {krs}: {e}")
