import csv
import os

import pandas
import pandas as pd
import psycopg2  # Import PostgreSQL library

# Specify the path to your CSV file
file_path = 'krs_spolki_2years.csv'

# Connection string
connection_string = os.environ['SQL_CONTACTS_BOT']
print(connection_string)


def clear_krs(krs: str, dataframe: pandas.DataFrame):
    # Remove the first row from the DataFrame
    dataframe = dataframe.iloc[1:]

    # Save the updated DataFrame back to the CSV file
    df.to_csv(file_path, index=False)


def find_contacts_for_krs(krs):
    # Connect to the PostgreSQL database using the connection string
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    # Use curr_krs in the SQL query
    query = "SELECT * FROM extra.persons WHERE krs = %s;"  # Use %s for parameterized query
    cursor.execute(query, (krs,))  # Pass krs as parameter

    # Fetch the results
    results = cursor.fetchall()
    cursor.close()  # Close the cursor
    conn.close()  # Close the connection

    if not results:
        print("Contacts not found")
        return None

    print("Query results:", results)
    return results


def find_additional_contacts(contacts):
    if contacts is None:
        return

    # Prepare a list of parameters for the new query
    query_parts = []
    for contact in contacts:
        first_name = contact[4]
        last_name = contact[6]
        if first_name and last_name:  # Ensure no None values
            query_parts.append(f"('{first_name}', '{last_name}')")

    if not query_parts:  # If no valid contacts, return
        return

    # Create a string from the contact pairs
    contacts_str = ', '.join(query_parts)
    # Define the new query
    new_query = f"""
    SELECT f.nazwisko, f.imie, f.email, p.krs, p.first_name, p.last_name 
    FROM main_data.firma_nd_email f 
    JOIN extra.persons p 
    ON (f.imie ILIKE (LEFT(p.first_name, 1) || '%')  
    AND f.nazwisko ILIKE (LEFT(p.last_name, 1) || '%')  
    AND LENGTH(f.imie) = (LENGTH(p.first_name) + LENGTH(REPLACE(p.first_name, '*', '')) - 1)  
    AND LENGTH(f.nazwisko) = (LENGTH(p.last_name) + LENGTH(REPLACE(p.last_name, '*', '')) - 1)
    )  -- Close the ON clause here
    WHERE (p.first_name, p.last_name) IN ({contacts_str});
    """
    print(new_query)

    # Execute the new query
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(new_query)

    # Fetch the results
    results = cursor.fetchall()
    cursor.close()  # Close the cursor
    conn.close()  # Close the connection

    if not results:
        print("Contacts matching while join tables was found")
        return None

    print("Contacts matching while join tables:", results)
    return results


def save_contacts_to_db(full_contacts):
    # Prepare the insert query
    insert_query = """
    INSERT INTO extra.contacts_owners (nazwisko, imie, email, krs, first_name, last_name)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    # Execute the insert query for each contact
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            try:
                for contact in full_contacts:
                    # Unpack the contact details
                    nazwisko = contact[0]  # Assuming this is at index 0
                    imie = contact[1]  # Assuming this is at index 1
                    email = contact[2]  # Assuming this is at index 2
                    krs = contact[3]  # Assuming this is at index 3
                    first_name = contact[4]  # Assuming this is at index 4
                    last_name = contact[5]  # Assuming this is at index 5

                    # Execute the insert for each contact
                    cursor.execute(insert_query, (nazwisko, imie, email, krs, first_name, last_name))

                conn.commit()  # Commit the transaction
                print(f"Inserted {len(full_contacts)} contacts into the database.")
            except psycopg2.Error as e:
                print(f"Error saving contacts to DB: {e}")


# Load the CSV file into a DataFrame
df = pd.read_csv(file_path, dtype={'krs': str})  # Ensure 'krs' is read as string

# Check if the DataFrame is not empty
if not df.empty:
    # Read the first value
    curr_krs = df.iloc[0]['krs']
    print(type(curr_krs))
    print(f"Processing value: {curr_krs}")
    curr_krs = "0000759929"

    contacts = find_contacts_for_krs(curr_krs)
    if contacts is None:
        # clear_krs(curr_krs, df)  # Clear in the future
        pass  # Break in the future

    # Use the contacts for another query
    full_contacts = find_additional_contacts(contacts)
    if full_contacts is None:
        # clear_krs(curr_krs, df)  # Clear in the future
        pass  # Break in the future
    save_contacts_to_db(full_contacts)

    print("First value processed and row deleted.")
else:
    print("All values have been processed. Exiting.")
