import csv
import os
import time
import pandas
import pandas as pd
import psycopg2  # Import PostgreSQL library

# Specify the path to your CSV file
file_path = 'krs_spolki_2years.csv'

# Connection string
connection_string = os.environ['SQL_CONTACTS_BOT']

COUNT_LIMIT = 5

def clear_and_resave(dataframe: pandas.DataFrame):
    # Remove the first row from the DataFrame
    dataframe = dataframe.iloc[1:]

    # Save the updated DataFrame back to the CSV file
    dataframe.to_csv(file_path, index=False)


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
    unique_contacts = set()  # To track unique contacts
    for contact in contacts:
        first_name = contact[4]
        last_name = contact[6]
        if first_name and last_name:  # Ensure no None values
            contact_key = (first_name, last_name)  # Create a unique key for each contact
            if contact_key not in unique_contacts:  # Check for duplicates
                unique_contacts.add(contact_key)  # Add to the set to track it
                query_parts.append(f"('{first_name}', '{last_name}')")

    if not query_parts:  # If no valid contacts, return
        return

    # Create a string from the contact pairs
    contacts_str = ', '.join(query_parts)
    print(len(contacts_str))
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

    # Execute the new query
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(new_query)

    # Fetch the results
    results = cursor.fetchall()
    cursor.close()  # Close the cursor
    conn.close()  # Close the connection

    if len(results) == 0:
        print("Contacts matching while join tables was found")
        return None

    # Filter out duplicates from the results
    unique_results = list(set(results))  # Convert to set and back to list to remove duplicates
    print("Contacts matching while join tables:", unique_results)
    return unique_results


def save_contacts_to_db(full_contacts):
    # Prepare the insert query
    insert_query = """
    INSERT INTO extra.contacts_owners (nazwisko, imie, email, krs, first_name, last_name)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    batch_size = 100  # Number of contacts to insert before committing
    contact_count = 0  # Track the number of contacts inserted

    with psycopg2.connect(connection_string) as conn:
        print("Start saving contacts")
        with conn.cursor() as cursor:
            for contact in full_contacts:
                try:
                    # Unpack the contact details
                    nazwisko = contact[0]
                    imie = contact[1]
                    email = contact[2]
                    krs = contact[3]
                    first_name = contact[4]
                    last_name = contact[5]

                    # Execute the insert for each contact
                    cursor.execute(insert_query, (nazwisko, imie, email, krs, first_name, last_name))
                    contact_count += 1

                    # Commit every batch_size contacts
                    if contact_count % batch_size == 0:
                        conn.commit()
                        print(f"Committed {contact_count} contacts.")

                except psycopg2.Error as e:
                    print(f"Error saving contacts to DB: {e}")

            # Commit any remaining contacts that didn't fill the batch
            if contact_count % batch_size != 0:
                conn.commit()
                print(f"Committed remaining {contact_count % batch_size} contacts.")

    print(f"Inserted {len(full_contacts)} contacts into the database.")

def main():
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path, dtype={'krs': str})  # Ensure 'krs' is read as string
    # Check if the DataFrame is not empty
    if not df.empty:
        count = 0
        start_time = time.time()
        while True:
            if count == COUNT_LIMIT:
                # Send discord, update count
                count = 0
                end_time = time.time()
                print(f"Time spend for {COUNT_LIMIT} elements: {start_time-end_time} s.")
                start_time = time.time()
            try:
                count += 1
                # Read the first value
                curr_krs = df.iloc[0]['krs']
                print(f"Processing value: {curr_krs}")
                contacts = find_contacts_for_krs(curr_krs)
                if contacts is None:
                    print("Contacts is none")
                    clear_and_resave(df)  # Clear in the future
                    continue
                print("Contacts found. Try fined FUll contacts")
                # Use the contacts for another query
                full_contacts = find_additional_contacts(contacts)
                if full_contacts is None:
                    print("Full contacts is none")
                    clear_and_resave(df)  # Clear in the future
                    continue  # Break in the future
                save_contacts_to_db(full_contacts)
                clear_and_resave(df)  # Clear in the future
            except Exception as e:
                print(e)

        print("First value processed and row deleted.")
    else:
        print("All values have been processed. Exiting.")
        # SEND DISCORD NOTIF


if __name__ == "__main__":
    main()