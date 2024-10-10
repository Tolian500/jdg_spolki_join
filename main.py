import os
import time
import pandas as pd
import psycopg2  # Import PostgreSQL library
import parser
import asyncio
from discord_manager import send_message as discord_send_message

# Specify the path to your CSV file
file_path = 'krs_spolki_2years.csv'

# Connection string
connection_string = os.environ['SQL_CONTACTS_BOT']

COUNT_LIMIT = 500 # Around every 20 minutes


def clear_and_resave(dataframe: pd.DataFrame):
    # Remove the first row from the DataFrame
    dataframe = dataframe.iloc[1:]

    # Save the updated DataFrame back to the CSV file
    dataframe.to_csv(file_path, index=False)
    return dataframe


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
    # Prepare a list of contact queries
    query_parts = []
    for contact in contacts:
        first_name = contact[0].strip()  # Extract and trim first name
        last_name = contact[1].strip()  # Extract and trim last name
        if first_name and last_name:  # Ensure valid names
            # Append the ILIKE conditions to the query
            query_parts.append(f"(nazwisko ILIKE '{last_name}' AND imie ILIKE '{first_name}')")

    if not query_parts:  # If no valid contacts, return
        return

    print(f"Query parts: {query_parts}")
    # Join all conditions with OR to search for all contacts at once
    conditions = " OR ".join(query_parts)

    # Define the query
    new_query = f"""
    SELECT id, nazwisko, imie, email 
    FROM main_data.firma_nd_email 
    WHERE {conditions};
    """

    # Execute the query
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(new_query)

        # Fetch the results
        results = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        if len(results) == 0:
            print("No contacts found.")
            return None

        print(f"Contacts found. Results: {results}")
        return results

    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def save_contacts_to_db(full_contacts):
    # Prepare the insert query
    insert_query = """
    INSERT INTO extra.contacts_owners (ceidg_id, nazwisko, imie, email, krs)
    VALUES (%s, %s, %s, %s, %s);
    """

    with psycopg2.connect(connection_string) as conn:
        print("Start saving contacts")
        with conn.cursor() as cursor:
            for contact in full_contacts:
                try:
                    # Unpack the contact details
                    ceidg_id = contact[1]
                    nazwisko = contact[2]
                    imie = contact[3]
                    email = contact[4]
                    krs = contact[0]

                    # Execute the insert for each contact
                    cursor.execute(insert_query, (ceidg_id, nazwisko, imie, email, krs))
                    conn.commit()
                    print(f"Committed contact for: {contact}")

                except psycopg2.Error as e:
                    print(f"Error saving contacts to DB: {e}")

    print(f"Inserted {len(full_contacts)} contacts into the database.")


async def process_krs(df):
    count = 0
    success_count = 0
    start_time = time.time()
    while True:
        if count == COUNT_LIMIT:
            end_time = time.time()
            message = (f"👩‍💼✉️ JPG_SPOŁKI_JOIN: Time spent for {COUNT_LIMIT} elements: {int(end_time - start_time)} s."
                       f" Success = {success_count}/{count}\n"
                       f"⚙️ Seconds per success = {(end_time - start_time) / success_count:.2f}s.")
            print(message)
            await discord_send_message(message=message, silent=True)
            # Restart counters and timers
            start_time = time.time()
            count = 0
            success_count = 0

        try:
            count += 1
            curr_krs = df.iloc[0]['krs']
            print(f"Processing value: {curr_krs}")
            all_contacts_list = parser.main(curr_krs, return_contacts=True)
            all_names_list = [[item[2], item[4], item[1]] for item in all_contacts_list]
            unique_all_names = set(tuple(contact) for contact in all_names_list)
            if unique_all_names is None:
                print("♻️ Contacts is none. Clearing and starting new round")
                df = clear_and_resave(df)
                continue
            full_contacts = find_additional_contacts(unique_all_names)
            if full_contacts is None:
                print("♻️ No full contacts (mail) found. Clearing and starting new round")
                df = clear_and_resave(df)
                continue
            formatted_full_contacts = [(curr_krs,) + item for item in full_contacts]
            save_contacts_to_db(formatted_full_contacts)
            df = clear_and_resave(df)
            print("🟩 SUCCESS! Saving results. Clearing and starting new round")
            success_count += 1
        except Exception as e:
            print(e)


async def main_async():
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path, dtype={'krs': str})
    if not df.empty:
        await process_krs(df)
    else:
        print("All values have been processed. Exiting.")
        await discord_send_message(message="All KRS entries have been processed.", silent=True)


if __name__ == "__main__":
    asyncio.run(main_async())
