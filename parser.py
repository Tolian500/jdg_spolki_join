import time

from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams
from download_manager import main as download_krs
from cleaner import full_clean
import re
import os
from sql_manager import write_contacts

# test_krs_list = ['0000398281', '0000573610', '0000735160']
test_krs_list = ['0001038309']


def get_pdf_files():
    # Get the list of all files and directories in the current folder
    files_in_directory = os.listdir(os.getcwd())

    # Filter and return only files with the '.pdf' extension
    pdf_files = [file for file in files_in_directory if file.lower().endswith('.pdf')]

    return pdf_files


# pdf_path = "./Odpis_Aktualny_KRS_0000573610.pdf"

def parse_pdf():
    elements = get_pdf_files()
    for element in elements:
        krs = f"{element.split("_")[3][:10]}"
        print(f"Krs: {krs}")
        text = extract_text(element, laparams=LAParams())
        try:
            x = [m.start() for m in re.finditer('Nazwisko', text)]
            print("X was found")
        except Exception as e:
            print(f"Error with parsing pdf: {e}")
            return None
        # print(x)
        repress = None
        contacts_list = []
        print("Start parsing")
        for index in x:
            try:
                repress = text[index:].split("\n")[16] == "5.Funkcja w organie"
                role = None
            except:
                print("ERROR WITH DEFINING ROLE. Role set to Owner")
                role = "udzial"
            if repress is True:
                role = "repr"
            if repress is False:
                role = "udzial"
            try:
                last_name = text[index:].split("\n")[2]
            except IndexError:
                # Skip iteration if no last name
                continue
            try:
                first_name = text[index:].split("\n")[6].split()[0]
            except IndexError:
                # Skip iteration if no last name
                continue
            try:
                middle_name = text[index:].split("\n")[6].split()[1]
            except IndexError:
                print("Error with middle name")
                middle_name = None
            # print(f'Middle name: {middle_name}')
            try:
                pesel = text[index:].split("\n")[10].split()[0][:-1]
            except IndexError:
                print("Error with middle pessel")
                pesel = None
            # print(f"Pesel: {pesel}")
            # print("---\n")
            contacts_list.append([krs, pesel, first_name, middle_name, last_name, role])
        print("Parsing done successfully")
        return contacts_list


def main(krs_list: list, return_contacts: bool = False):
    if return_contacts:
        # converting one elemment to list with 1 element
        krs_list = [krs_list]
    print("Parser.py start")
    print(f"Return contacts: {return_contacts}")
    download_krs(krs_list)
    all_contacts = parse_pdf()
    # print(all_contacts)
    for contact in all_contacts:
        write_contacts(*contact)
    full_clean()
    if return_contacts:
        print("Returning contacts")
        return all_contacts

if __name__ == "__main__":
    main(test_krs_list)
