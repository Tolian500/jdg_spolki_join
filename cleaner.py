import os
import signal
import subprocess


def clean_folder_from_pdfs():
    # List all files in the current directory
    files_in_directory = os.listdir(os.getcwd())

    # Remove all files with .pdf and .doc extensions
    files_count = 0
    for file_name in files_in_directory:
        if file_name.lower().endswith(('.pdf', '.doc')):
            # Delete the file
            os.remove(file_name)
            print(f"Deleted: {file_name}")
            files_count += 1  # Correctly increment the count

    print(f"All .pdf and .doc files have been deleted. Total files deleted: {files_count}")
    return files_count


def kill_all_firefox():
    # Find all processes related to 'firefox-bin'
    processes = subprocess.Popen(['pgrep', '-f', 'firefox-bin'], stdout=subprocess.PIPE)

    closed_count = 0  # Counter for closed processes

    for pid in processes.stdout:
        try:
            os.kill(int(pid.strip()), signal.SIGKILL)
            closed_count += 1  # Increment count if the process is killed
        except Exception as e:
            print(f"Error killing process {pid}: {e}")

    print(f"{closed_count} firefox processes were closed")


def full_clean():
    clean_folder_from_pdfs()
    try:
        kill_all_firefox()
    except Exception as e:
        print("Probably not on Linux")
        print(e)



