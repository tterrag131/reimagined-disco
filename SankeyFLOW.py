import pandas as pd
import os
from datetime import datetime
import win32com.client as win32 # Used to interact with the Excel Application
import time

def process_sankey_data(excel_file_path, sheet_name="SANKEY", output_folder="HISTORICALS"):
    """
    Reads data from a specific sheet in an Excel file, prints it to the console,
    and saves it as a timestamped CSV file in a specified folder.

    Args:
        excel_file_path (str): The full path to the .xlsm Excel file.
        sheet_name (str): The name of the sheet to read data from.
        output_folder (str): The name of the folder to save the CSV file in.
    """
    try:
        # --- 1. Read the Excel Sheet ---
        # We use openpyxl as the engine because .xlsm is a macro-enabled format.
        print(f"Reading sheet '{sheet_name}' from '{excel_file_path}'...")
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

        # --- 2. Print Data to Terminal ---
        print("\n--- Data from SANKEY sheet ---")
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        print(df)
        print("\n------------------------------\n")

        # --- 3. Create Historicals Folder ---
        if not os.path.exists(output_folder):
            print(f"Creating directory: '{output_folder}'")
            os.makedirs(output_folder)

        # --- 4. Save Timestamped CSV ---
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        csv_filename = f"SANKEY_DATA_{timestamp}.csv"
        csv_filepath = os.path.join(output_folder, csv_filename)

        df.to_csv(csv_filepath, index=False)
        print(f"Successfully saved data to: '{csv_filepath}'")

    except FileNotFoundError:
        print(f"Error: The file '{excel_file_path}' was not found.")
        print("Please ensure the script is in the same directory as the Excel file.")
    except Exception as e:
        print(f"An unexpected error occurred during data processing: {e}")

def run_excel_macro(excel_file_path, macro_name):
    """
    Connects to or opens Excel, runs a specified macro, and leaves the application open.

    Args:
        excel_file_path (str): The full path to the .xlsm Excel file.
        macro_name (str): The name of the macro to run (e.g., "packman.RunAll").
    """
    excel = None
    workbook = None
    try:
        print(f"\nAttempting to run macro '{macro_name}'...")
        print("This may take a moment. Python will wait for the macro to finish.")
        
        # Try to connect to a running instance of Excel
        try:
            excel = win32.GetActiveObject("Excel.Application")
            print("Connected to existing Excel instance.")
        except Exception:
            # If Excel is not running, start a new instance
            excel = win32.Dispatch("Excel.Application")
            print("Started a new Excel instance.")
        
        excel.Visible = True

        # Get the absolute path for Excel
        abs_excel_path = os.path.abspath(excel_file_path)
        workbook_name = os.path.basename(excel_file_path)
        
        # Check if the workbook is already open in this instance
        try:
            workbook = excel.Workbooks(workbook_name)
        except Exception:
            workbook = excel.Workbooks.Open(abs_excel_path)
        
        # --- Run the Macro ---
        # The Run method is synchronous, meaning Python will pause here
        # and wait until the VBA macro has finished executing.
        excel.Application.Run(macro_name)
        
        # Give Excel a moment to save/process after the macro
        time.sleep(2) 
        
        workbook.Save()
        print(f"Macro '{macro_name}' executed and workbook saved.")
        print("Excel will remain open.")

    except Exception as e:
        print(f"An error occurred while trying to run the Excel macro: {e}")
    # We no longer have a 'finally' block, so Excel will not be closed by the script.

if __name__ == "__main__":
    # --- Configuration ---
    excel_filename = "OB WIP Tool.V13.4.xlsm"
    # The macro name should be in the format 'ModuleName.SubName'
    vba_macro_name = "packman.RunAll" 

    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_excel_path = os.path.join(script_dir, excel_filename)

    # --- Initial Run ---
    # First, process the data as it currently exists in the file.
    process_sankey_data(full_excel_path)

    # --- Refresh Loop ---
    while True:
        # Ask the user if they want to refresh the data
        user_input = input("\nWould you like to refresh the data by running the macro? (yes/no): ").lower()

        if user_input == 'yes':
            # Run the macro to update the data in the Excel sheet
            run_excel_macro(full_excel_path, vba_macro_name)
            
            # Re-process the data to capture the updates
            print("\nReprocessing data after refresh...")
            process_sankey_data(full_excel_path)
        elif user_input == 'no':
            print("Exiting script. Please close Excel manually when you are finished.")
            break
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
