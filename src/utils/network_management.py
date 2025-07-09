from os import listdir, path, rename

#Check the provided directory for a specified excel file and return the name
def fetch_excel_file (dir, ext=""):
    # Get the latest file
    # This checks every file in the current directory using the listdir function 
    # and then returns only files that end in the ext if specified 

    if ext == "":
        latest_file = [file for file in listdir(dir) if (".x" in file) or file.endswith(".csv")] 
    else:
        latest_file = [file for file in listdir(dir) if file.endswith(ext)]

    #Error checking to make sure the new data file is in the directory as expected.

    #If there are no xlsb files found
    if len(latest_file) == 0:
        raise Exception(f"No matching excel file {ext} found in the directory.")
        
    return latest_file

#Archive the data file
def archive_data_file(data_file, dir, ds, date_label, ext=".xlsx"):
    #Rename the current file to archive it
    new_filename = f"{dir}/archive/{ds} {date_label}{ext}"
    rename(data_file, new_filename)