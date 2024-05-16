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
    #If the new data file can't be unidentified then raise an error so the person running this code can fix it
    if len(latest_file) != 1:
        #If there are no xlsb files found
        if len(latest_file) == 0:
            raise Exception(f"No matching excel file {ext} found in the directory.")
        #If there are multiple xlsb files found
        if len(latest_file) > 1:
            raise Exception(f"Error, multiple matching excel files {ext} found in the directory or the target file is open and locked.")


    #Return the file name from the latest_file variable
    return path.join(dir,latest_file[0])

#Archive the data file
def archive_data_file(data_file, dir, ds, date_label):
    #Rename the current file to archive it
    new_filename = f"{dir}/archive/{ds} {date_label}.xlsx"
    rename(data_file, new_filename)