import re

# clean column names
def clean_column_name(column_name):
    column_name = column_name.strip()
    #column_name = re.sub(r"[^\w\s]", "", column_name)
    column_name = re.sub(r"\n", "", column_name)
    column_name = re.sub(r"\s+", "_", column_name)
    return column_name