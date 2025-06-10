import os

"""
Prepare a copy of the iMessage database and address book database
"""
os.system("cp ~/Library/Messages/chat.db dbt/dbs")
os.system(
    """cp ~/Library/Application\\ Support/AddressBook/AddressBook-v22.abcddb dbt/dbs"""
)
