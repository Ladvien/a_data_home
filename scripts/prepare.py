import os

"""
Prepare a copy of the iMessage database and address book database
"""
os.system("cp ~/Library/Messages/chat.db a_data_home/dbs/chat.db")
os.system(
    """cp ~/Library/Application\\ Support/AddressBook/AddressBook-v22.abcddb a_data_home/dbs/AddressBook-v22.abcddb"""
)
