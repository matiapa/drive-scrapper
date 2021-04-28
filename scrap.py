from __future__ import print_function
from copy import Error
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import sqlite3
import re
from utils import printc

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
ROOT_FOLDER_ID = '1-4NjxhPLj94bOiZviK0vDjYxiH2E9C0y'
SKIP_PATTERN = '^\..*|^_.*|^src$|^build$'


def authenticate():
    creds = None

    if os.path.exists('keys/token.json'):
        creds = Credentials.from_authorized_user_file('keys/token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open('keys/token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def prepareDatabase(conn, cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS file(path text, id text, link text, owner text,'
        +' PRIMARY KEY(id))')
    conn.commit()


def listFolder(parentPath, folder, conn, cursor, service):
    # Check if folder should be skipped

    folderPath = f"{parentPath}/{folder['name']}"

    if re.search(SKIP_PATTERN, folder['name']) != None:
        printc(f">> Skipping {folderPath}", 'WARNING')
        return

    # Check if folder has been explored

    res = cursor.execute(f"SELECT completed FROM file WHERE id='{folder['id']}'").fetchone()
    if res != None and res[0]:
        printc(f">> Already explored {folderPath}", 'WARNING')
        return

    # Insert folder record

    cursor.execute(
        "INSERT INTO file VALUES (?,?,?,?,'folder','false') ON CONFLICT(id) DO NOTHING",
        (folderPath, folder['id'], folder['webViewLink'], folder['owners'][0]['emailAddress'])
    )

    # List folder content
    
    printc(f'> LISTING {folderPath}', 'HEADER')

    items = service.files().list(
        q=f"'{folder['id']}' in parents",
        fields="files(id,name,mimeType,webViewLink,owners(emailAddress))"
    ).execute().get('files', [])

    for item in items:
        try:
            printc(f">> Inserting file {folderPath}/{item['name']}", 'OKGREEN')

            if item['mimeType'] == 'application/vnd.google-apps.folder':
                # Folder

                conn.commit()   # Commit before going to another folder
                listFolder(folderPath, item, conn, cursor, service)
            
            else:
                # File

                filePath = f"{folderPath}/{item['name']}"
                cursor.execute(
                    "INSERT INTO file VALUES (?,?,?,?,'file','true') ON CONFLICT(id) DO NOTHING",
                    (filePath, item['id'], item['webViewLink'], item['owners'][0]['emailAddress'])
                )
        except Error as error:
            printc(f">> Failed parsing {folderPath}/{item['name']}", 'FAIL')
            printc(f">> {error}", 'FAIL')
            pass

    # Mark folder as explored

    cursor.execute(f"UPDATE file SET completed=true WHERE id='{folder['id']}'")
    conn.commit()
    printc(f'> COMPLETED {folderPath}', 'HEADER')


def main():
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    conn = sqlite3.connect('out/data.db')
    cursor = conn.cursor()
    prepareDatabase(conn, cursor)
    
    root_folder = service.files().get(
        fileId=ROOT_FOLDER_ID,
        fields="id,name,mimeType,webViewLink,owners(emailAddress)"
    ).execute()
    listFolder('', root_folder, conn, cursor, service)

    conn.commit()    
    conn.close()

    print('> DONE')

if __name__ == '__main__':
    main()