import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email import policy
from email.parser import BytesParser
import mysql.connector
from mysql.connector.cursor import MySQLCursor
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def main():
    print("Starting the script...", flush=True)

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds)
        results = service.users().messages().list(userId='me', maxResults=20).execute()
        messages = results.get('messages', [])

        if not messages:
            print('No messages found.', flush=True)
            return

        print('Messages:', flush=True)
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id'], format='raw').execute()

            msg_str = base64.urlsafe_b64decode(msg['raw'].encode('ASCII'))
            mime_msg = BytesParser(policy=policy.default).parsebytes(msg_str)

            print(f"Subject: {mime_msg['Subject']}", flush=True)
            subject = mime_msg['Subject']
            print(f"From: {mime_msg['From']}", flush=True)
            from_mail = mime_msg['From']
            print(f"To: {mime_msg['To']}", flush=True)
            to = mime_msg['To']
            print(f"Date: {mime_msg['Date']}", flush=True)
            date = mime_msg['Date']
            if mime_msg.is_multipart():
                for part in mime_msg.iter_parts():
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload(decode=True).decode('utf-8')
                        print(f"Body: {part.get_payload(decode=True).decode('utf-8')}", flush=True)
            else:
                if mime_msg.get_content_type() == 'text/plain':
                    body = mime_msg.get_payload(decode=True).decode('utf-8')
                    print(f"Body: {mime_msg.get_payload(decode=True).decode('utf-8')}", flush=True)
            print("="*100, flush=True)
            conn = mysql.connector.connect(host='localhost', user='root', password='root', database='sample_python')
            cursor = conn.cursor()
            create_table_query = '''CREATE TABLE IF NOT EXISTS emails (id INT AUTO_INCREMENT PRIMARY KEY, subject VARCHAR(255) NOT NULL, body TEXT NOT NULL, from_email VARCHAR(400) NOT NULL,to_mail VARCHAR(200) NOT NULL,date DATE NOT NULL)'''
            cursor.execute(create_table_query)
            insert_query = "INSERT INTO emails (subject, body, from_email,to_mail, date) VALUES (%s, %s, %s, %s, %s)"
            insert_values = (subject, body,from_mail, to, date.datetime.date())
            cursor.execute(insert_query, insert_values)
            cursor.close()
            conn.commit()
            conn.close()
    except HttpError as error:
        print(f'An error occurred: {error}', flush=True)

if __name__ == '__main__':
    main()
