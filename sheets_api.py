import gspread
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(credentials)


