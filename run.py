import logging
import math
import pickle
import os.path
import unicodedata
from time import sleep

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SRC_SPREADSHEET_ID = '183zvc1vAPx5OhnJosIJCaYVZ3VeLKm-0CnEp3zCZP18'
DEST_SPREADSHEET_ID = '1Rs0SCa8ZhbiduR_Kbx32xvc8Z38M_wnrOTQNiqXPsso'
SAMPLE_RANGE_NAME = 'A2:B'

PRICE_MULTIPLIER = 1.02

TIME_TO_SLEEP = 10
TIME_TO_SLEEP_AFTER_ERROR = 30

HEADER = ['Наименование', 'Цена']
LOG_FILE = 'log'

# logger = logging.getLogger(__name__)


def get_service():
    """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    return service


def get_sheets(spreadsheet_id):
    service = get_service()

    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get('sheets')

    return sheets


def read_base_file():
    logger.info('reading src file')

    service = get_service()
    range = 'A2:B'

    result = service.spreadsheets().values().get(spreadsheetId=SRC_SPREADSHEET_ID,
                                                 range=range).execute()

    return result.get('values', [])


def get_new_price(old_price):
    def _roundup(a, digits=0):
        n = 10 ** -digits
        return round(math.ceil(a / n) * n, digits)

    if isinstance(old_price, str):
        old_price = unicodedata.normalize('NFKD', old_price)
        old_price = old_price.replace(',', '.').replace(' ', '')
    return _roundup(float(old_price) * PRICE_MULTIPLIER)


def parse_list(data):
    logger.info('updating prices')
    for d in data:
        d[1] = get_new_price(d[1])
    return data


def write_to_sheet(range, values):
    service = get_service()

    body = {
        'valueInputOption': 'RAW',
        'data': [
            {'range': range,
             'majorDimension': 'ROWS',
             'values': values}
        ]
    }

    result = service.spreadsheets().values().batchUpdate(spreadsheetId=DEST_SPREADSHEET_ID, body=body).execute()
    return result


def copy_sheet():
    src_sheet_id = 0

    body = {
        'destination_spreadsheet_id': DEST_SPREADSHEET_ID
    }

    service = get_service()
    result = service.spreadsheets().sheets().copyTo(spreadsheetId=SRC_SPREADSHEET_ID, sheetId=src_sheet_id,
                                                    body=body).execute()
    return result


def create_sheet(name):
    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": name,
                    }
                }
            }
        ]
    }

    service = get_service()

    result = service.spreadsheets().batchUpdate(spreadsheetId=DEST_SPREADSHEET_ID, body=body).execute()
    return result


def clear_sheet(sheet_id):
    body = {
        "requests": [
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id
                    },
                    "fields": "userEnteredValue"
                }
            }
        ]
    }
    service = get_service()

    result = service.spreadsheets().batchUpdate(spreadsheetId=DEST_SPREADSHEET_ID, body=body).execute()
    return result


def delete_sheet(sheet_id):
    body = {
        "requests": [
            {
                "deleteSheet": {
                    "sheetId": sheet_id
                }
            }
        ]
    }
    service = get_service()

    result = service.spreadsheets().batchUpdate(spreadsheetId=DEST_SPREADSHEET_ID, body=body).execute()
    return result


def divide_by_brands(new_data):
    data_divided_by_brands = {}

    sheets = get_sheets(DEST_SPREADSHEET_ID)
    all_s = [name.get('properties').get('title').lower() for name in sheets]

    logger.info('parsing brands')

    for data in new_data:
        brand = data[0].split()[0].lower()

        if brand not in data_divided_by_brands.keys():
            data_divided_by_brands[brand] = []

        if brand not in all_s:
            sheet_title = data[0].split()[0]
            logger.info('creating sheet {}'.format(sheet_title))
            create_sheet(sheet_title)
            sheets = get_sheets(DEST_SPREADSHEET_ID)
            all_s = [name.get('properties').get('title').lower() for name in sheets]

        data_divided_by_brands[brand].append(data)

    return data_divided_by_brands


def run_update_cycle():
    # copy src sheet to dest file
    # delete old list, move new to the 1st place

    # read data
    data = read_base_file()

    if not data:
        print('empty src file')
        logger.info('src file was empty')
        return False

    # change prices
    new_data = parse_list(data)

    # in foreach check if brand sheet exists, create if not
    # AND
    # create dict with brand keys and values
    data_divided_by_brands = divide_by_brands(new_data)

    sheets = get_sheets(DEST_SPREADSHEET_ID)

    # clear data in corresponding brand sheet and write new data
    range_suffix = 'A1:B'
    for s in sheets:
        page_title = s.get('properties').get('title')
        logger.info('clearing sheet {}'.format(page_title))
        clear_sheet(s.get('properties').get('sheetId'))
        range = '{}!{}'.format(page_title, range_suffix)
        logger.info('updating data')
        to_write = data_divided_by_brands.get(page_title.lower())
        if to_write:
            to_write.insert(0, HEADER)
            write_to_sheet(range, to_write)
    return True


def run():
    try:
        print('updating')
        res = run_update_cycle()

        if not res:
            raise Exception('src file was empty')

        logger.info('Waiting {} seconds before next run'.format(TIME_TO_SLEEP))
        print('waiting {} seconds'.format(TIME_TO_SLEEP))
        sleep(TIME_TO_SLEEP)
    except Exception as e:
        logger.error('oops! something went wrong\n{}'.format(e))
        logger.info('Waiting {} seconds before next run'.format(TIME_TO_SLEEP_AFTER_ERROR))
        print('error happened, waiting {} seconds'.format(TIME_TO_SLEEP_AFTER_ERROR))
        sleep(TIME_TO_SLEEP_AFTER_ERROR)


if __name__ == '__main__':

    try:
        os.remove(LOG_FILE)
    except OSError:
        pass

    print('starting')
    logging.basicConfig(filename=LOG_FILE,
                        format='%(asctime)s  - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    logger = logging.getLogger('gsheet-client')

    while True:
        run()
