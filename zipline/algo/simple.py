from zipline.api import order, record, symbol
import time

def initialize(context):
    pass


def handle_data(context, data):
    while True:
        order(symbol('AAPL'), 10)
        record(AAPL=data.current(symbol('AAPL'), 'price'))
        time.sleep(5)