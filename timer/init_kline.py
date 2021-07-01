from api.exchange import ExchangeAPI
from db.db_context import session_socpe
from db.model import SymbolModel


def init_kline_demo():
    with session_socpe() as sc:
        symbols = sc.query(SymbolModel).filter_by(exchange='okex', market_type='swap').all()
        for symbol in symbols:
            if symbol.symbol[:3] in ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']:
                ok = ExchangeAPI(1, symbol.symbol)
                ok.synchronize_kline_syn(timeframe='1m', sc=sc)


if __name__ == '__main__':
    init_kline_demo()
