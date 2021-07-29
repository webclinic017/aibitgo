import asyncio
import click

from api.exchange import ExchangeidSymbolidAPI
from db.db_context import session_socpe
from db.model import ExchangeAPIModel
from periodic_task.strategy_628 import Strategy_628


def init_account():
    infos = [
        {
            "id": 101,
            "account": "陈1",
            "api_key": "pOY9fPLMT7w994fot15hUBWeol8L55ZCVg4kXuTCnYYzHvaLRZJMaLx3elAeaMaR",
            "secret_key": "2PSnY5KklkjPDzRPpILi3n9zT1XG4SePbrMoNmJZdDjKTCUFckVOcmhfySjypkPY",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 102,
            "account": "陈2",
            "api_key": "kg60EKWKVCiFurEqBICu7S0dPJ46Or3bikM2OCJZ8pj7thpHNuqGH8yb7mwJSW3J",
            "secret_key": "FAlJPp26knRbBdsGJ8tShtHYy3IzRO3vuBQOkhbqadStl4s0FZ8oxJdfwle7ecON",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 103,
            "account": "王宙斯",
            "api_key": "C8SaWPS8LhmzbRu0UJoyyUAuRxFCsQqg80BGJKJbFN7PkWctRWKNKXE0HbfZo3Ej",
            "secret_key": "1Fb5vnTxwFT2a2ZAi6W6V8xIpRGrhoQqj8Os4wjj6jQotvnmIK88j5lNta17rOpy",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 104,
            "account": "彭总",
            "api_key": "h3gdAN5U1YLXf1vTAHurWdoTELl1i54Ky4ITc99XTEYsllJnu1D9c6Csqboc9PeY",
            "secret_key": "jwxswjfTW1Ov5vTLUDyAeKS7PkliVSBWF3x7K2QJ4Y7RcW5PhaSQ6zNVUYA8rmaX",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 105,
            "account": "张总",
            "api_key": "5g2JoZnV5v5DWfzl9kJry1jFlaCHZJJU2yg2RWYGQklrMiD775fQFDWWktn9mFCI",
            "secret_key": "R3ejGHRcg5Y20N9qChJqZcyLhcMpG4OqQvwJm41LHZG22T1p5gtqZpXdFLyt4ODv",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 106,
            "account": "易总",
            "api_key": "cDEAYTkJvZ1IE8vsxEhYH8KElURD9Mq9j3CDP9eSFLxDIoPYP8818xIou3X1cC0Z",
            "secret_key": "ctNsa3Iw0j7V80n7OuIEioQg4Cp8hxfz9Zx5IxOgqGTgMXL2THtVHjMLBPBV4Jxg",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 107,
            "account": "谭总",
            "api_key": "vNbGmnUikOZ6f5oLcUZkZ5fSXXYynsUWWfntpRQl0DxA71iY6IrtKB7yq9L6pbRh",
            "secret_key": "oNjWUpmgiodHkyz1O7RCLdIPHruzspH1sQmq5kiKxpjIR7cimnxZpo4SxMWonWOY",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "id": 108,
            "account": "林总",
            "api_key": "Yo0IapxftFE1nAYLjsiWNKmH2TSvU3WrJMuzbSioJapoM7S7kaMThIcrvR2g3MeG",
            "secret_key": "EnVz3PWTDFDDlq33nJoC2zz9ETkxJDxWNr0l38UMDbG69EKQ9xsSAJrS2X5mqUow",
            "passphrase": "123456",
            'exchange': 'binance'
        },
    ]

    with session_socpe() as sc:
        for info in infos:
            api = ExchangeAPIModel(**info)
            sc.merge(api)
            print(f"success add:{api.id}-{api.account}")


@click.command()
@click.argument('task')
def run(task):
    if task == "account":
        init_account()
    elif task == "check":
        coin_api_id = 108
        coin_symbol_id = 765
        coin_api = ExchangeidSymbolidAPI(api_id=coin_api_id, symbol_id=coin_symbol_id)
        long_amount, short_amount, _ = asyncio.run(coin_api.get_symbol_position_short_long())
        print(long_amount, short_amount)
    elif task == "btc":
        symbol_id = 785
        account_id_amount = {
            # my account
            32: 0.001,

            # 旷总账户
            # 31: 0.1,

            101: 0.05,
            102: 0.05,

            103: 0.1,
            104: 0.1,
            105: 0.1,
            106: 0.1,
            107: 0.1
        }
        strategy = Strategy_628(symbol_id=symbol_id, account_id_amount=account_id_amount)
        asyncio.run(strategy.run())

    elif task == "bnb":
        symbol_id = 800
        account_id_amount = {
            101: 2,
            102: 2,
            32: 0.01
        }
        strategy = Strategy_628(symbol_id=symbol_id, account_id_amount=account_id_amount)
        asyncio.run(strategy.run())

    elif task == "eth":
        symbol_id = 786
        account_id_amount = {
            101: 0.5,
            102: 0.5,
            32: 0.0003
        }
        strategy = Strategy_628(symbol_id=symbol_id, account_id_amount=account_id_amount)
        asyncio.run(strategy.run())


if __name__ == '__main__':
    run()
