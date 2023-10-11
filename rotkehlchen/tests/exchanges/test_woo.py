from unittest.mock import call, patch

import pytest

from rotkehlchen.accounting.structures.balance import Balance
from rotkehlchen.constants.assets import A_BTC, A_ETH, A_WOO
from rotkehlchen.exchanges.data_structures import Trade, TradeType
from rotkehlchen.exchanges.woo import API_MAX_LIMIT, Woo
from rotkehlchen.fval import FVal
from rotkehlchen.types import Location, Timestamp


def test_name():
    exchange = Woo('woo', 'a', b'a', object(), object())
    assert exchange.location == Location.WOO
    assert exchange.name == 'woo'


@pytest.mark.parametrize(('start_ts', 'end_ts'), [(0, 1), (1634600000, 1634610000)])
def test_query_online_trade_history_basic(mock_woo, start_ts, end_ts):
    mock_trades = [
        {
            'id': 1,
            'symbol': 'SPOT_BTC_USDT',
            'order_id': 101,
            'executed_price': 50000.0,
            'executed_quantity': 1.0,
            'side': 'BUY',
            'fee': 0.1,
            'fee_asset': 'USDT',
            'executed_timestamp': '1634600000.0',
        },
        {
            'id': 2,
            'symbol': 'SPOT_ETH_USDT',
            'order_id': 102,
            'executed_price': 3000.0,
            'executed_quantity': 2.0,
            'side': 'SELL',
            'fee': 0.2,
            'fee_asset': 'USDT',
            'executed_timestamp': '1634610000.0',
        },
    ]
    with patch.object(Woo, 'query_online_trade_history', return_value=mock_trades):
        result = mock_woo.query_online_trade_history(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
        )
    assert result == mock_trades


@pytest.mark.parametrize(('start_ts', 'end_ts'), [(0, 1), (1634600000, 1634620000)])
def test_query_online_history_paginated(mock_woo, start_ts, end_ts):
    with patch.object(Woo, '_api_query_paginated') as mock_api_query_paginated:
        mock_woo.query_online_trade_history(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
        )
        expected_call_page_one = call(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
            options={'limit': API_MAX_LIMIT},
            case='trades',
        )
        assert mock_api_query_paginated.call_args == expected_call_page_one
        mock_woo.query_online_trade_history(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
        )
        expected_call_page_two = call(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
            options={'limit': API_MAX_LIMIT},
            case='trades',
        )
        assert mock_api_query_paginated.call_args == expected_call_page_two


def test_query_balances(mock_woo):
    balances_response = {
        'success': 'true',
        'data': {
            'holding': [
                {
                    'token': 'WOO',
                    'holding': 1,
                    'frozen': 0,
                    'staked': 0,
                    'unbonding': 0,
                    'vault': 0,
                    'interest': 0,
                    'pendingShortQty': 0,
                    'pendingLongQty': 0,
                    'availableBalance': 0,
                    'averageOpenPrice': 0.23432,
                    'markPrice': 0.25177,
                    'updatedTime': 312321.121,
                },
            ],
        },
    }

    asset_balances = mock_woo._deserialize_accounts_balances(balances_response)
    assert asset_balances == {
        A_WOO: Balance(
            amount=FVal('1'),
            usd_value=FVal('1.5'),
        ),
    }


def test_deserialize_trade_buy(mock_woo):
    mock_trades = {
        'id': 1,
        'symbol': 'SPOT_BTC_ETH',
        'order_id': 101,
        'executed_price': 50000.0,
        'executed_quantity': 1.0,
        'side': 'BUY',
        'fee': 0.1,
        'fee_asset': 'ETH',
        'executed_timestamp': '1634600000.0',
    }
    result = mock_woo._deserialize_trade(mock_trades)
    assert result == Trade(
        timestamp=1634600000,
        location=Location.WOO,
        base_asset=A_BTC,
        quote_asset=A_ETH,
        trade_type=TradeType.BUY,
        amount=FVal('1'),
        rate=FVal('50000'),
        fee=FVal('0.1'),
        fee_currency=A_ETH,
        link='1',
    )
