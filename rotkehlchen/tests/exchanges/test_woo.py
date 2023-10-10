import json
import warnings as test_warnings
from http import HTTPStatus
from json.decoder import JSONDecodeError
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from rotkehlchen.accounting.structures.balance import Balance
from rotkehlchen.assets.converters import asset_from_woo
from rotkehlchen.constants.assets import A_BTC, A_ETH, A_SOL, A_USDT, A_WOO
from rotkehlchen.errors.asset import UnknownAsset
from rotkehlchen.errors.misc import RemoteError
from rotkehlchen.exchanges.data_structures import (
    AssetMovement,
    AssetMovementCategory,
    Trade,
    TradeType,
)
from rotkehlchen.exchanges.woo import API_MAX_LIMIT, Woo
from rotkehlchen.fval import FVal
from rotkehlchen.tests.utils.mock import MockResponse
from rotkehlchen.types import Location, Timestamp


def test_name():
    exchange = Woo('woo', 'a', b'a', object(), object())
    assert exchange.location == Location.WOO
    assert exchange.name == 'woo'


@pytest.mark.parametrize(('start_ts', 'end_ts'), [(0, 1), (1634600000, 1634620000)])
def test_query_online_trade_history_basic(mock_woo, start_ts, end_ts):
    expected_call = call(
        start_ts=Timestamp(start_ts),
        end_ts=Timestamp(end_ts),
        options={'limit': API_MAX_LIMIT},
        case='trades',
    )

    with patch.object(mock_woo, '_api_query_paginated') as mock_query:
        mock_woo.query_online_trade_history(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
        )

    assert mock_query.call_args_list == [expected_call]


@pytest.mark.parametrize(('start_ts', 'end_ts'), [(0, 1), (1634600000, 1634620000)])
def test_query_online_history_paginated(mock_woo, start_ts, end_ts):
    api_limit = 2
    first_page_results = {
        'success': 'true',
        'data': [
            {'id': 1, 'symbol': 'SPOT_BTC_ETH', 'order_id': 101, 'executed_price': 50000.0, 'executed_quantity': 1.0, 'side': 'BUY', 'fee': 0.1, 'fee_asset': 'ETH', 'executed_timestamp': '1634600000.0'},  # noqa: E501
            {'id': 2, 'symbol': 'SPOT_ETH_USDT', 'order_id': 102, 'executed_price': 3000.0, 'executed_quantity': 2.0, 'side': 'SELL', 'fee': 0.2, 'fee_asset': 'USDT', 'executed_timestamp': '1634610000.0'},  # noqa: E501
        ],
    }
    second_page_results = {
        'success': 'true',
        'data': [
            {'id': 3, 'symbol': 'SPOT_BTC_ETH', 'order_id': 103, 'executed_price': 40000.0, 'executed_quantity': 3.0, 'side': 'BUY', 'fee': 0.3, 'fee_asset': 'ETH', 'executed_timestamp': '1634620000.0'},  # noqa: E501
            {'id': 4, 'symbol': 'SPOT_ETH_USDT', 'order_id': 104, 'executed_price': 2000.0, 'executed_quantity': 4.0, 'side': 'SELL', 'fee': 0.4, 'fee_asset': 'USDT', 'executed_timestamp': '1634630000.0'},  # noqa: E501
        ],
    }
    mock_results = [first_page_results, second_page_results]
    expected_calls = [
        call(
            endpoint='v1/client/hist_trades',
            options={'end_t': end_ts * 1000, 'fromId': 2, 'limit': api_limit, 'start_t': start_ts * 1000},  # noqa: E501
        ),
    ]

    def mock_api_query_response(endpoint, options):
        return MockResponse(HTTPStatus.OK, json.dumps(mock_results.pop(0)))

    limit_patch = patch(
        'rotkehlchen.exchanges.woo.API_MAX_LIMIT',
        new_callable=MagicMock(return_value=api_limit),
    )
    api_query_patch = patch.object(
        mock_woo,
        '_api_query',
        side_effect=mock_api_query_response,
    )
    with limit_patch, api_query_patch:
        mock_woo.query_online_trade_history(
            start_ts=Timestamp(start_ts),
            end_ts=Timestamp(end_ts),
        )
        assert mock_woo._api_query.call_args_list == expected_calls


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
                {
                    'token': 'ETH',
                    'holding': 2,
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
                    'updatedTime': 31321.121,
                },
                {
                    'token': 'BTC',
                    'holding': 0,
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
                    'updatedTime': 31321.121,
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
        A_ETH: Balance(
            amount=FVal('2'),
            usd_value=FVal('3'),
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


def test_deserialize_trade_sell(mock_woo):
    mock_trades = {
        'id': 2,
        'symbol': 'SPOT_ETH_USDT',
        'order_id': 102,
        'executed_price': 3000.0,
        'executed_quantity': 2.0,
        'side': 'SELL',
        'fee': 0.2,
        'fee_asset': 'USDT',
        'executed_timestamp': '1634610000.0',
    }
    result = mock_woo._deserialize_trade(mock_trades)
    assert result == Trade(
        timestamp=1634610000,
        location=Location.WOO,
        base_asset=A_ETH,
        quote_asset=A_USDT,
        trade_type=TradeType.SELL,
        amount=FVal('2'),
        rate=FVal('3000'),
        fee=FVal('0.2'),
        fee_currency=A_USDT,
        link='2',
    )


def test_deserialize_asset_movement_deposit(mock_woo):
    mock_deposit = {
        'created_time': '1579399877.041',
        'updated_time': '1579399877.041',
        'id': '202029292829292',
        'external_id': '202029292829292',
        'application_id': 'null',
        'token': 'ETH',
        'target_address': '0x31d64B3230f8baDD91dE1710A65DF536aF8f7cDa',
        'source_address': '0x70fd25717f769c7f9a46b319f0f9103c0d887af0',
        'confirming_threshold': 12,
        'confirmed_number': 12,
        'extra': '',
        'type': 'BALANCE',
        'token_side': 'DEPOSIT',
        'amount': 1000,
        'tx_id': '0x8a74c517bc104c8ebad0c3c3f64b1f302ed5f8bca598ae4459c63419038106b6',
        'fee_token': 'ETH',
        'fee_amount': 0,
        'status': 'CONFIRMING',
    }
    result = mock_woo._deserialize_asset_movement(mock_deposit)
    assert result == AssetMovement(
        location=Location.WOO,
        category=AssetMovementCategory.DEPOSIT,
        timestamp=Timestamp(1579399877),
        address='0x31d64B3230f8baDD91dE1710A65DF536aF8f7cDa',
        transaction_id='0x8a74c517bc104c8ebad0c3c3f64b1f302ed5f8bca598ae4459c63419038106b6',
        asset=A_ETH,
        amount=FVal(1000),
        fee_asset=A_ETH,
        fee=0,
        link='202029292829292',
    )


def test_deserialize_asset_movement_withdrawal(mock_woo):
    mock_withdrawal = {
        'id': '23061317355600291',
        'token': 'SOL',
        'extra': '',
        'amount': 12.71,
        'status': 'COMPLETED',
        'created_time': '1686677756.753',
        'updated_time': '1686677950.305',
        'external_id': '230613173800289',
        'application_id': 'e312028a-6afd-4eac-b2b8-206f66e7e086',
        'target_address': 'D2egh1gRCHNuDLWhdcxPVEVvmiMB6KGMKAgQm6vR1diL',
        'source_address': '',
        'type': 'BALANCE',
        'token_side': 'WITHDRAW',
        'tx_id': '4DPkJEmE3RnmDLZnM65NtFZ6L5J2R8Lwy2C1sVq7iwcPfTkjJhN5Uuh2GFsT6m13UHkeQiKjznLKK5SqK1kfTfZa',  # noqa: E501
        'fee_token': 'SOL',
        'fee_amount': 0.0,
        'confirming_threshold': 1,
        'confirmed_number': 1,
    }
    result = mock_woo._deserialize_asset_movement(mock_withdrawal)
    assert result == AssetMovement(
        location=Location.WOO,
        category=AssetMovementCategory.WITHDRAWAL,
        timestamp=Timestamp(1686677756),
        address='D2egh1gRCHNuDLWhdcxPVEVvmiMB6KGMKAgQm6vR1diL',
        transaction_id='4DPkJEmE3RnmDLZnM65NtFZ6L5J2R8Lwy2C1sVq7iwcPfTkjJhN5Uuh2GFsT6m13UHkeQiKjznLKK5SqK1kfTfZa',
        asset=A_SOL,
        amount=FVal(12.71),
        fee_asset=A_SOL,
        fee=0,
        link='23061317355600291',
    )


def test_woo_asset_are_known(mock_woo):
    request_url = f'{mock_woo.base_uri}/v1/public/token'
    try:
        response = requests.get(request_url)
    except requests.exceptions.RequestException as e:
        raise RemoteError(
            f'Woo get request at {request_url} connection error: {e!s}.',
        ) from e
    if response.status_code != 200:
        raise RemoteError(
            f'Woo query responded with error status code: {response.status_code} '
            f'and text: {response.text}',
        )
    try:
        response_list = response.json()
    except JSONDecodeError as e:
        raise RemoteError(f'Woo returned invalid JSON response: {response.text}') from e
    rows = {x['balance_token'] for x in response_list['rows']}
    for row in rows:
        try:
            asset_from_woo(row)
        except UnknownAsset as e:
            test_warnings.warn(UserWarning(
                f'Found unknown asset {e.identifier} in {mock_woo.name}. '
                f'Support for it has to be added',
            ))
