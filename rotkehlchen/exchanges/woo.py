import hashlib
import hmac
import logging
import urllib
from collections import defaultdict
from http import HTTPStatus
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, Any, Callable, Literal, NamedTuple, Optional, Union, overload

import requests
from requests.adapters import Response

from rotkehlchen.accounting.structures.balance import Balance
from rotkehlchen.assets.asset import AssetWithOracles
from rotkehlchen.assets.converters import asset_from_woo
from rotkehlchen.constants import ZERO
from rotkehlchen.errors.asset import UnknownAsset, UnsupportedAsset
from rotkehlchen.errors.misc import RemoteError
from rotkehlchen.errors.serialization import DeserializationError
from rotkehlchen.exchanges.data_structures import AssetMovement, Trade, TradeType
from rotkehlchen.exchanges.exchange import ExchangeInterface, ExchangeQueryBalances
from rotkehlchen.history.deserialization import deserialize_price
from rotkehlchen.inquirer import Inquirer
from rotkehlchen.logging import RotkehlchenLogsAdapter
from rotkehlchen.serialization.deserialize import deserialize_asset_amount, deserialize_fee
from rotkehlchen.types import ApiKey, ApiSecret, ExchangeAuthCredentials, Location, Timestamp
from rotkehlchen.user_messages import MessagesAggregator
from rotkehlchen.utils.misc import ts_now_in_ms, ts_sec_to_ms
from rotkehlchen.utils.mixins.cacheable import cache_response_timewise
from rotkehlchen.utils.mixins.lockable import protect_with_lock
from rotkehlchen.utils.serialization import jsonloads_dict

if TYPE_CHECKING:
    from rotkehlchen.db.dbhandler import DBHandler

logger = logging.getLogger(__name__)
log = RotkehlchenLogsAdapter(logger)

API_KEY_ERROR_CODE_ACTION = {
    '-1000': 'An unknown error occurred while processing the request.',
    '-1001': 'The api key or secret is in wrong format.',
    '-1002': 'The api key or secret is invalid.',
}
# Max limit for all API v2 endpoints
API_MAX_LIMIT = 1000
# user_transactions endpoint constants
# Sort mode
USER_TRANSACTION_SORTING_MODE = 'asc'
# Starting `since_id`
USER_TRANSACTION_MIN_SINCE_ID = 1
# Trade type int
USER_TRANSACTION_TRADE_TYPE = {2}
# Asset movement type int: 0 - deposit, 1 - withdrawal
USER_TRANSACTION_ASSET_MOVEMENT_TYPE = {0, 1}
KNOWN_NON_ASSET_KEYS_FOR_MOVEMENTS = {
    'datetime',
    'id',
    'type',
    'fee',
}


class TradePairData(NamedTuple):
    pair: str
    base_asset_symbol: str
    quote_asset_symbol: str
    base_asset: AssetWithOracles
    quote_asset: AssetWithOracles


class Woo(ExchangeInterface):
    """Woo exchange api docs:
    https://docs.woo.org/#general-information

    """
    def __init__(
            self,
            name: str,
            api_key: ApiKey,
            secret: ApiSecret,
            database: 'DBHandler',
            msg_aggregator: MessagesAggregator,
    ):
        super().__init__(
            name=name,
            location=Location.WOO,
            api_key=api_key,
            secret=secret,
            database=database,
        )
        self.base_uri = 'https://api.woo.org'
        self.msg_aggregator = msg_aggregator
        # NB: x-api-signature & x-api-timestamp change per request
        # x-api-key is constant
        self.session.headers.update({
            'x-api-key': self.api_key,
        })

    def first_connection(self) -> None:
        self.first_connection_made = True

    def edit_exchange_credentials(self, credentials: ExchangeAuthCredentials) -> bool:
        changed = super().edit_exchange_credentials(credentials)
        if credentials.api_key is not None:
            self.session.headers.update({'x-api-key': credentials.api_key})
        return changed

    @protect_with_lock()
    @cache_response_timewise()
    def query_balances(self) -> ExchangeQueryBalances:
        """Return the account balances on Woo """
        response = self._api_query('v3/balances')

        if response.status_code != HTTPStatus.OK:
            result, msg = self._process_unsuccessful_response(
                response=response,
                case='balances',
            )
            return result, msg
        try:
            response_dict = jsonloads_dict(response.text)
        except JSONDecodeError as e:
            msg = f'Woo returned invalid JSON response: {response.text}.'
            log.error(msg)
            raise RemoteError(msg) from e
        account_balances = self._deserialize_accounts_balances(response_dict=response_dict)
        return account_balances, ''

    def _deserialize_accounts_balances(
            self,
            response_dict: dict[str, Any],
    ) -> dict[AssetWithOracles, Balance]:
        try:
            balances = response_dict['data']['holding']
        except KeyError as e:
            msg = 'Woo balances JSON response is missing data key'
            log.error(msg, response_dict)
            raise RemoteError(msg) from e

        assets_balance: defaultdict[AssetWithOracles, Balance] = defaultdict(Balance)
        for entry in balances:
            symbol = entry.get('token')
            try:
                amount = deserialize_asset_amount(entry.get('holding') + entry.get('staked'))
                if amount == ZERO:
                    continue
                asset = asset_from_woo(symbol)
            except DeserializationError as e:
                log.error(
                    'Error processing a Woo balance.',
                    entry=entry,
                    error=str(e),
                )
                self.msg_aggregator.add_error(
                    'Failed to deserialize a Woo balance. '
                    'Check logs for details. Ignoring it.',
                )
                continue
            except (UnknownAsset, UnsupportedAsset) as e:
                log.error(str(e))
                asset_tag = 'unknown' if isinstance(e, UnknownAsset) else 'unsupported'
                self.msg_aggregator.add_warning(
                    f'Found {asset_tag} Woo asset {e.identifier}. Ignoring its balance query.',
                )
                continue
            try:
                usd_price = Inquirer().find_usd_price(asset=asset)
            except RemoteError as e:
                log.error(str(e))
                self.msg_aggregator.add_error(
                    f'Error processing Woo balance result due to inability to '
                    f'query USD price: {e!s}. Skipping balance entry.',
                )
                continue

            assets_balance[asset] += Balance(
                amount=amount,
                usd_value=amount * usd_price,
            )

        return dict(assets_balance)

    def _deserialize_trade(
            self,
            trade: dict[str, Any],
    ) -> Trade:
        """Deserialize a Woo trade returned from the API to History Event"""
        symbol = trade['symbol']
        _, base_asset_symbol, quote_asset_symbol = symbol.split('_')
        base_asset = asset_from_woo(base_asset_symbol)
        quote_asset = asset_from_woo(quote_asset_symbol)
        side = trade['side']
        fee_asset = asset_from_woo(trade['fee_asset'])
        trade_type = TradeType.BUY if side == 'BUY' else TradeType.SELL
        timestamp = Timestamp(int(float(trade['executed_timestamp'])))
        fee = deserialize_fee(trade['fee'])
        executed_price = deserialize_price(trade['executed_price'])
        executed_quantity = deserialize_asset_amount(trade['executed_quantity'])
        return Trade(
            timestamp=timestamp,
            location=Location.WOO,
            base_asset=base_asset,
            quote_asset=quote_asset,
            trade_type=trade_type,
            amount=executed_quantity,
            rate=executed_price,
            fee=fee,
            fee_currency=fee_asset,
            link=str(trade['id']),
        )

    def query_online_trade_history(
            self,
            start_ts: Timestamp,
            end_ts: Timestamp,
    ) -> tuple[list[Trade], tuple[Timestamp, Timestamp]]:
        """Return trade history on Woo in a range of time."""

        trades: list[Trade] = self._api_query_paginated(
            start_ts=start_ts,
            end_ts=end_ts,
            options={'limit': API_MAX_LIMIT},
            case='trades',
        )
        return trades, (start_ts, end_ts)

    def validate_api_key(self) -> tuple[bool, str]:
        """Validates that the Woo API key is good for usage in rotki"""
        response = self._api_query('v1/client/trades')
        if response.status_code != HTTPStatus.OK:
            result, msg = self._process_unsuccessful_response(
                response=response,
                case='validate_api_key',
            )
            return result, msg
        return True, ''

    def _api_query(
            self,
            endpoint: str,
            method: Literal['GET', 'POST'] = 'GET',
            options: Optional[dict[str, Any]] = None,
    ) -> Response:
        """Request a  Woo API endpoint (from `endpoint`)."""
        call_options = options if options else {}
        request_url = f'{self.base_uri}/{endpoint}'
        timestamp = str(ts_now_in_ms())
        parameters = urllib.parse.urlencode(call_options)
        normalized_content = f'{timestamp}{method}/{endpoint}{parameters}' if endpoint.startswith('v3') else f'{parameters}|{timestamp}'  # noqa: E501
        signature = hmac.new(
            self.secret,
            msg=normalized_content.encode('utf-8'),
            digestmod=hashlib.sha256,
        ).hexdigest()
        self.session.headers.update({
            'x-api-signature': signature,
            'x-api-timestamp': timestamp,
        })

        log.debug('Woo API request', request_url=request_url, options=options)
        try:
            response = self.session.request(
                method=method,
                url=request_url,
                data=call_options if method == 'POST' else {},
                params=call_options if method == 'GET' else {},
                headers=self.session.headers,
            )
        except requests.exceptions.RequestException as e:
            raise RemoteError(
                f'Woo {method} request at {request_url} connection error: {e!s}.',
            ) from e

        return response

    @overload
    def _api_query_paginated(
            self,
            start_ts: Timestamp,
            end_ts: Timestamp,
            case: Literal['trades'],
            options: dict[str, Any],
    ) -> list[Trade]:
        ...

    @overload
    def _api_query_paginated(
            self,
            start_ts: Timestamp,
            end_ts: Timestamp,
            case: Literal['asset_movements'],
            options: dict[str, Any],
    ) -> list[AssetMovement]:
        ...

    def _api_query_paginated(
            self,
            start_ts: Timestamp,
            end_ts: Timestamp,
            case: Literal['trades', 'asset_movements'],
            options: dict[str, Any],
    ) -> Union[list[Trade], list[AssetMovement], list]:
        """Request a Woo API endpoint paginating via an options attribute."""
        deserialization_method: Callable[[dict[str, Any]], Any]
        results = []
        endpoint = 'v1/client/hist_trades' if case == 'trades' else 'v1/asset/history'
        call_options = {
            'end_t': ts_sec_to_ms(end_ts),
            'fromId': 1,
            'limit': options.get('limit', API_MAX_LIMIT),
            'start_t': ts_sec_to_ms(start_ts),
        }
        while True:
            response = self._api_query(
                endpoint=endpoint,
                options=call_options,
            )
            if response.status_code != HTTPStatus.OK:
                return self._process_unsuccessful_response(
                    response=response,
                    case=case,
                )
            try:
                response_data = jsonloads_dict(response.text)
            except JSONDecodeError:
                msg = f'{self.name} {case} returned an invalid JSON response: {response.text}.'
                log.error(msg, options=call_options)
                self.msg_aggregator.add_error(
                    f'Got remote error while querying {self.name} {case}: {msg}',
                )
                return []
            deserialization_method = self._deserialize_trade
            entries = response_data['data']

            for entry in entries:
                try:
                    result = deserialization_method(entry)
                    results.append(result)
                except DeserializationError as e:
                    log.error(
                        'Error processing a Woo balance.',
                        entry=entry,
                        error=str(e),
                    )
            if len(entries) < call_options['limit']:
                break
            call_options['fromId'] = entries[-1]['id']

        return results

    @overload
    def _process_unsuccessful_response(
            self,
            response: Response,
            case: Literal['validate_api_key'],
    ) -> tuple[Literal[False], str]:
        ...

    @overload
    def _process_unsuccessful_response(
            self,
            response: Response,
            case: Literal['balances'],
    ) -> ExchangeQueryBalances:
        ...

    @overload
    def _process_unsuccessful_response(
            self,
            response: Response,
            case: Literal['trades'],
    ) -> list[Trade]:
        ...

    @overload
    def _process_unsuccessful_response(
            self,
            response: Response,
            case: Literal['asset_movements'],
    ) -> list[AssetMovement]:
        ...

    def _process_unsuccessful_response(
            self,
            response: Response,
            case: Literal['validate_api_key', 'balances', 'trades', 'asset_movements'],
    ) -> Union[
        list,
        tuple[Literal[False], str],
        ExchangeQueryBalances,
    ]:
        """This function processes not successful responses for the following
        cases listed in `case`.
        """
        case_pretty = case.replace('_', ' ')  # human readable case
        try:
            response_dict = jsonloads_dict(response.text)
        except JSONDecodeError as e:
            msg = f'{self.name} returned invalid JSON response: {response.text}.'
            log.error(msg)

            if case in ('validate_api_key', 'balances'):
                return False, msg
            if case in ('trades', 'asset_movements'):
                self.msg_aggregator.add_error(
                    f'Got remote error while querying {self.name} {case}: {msg}',
                )
                return []

            raise AssertionError(f'Unexpected {self.name} response_case: {case_pretty}.') from e

        error_code = response_dict.get('code')
        if error_code in API_KEY_ERROR_CODE_ACTION:
            msg = API_KEY_ERROR_CODE_ACTION[error_code]
        else:
            reason = response_dict.get('reason') or response.text
            msg = (
                f'{self.name} query responded with error status code: {response.status_code} '
                f'and text: {reason}.'
            )
            log.error(msg)

        if case in ('validate_api_key', 'balances'):
            return False, msg
        if case in ('trades', 'asset_movements'):
            self.msg_aggregator.add_error(
                f'Got remote error while querying {self.name} {case}: {msg}',
            )
            return []

        raise AssertionError(f'Unexpected {self.name} response_case: {case_pretty}.')
