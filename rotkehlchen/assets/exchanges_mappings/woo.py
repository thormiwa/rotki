from rotkehlchen.assets.exchanges_mappings.common import COMMON_ASSETS_MAPPINGS
from rotkehlchen.constants.resolver import evm_address_to_identifier, strethaddress_to_identifier
from rotkehlchen.types import ChainID, EvmTokenKind

WORLD_TO_WOO = COMMON_ASSETS_MAPPINGS | {
    # Luna Terra is LUNA-2 in rotki
    'LUNA-2': 'LUNA',
    # Solana is SOL-2 in rotki
    'SOL-2': 'SOL',
    evm_address_to_identifier('0x4691937a7508860F876c9c0a2a617E7d9E945D4B', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'WOO',  # noqa: E501
    evm_address_to_identifier('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'STG',  # noqa: E501
    # ONE is Harmony in Binance
    'ONE-2': 'ONE',
    evm_address_to_identifier('0x43Dfc4159D86F3A37A5A4B3D4580b888ad7d4DDd', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'DODO',  # noqa: E501
    evm_address_to_identifier('0x0996bFb5D057faa237640E2506BE7B4f9C46de0B', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'RNDR',  # noqa: E501
    evm_address_to_identifier('0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'FXS',  # noqa: E501
    strethaddress_to_identifier('0xF57e7e7C23978C3cAEC3C3548E3D615c346e79fF'): 'IMX',
    evm_address_to_identifier('0x320623b8E4fF03373931769A31Fc52A4E78B5d70', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'RSR',  # noqa: E501
    evm_address_to_identifier('0xbC396689893D065F41bc2C6EcbeE5e0085233447', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'PERP',  # noqa: E501
    evm_address_to_identifier('0x163f8C2467924be0ae7B5347228CABF260318753', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'WLD',  # noqa: E501
    evm_address_to_identifier('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'LDO',  # noqa: E501
    evm_address_to_identifier('0xB0c7a3Ba49C7a6EaBa6cD4a96C55a1391070Ac9A', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'MAGIC',  # noqa: E501
    evm_address_to_identifier('0xB62E45c3Df611dcE236A6Ddc7A493d79F9DFadEf', ChainID.ETHEREUM, EvmTokenKind.ERC20): 'WSM',  # noqa: E501
    evm_address_to_identifier('0x6626c47c00F1D87902fc13EECfaC3ed06D5E8D8a', ChainID.FANTOM, EvmTokenKind.ERC20): 'FTMWOO',  # noqa: E501
}
