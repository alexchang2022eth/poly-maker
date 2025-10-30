from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY

from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from poly_data.utils import (
    build_tx_params,
    estimate_and_attach_gas,
    send_signed_transaction_with_receipt,
    get_pending_nonce,
)

import json

from dotenv import load_dotenv
load_dotenv()

import time

import os

MAX_INT = 2**256 - 1

def get_clob_client():
    host = "https://clob.polymarket.com"
    key = os.getenv("PK")
    chain_id = POLYGON
    
    if key is None:
        print("Environment variable 'PK' cannot be found")
        return None


    try:
        client = ClobClient(host, key=key, chain_id=chain_id)
        api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(api_creds)
        return client
    except Exception as ex: 
        print("Error creating clob client")
        print("________________")
        print(ex)
        return None


def approveContracts():
    web3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    wallet = Account.from_key(os.getenv("PK"))
    
    
    with open('erc20ABI.json', 'r') as file:
        erc20_abi = json.load(file)

    ctf_address = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    # Include isApprovedForAll to avoid redundant approvals
    erc1155_abi = """[
        {"inputs": [
            {"internalType": "address", "name": "operator", "type": "address" },
            {"internalType": "bool", "name": "approved", "type": "bool" }
        ], "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
        {"inputs": [
            {"internalType": "address", "name": "account", "type": "address" },
            {"internalType": "address", "name": "operator", "type": "address" }
        ], "name": "isApprovedForAll", "outputs": [ {"internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function"}
    ]"""

    usdc_contract = web3.eth.contract(address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", abi=erc20_abi)   # usdc.e
    ctf_contract = web3.eth.contract(address=ctf_address, abi=erc1155_abi)
    

    for address in ['0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E', '0xC5d563A36AE78145C45a50134d48A1215220f80a', '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296']:
        # USDC allowance check: only approve if not already MAX_INT
        try:
            current_allowance = usdc_contract.functions.allowance(wallet.address, address).call()
        except Exception as ex:
            print(f"Error reading USDC allowance for {address}: {ex}")
            current_allowance = 0

        if current_allowance < MAX_INT:
            usdc_nonce = get_pending_nonce(web3, wallet.address)
            tx_params = build_tx_params(web3, wallet.address, chain_id=137, nonce=usdc_nonce, eip1559=True, priority_fee_gwei=30)
            raw_usdc_txn = usdc_contract.functions.approve(address, MAX_INT).build_transaction(tx_params)
            raw_usdc_txn = estimate_and_attach_gas(web3, raw_usdc_txn)
            signed_usdc_txn = Account.sign_transaction(raw_usdc_txn, os.getenv("PK"))
            usdc_tx_receipt = send_signed_transaction_with_receipt(web3, signed_usdc_txn, timeout=600)
            print(f'USDC Transaction for {address} returned {usdc_tx_receipt}')
            time.sleep(1)
        else:
            print(f'USDC allowance already MAX_INT for {address}, skipping approve')

        # ERC1155 approval check: only set if not already approved
        try:
            already_approved = ctf_contract.functions.isApprovedForAll(wallet.address, address).call()
        except Exception as ex:
            print(f"Error reading CTF isApprovedForAll for {address}: {ex}")
            already_approved = False

        if not already_approved:
            ctf_nonce = get_pending_nonce(web3, wallet.address)
            tx_params = build_tx_params(web3, wallet.address, chain_id=137, nonce=ctf_nonce, eip1559=True, priority_fee_gwei=30)
            raw_ctf_approval_txn = ctf_contract.functions.setApprovalForAll(address, True).build_transaction(tx_params)
            raw_ctf_approval_txn = estimate_and_attach_gas(web3, raw_ctf_approval_txn)
            signed_ctf_approval_tx = Account.sign_transaction(raw_ctf_approval_txn, os.getenv("PK"))
            ctf_approval_tx_receipt = send_signed_transaction_with_receipt(web3, signed_ctf_approval_tx, timeout=600)
            print(f'CTF Transaction for {address} returned {ctf_approval_tx_receipt}')
            time.sleep(1)
        else:
            print(f'CTF isApprovedForAll already true for {address}, skipping setApprovalForAll')


    
    
def market_action( marketId, action, price, size ):
    order_args = OrderArgs(
        price=price,
        size=size,
        side=action,
        token_id=marketId,
    )
    signed_order = get_clob_client().create_order(order_args)
    
    try:
        resp = get_clob_client().post_order(signed_order)
        print(resp)
    except Exception as ex:
        print(ex)
        pass
    
    
def get_position(marketId):
    client = get_clob_client()
    position_res = client.get_balance_allowance(
        BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=marketId
        )
    )
    orderBook = client.get_order_book(marketId)
    price = float(orderBook.bids[-1].price)
    shares = int(position_res['balance']) / 1e6
    return shares * price