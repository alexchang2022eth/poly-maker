import json
from poly_utils.google_utils import get_spreadsheet
import pandas as pd 
import os
import time
from web3 import Web3
from web3.exceptions import TransactionNotFound, TimeExhausted

def pretty_print(txt, dic):
    print("\n", txt, json.dumps(dic, indent=4))

def get_sheet_df(read_only=None):
    """
    Get sheet data with optional read-only mode
    
    Args:
        read_only (bool): If None, auto-detects based on credentials availability
    """
    all = 'All Markets'
    sel = 'Selected Markets'

    # Auto-detect read-only mode if not specified
    if read_only is None:
        creds_file = 'credentials.json' if os.path.exists('credentials.json') else '../credentials.json'
        read_only = not os.path.exists(creds_file)
        if read_only:
            print("No credentials found, using read-only mode")

    try:
        spreadsheet = get_spreadsheet(read_only=read_only)
    except FileNotFoundError:
        print("No credentials found, falling back to read-only mode")
        spreadsheet = get_spreadsheet(read_only=True)

    wk = spreadsheet.worksheet(sel)
    df = pd.DataFrame(wk.get_all_records())
    df = df[df['question'] != ""].reset_index(drop=True)

    wk2 = spreadsheet.worksheet(all)
    df2 = pd.DataFrame(wk2.get_all_records())
    df2 = df2[df2['question'] != ""].reset_index(drop=True)

    result = df.merge(df2, on='question', how='inner')

    wk_p = spreadsheet.worksheet('Hyperparameters')
    records = wk_p.get_all_records()
    hyperparams, current_type = {}, None

    for r in records:
        # Update current_type only when we have a non-empty type value
        # Handle both string and NaN values from pandas
        type_value = r['type']
        if type_value and str(type_value).strip() and str(type_value) != 'nan':
            current_type = str(type_value).strip()
        
        # Skip rows where we don't have a current_type set
        if current_type:
            # Convert numeric values to appropriate types
            value = r['value']
            try:
                # Try to convert to float if it's numeric
                if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                    value = float(value)
                elif isinstance(value, (int, float)):
                    value = float(value)
            except (ValueError, TypeError):
                pass  # Keep as string if conversion fails
            
            hyperparams.setdefault(current_type, {})[r['param']] = value

    return result, hyperparams


# ===== Transaction Utilities =====

def get_raw_tx_bytes(signed_tx):
    raw = getattr(signed_tx, "rawTransaction", None)
    if raw is None:
        raw = getattr(signed_tx, "raw_transaction", None)
    return raw


def get_pending_nonce(web3, address):
    try:
        return web3.eth.get_transaction_count(address, 'pending')
    except Exception:
        return web3.eth.get_transaction_count(address)


def build_eip1559_fees(web3, priority_fee_gwei=30, multiplier=2.0):
    base_fee = None
    try:
        block = web3.eth.get_block('pending')
        base_fee = block.get('baseFeePerGas')
    except Exception:
        pass

    priority = None
    try:
        priority = getattr(web3.eth, "max_priority_fee")
    except Exception:
        priority = None

    if priority is None:
        priority = Web3.to_wei(priority_fee_gwei, 'gwei')

    if base_fee is not None:
        max_fee = int(multiplier * int(base_fee)) + int(priority)
    else:
        try:
            gas_price = web3.eth.gas_price
            max_fee = int(gas_price) + int(priority)
        except Exception:
            max_fee = int(priority)

    return {
        "maxPriorityFeePerGas": int(priority),
        "maxFeePerGas": int(max_fee),
    }


def build_tx_params(web3, from_addr, chain_id, nonce=None, eip1559=True, priority_fee_gwei=30):
    if nonce is None:
        nonce = get_pending_nonce(web3, from_addr)

    params = {
        "chainId": chain_id,
        "from": from_addr,
        "nonce": nonce,
    }

    if eip1559:
        fees = build_eip1559_fees(web3, priority_fee_gwei=priority_fee_gwei)
        params.update(fees)
    else:
        try:
            params["gasPrice"] = web3.eth.gas_price
        except Exception:
            pass

    return params


def estimate_and_attach_gas(web3, tx, buffer_ratio=1.2):
    try:
        gas = web3.eth.estimate_gas(tx)
        tx["gas"] = int(int(gas) * buffer_ratio)
    except Exception:
        pass
    return tx


def send_signed_transaction_with_receipt(web3, signed_tx, timeout=600, poll_interval=2, max_retries=3):
    raw = get_raw_tx_bytes(signed_tx)
    if not raw:
        raise ValueError("Signed transaction missing raw bytes")

    attempt = 0
    last_error = None
    tx_hash = None

    while attempt < max_retries:
        try:
            tx_hash = web3.eth.send_raw_transaction(raw)
            receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
            return receipt
        except TimeExhausted as e:
            last_error = e
        except ValueError as e:
            msg = str(e)
            if "already known" in msg or "known" in msg:
                if tx_hash:
                    try:
                        receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
                        return receipt
                    except Exception as e2:
                        last_error = e2
            elif ("nonce too low" in msg or
                  "replacement transaction underpriced" in msg or
                  "transaction underpriced" in msg):
                last_error = e
            elif "insufficient funds" in msg:
                raise
            else:
                last_error = e
        except Exception as e:
            last_error = e

        attempt += 1
        time.sleep(poll_interval * attempt)

    if tx_hash:
        try:
            receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=poll_interval * max_retries)
            return receipt
        except Exception:
            pass

    raise RuntimeError(f"Failed to send transaction after {max_retries} attempts: {last_error}")
