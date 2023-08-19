import json
import time
import requests
import os
from eth_abi import abi
from eth_abi.exceptions import NonEmptyPaddingBytes


def fetch_transfer_logs(token_address, until_block=None):
    base_url = "https://api.syve.ai/v1/filter-api/logs"
    size = 100_000
    params = {
        "eq:address": token_address,
        "eq:topic_0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "sort": "desc",
        "size": size,
    }
    if until_block is not None:
        params["lte:block_number"] = until_block
    response = requests.get(base_url, params=params)
    records = response.json()
    return records


class ERC20TransferParser(object):
    def __init__(self):
        pass

    def _decode_address(self, address_hex):
        try:
            return abi.decode(types=["address"], data=bytes.fromhex(address_hex[2:]))[0]
        except NonEmptyPaddingBytes:
            return None

    def decode_transfer_log(self, log):
        topics = []
        for i in range(4):
            if f"topic_{i}" in log:
                topics.append(log[f"topic_{i}"])
        if len(topics) == 0:
            return
        if topics[0] != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
            return
        if len(topics) != 3:
            return
        log_decoded = {
            "address": log["address"],
            "block_number": log["block_number"],
            "timestamp": log["timestamp"],
            "log_index": log["log_index"],
            "args": {},
            "block_hash": log.get("block_hash"),
            "transaction_hash": log["transaction_hash"],
            "transaction_index": log["transaction_index"],
        }
        log_decoded["args"].update(
            {
                # note: abi.decode returns list
                "from": self._decode_address(topics[1]),
                "to": self._decode_address(topics[2]),
                "value": abi.decode(types=["uint256"], data=bytes.fromhex(log["data"][2:]))[0],
            }
        )
        if log_decoded["args"]["from"] is None:
            return
        if log_decoded["args"]["to"] is None:
            return
        return log_decoded


def main():
    token_address = "0x6982508145454ce325ddbe47a25d4ec3d2311933"

    start = time.time()
    save_path = f"data/{token_address}-{int(start)}.json"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    last_block = None
    last_tx_index = None
    last_log_index = None
    parser = ERC20TransferParser()
    visited = set()
    total_saved = 0

    for i in range(20):
        if i == 0:
            logs = fetch_transfer_logs(token_address)
        else:
            logs = fetch_transfer_logs(
                token_address=token_address,
                until_block=last_block,
            )
        transfers = []
        for log in logs:
            transfer = parser.decode_transfer_log(log)
            if transfer is None:
                continue
            if (transfer["block_number"], transfer["transaction_index"], transfer["log_index"]) in visited:
                continue
            transfers.append(transfer)

        if len(transfers) > 0:
            last_result = transfers[-1]
            last_block = last_result["block_number"]
            last_tx_index = last_result["transaction_index"]
            last_log_index = last_result["log_index"]
            visited.add((last_block, last_tx_index, last_log_index))
        else:
            print(f"Finished decoding transfer logs for token {token_address}.")
            break

        # Saving results to file:
        with open(save_path, "a+") as f:
            for transfer in transfers:
                f.write(json.dumps(transfer) + "\n")
            total_saved += len(transfers)

        from_block = transfers[-1]["block_number"]
        until_block = transfers[0]["block_number"]
        block_range = f"Block: {from_block} - {until_block}"
        print(f"[{block_range}] Saved {total_saved:,} to {save_path} - Took: {time.time() - start:.2f}")


if __name__ == "__main__":
    main()
