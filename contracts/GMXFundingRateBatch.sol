// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface IDataStore {
    function getInt(bytes32 key) external view returns (int256);
}

/**
 * @dev NOT MEANT TO BE DEPLOYED.
 *
 * Reads `savedFundingFactorPerSecond` for multiple GMX V2 markets in a single
 * `eth_call`.  Invoke by calling `eth_call` with:
 *
 *   to:   null  (contract creation, never lands on-chain)
 *   data: <deployment_bytecode> ++ abi.encode(address[] markets)
 *
 * Returns:  abi.encode(int256[] values, uint8[] statuses)
 *   statuses[i] == 0 (OK):           values[i] = savedFundingFactorPerSecond for
 *                                    markets[i]; a stored zero is a genuine zero.
 *   statuses[i] == 1 (NOT_DEPLOYED): markets[i] has no code at the queried block
 *                                    (market not created yet); values[i] is 0 and
 *                                    must be ignored.
 *   statuses[i] == 2 (READ_FAILED):  getInt reverted; values[i] is 0 and must be
 *                                    treated as a failed read, never as a zero.
 *
 * Key derivation (matches Python extract_funding_datastore.py):
 *   baseKey = keccak256(abi.encode("SAVED_FUNDING_FACTOR_PER_SECOND"))
 *   key[i]  = keccak256(abi.encode(baseKey, markets[i]))
 *
 * To recompile (requires Foundry):
 *   cd contracts && forge build
 *   # deployment bytecode is in out/GMXFundingRateBatch.sol/GMXFundingRateBatchRequest.json
 */
contract GMXFundingRateBatchRequest {
    // Arbitrum mainnet DataStore — never changes post-deployment
    address private constant DATASTORE = 0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8;

    constructor(address[] memory markets) {
        IDataStore ds = IDataStore(DATASTORE);

        bytes32 baseKey = keccak256(abi.encode("SAVED_FUNDING_FACTOR_PER_SECOND"));

        uint256 n = markets.length;
        int256[] memory results = new int256[](n);
        uint8[] memory statuses = new uint8[](n);

        for (uint256 i = 0; i < n; i++) {
            if (markets[i].code.length == 0) {
                statuses[i] = 1;
                continue;
            }
            bytes32 key = keccak256(abi.encode(baseKey, markets[i]));
            try ds.getInt(key) returns (int256 value) {
                results[i] = value;
            } catch {
                statuses[i] = 2;
            }
        }

        bytes memory encoded = abi.encode(results, statuses);
        assembly {
            // Return the ABI-encoded array (skip the 32-byte length prefix of `bytes`)
            return(add(encoded, 0x20), mload(encoded))
        }
    }
}
