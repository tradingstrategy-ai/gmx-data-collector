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
 * Returns:  abi.encode(int256[] values)
 *   values[i] = savedFundingFactorPerSecond for markets[i], or 0 on failure.
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

        for (uint256 i = 0; i < n; i++) {
            bytes32 key = keccak256(abi.encode(baseKey, markets[i]));
            try ds.getInt(key) returns (int256 value) {
                results[i] = value;
            } catch {
                results[i] = 0;
            }
        }

        bytes memory encoded = abi.encode(results);
        assembly {
            // Return the ABI-encoded array (skip the 32-byte length prefix of `bytes`)
            return(add(encoded, 0x20), mload(encoded))
        }
    }
}
