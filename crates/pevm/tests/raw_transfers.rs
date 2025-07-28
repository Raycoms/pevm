//! Test raw transfers -- only send some ETH from one account to another without extra data.

use alloy_primitives::B256;
use alloy_rpc_types_eth::AccessListItem;
use rand::distributions::{Distribution, WeightedIndex};
use rand::prelude::ThreadRng;
use pevm::{chain::PevmEthereum, InMemoryStorage};
use rand::{random, thread_rng};
use revm::primitives::{alloy_primitives::U160, env::TxEnv, Address, TransactTo, U256};
use crate::p2p::{TX_FROM, TX_TO};

pub mod common;

/// p2p evaluation data.
#[path = "data/p2p.rs"]
pub mod p2p;

#[test]
fn raw_transfers_independent() {

    let p2p_receiver_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_FROM).unwrap();
    let p2p_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_TO).unwrap();
    let mut rng: ThreadRng = thread_rng();

    let block_size = 47620; // number of transactions
    common::test_execute_revm(
        &PevmEthereum::mainnet(),
        // Mock the beneficiary account (`Address:ZERO`) and the next `block_size` user accounts.
        InMemoryStorage::new(
            (0..=block_size).map(common::mock_account).collect(),
            Default::default(),
            Default::default(),
        ),
        // Mock `block_size` transactions sending some tokens to itself.
        // Skipping `Address::ZERO` as the beneficiary account.
        (1..=block_size)
            .map(|_| {
                let sender = Address::from(U160::from(p2p_sender_distribution.sample(&mut rng)));
                let receiver = Address::from(U160::from(p2p_receiver_distribution.sample(&mut rng)));

                TxEnv {
                    caller: sender,
                    transact_to: TransactTo::Call(receiver),
                    value: U256::from(1),
                    gas_limit: common::RAW_TRANSFER_GAS_LIMIT * 2,
                    gas_price: U256::from(1),
                    access_list: vec!(AccessListItem {address: sender, storage_keys: vec!(B256::ZERO)}, AccessListItem {address: receiver, storage_keys: vec!(B256::ZERO)}),
                    ..TxEnv::default()
                }
            })
            .collect(),
    );
}

// The same sender sending multiple transfers with increasing nonces.
// These must be detected and executed in the correct order.
#[test]
fn raw_transfers_same_sender_multiple_txs() {
    let block_size = 5_000; // number of transactions

    let same_sender_address = Address::from(U160::from(1));
    let mut same_sender_nonce: u64 = 0;

    common::test_execute_revm(
        &PevmEthereum::mainnet(),
        // Mock the beneficiary account (`Address:ZERO`) and the next `block_size` user accounts.
        InMemoryStorage::new(
            (0..=block_size).map(common::mock_account).collect(),
            Default::default(),
            Default::default(),
        ),
        (1..=block_size)
            .map(|i| {
                // Insert a "parallel" transaction every ~256 transactions
                // after the first ~30 guaranteed from the same sender.
                let (address, nonce) = if i > 30 && random::<u8>() == 0 {
                    (Address::from(U160::from(i)), 1)
                } else {
                    same_sender_nonce += 1;
                    (same_sender_address, same_sender_nonce)
                };
                TxEnv {
                    caller: address,
                    transact_to: TransactTo::Call(address),
                    value: U256::from(1),
                    gas_limit: common::RAW_TRANSFER_GAS_LIMIT,
                    gas_price: U256::from(1),
                    nonce: Some(nonce),
                    ..TxEnv::default()
                }
            })
            .collect(),
    );
}

#[test]
fn ethereum_empty_alloy_block() {
    common::test_independent_raw_transfers(&PevmEthereum::mainnet(), 0);
}

#[test]
fn ethereum_one_tx_alloy_block() {
    common::test_independent_raw_transfers(&PevmEthereum::mainnet(), 1);
}

#[test]
fn ethereum_independent_raw_transfers() {
    common::test_independent_raw_transfers(&PevmEthereum::mainnet(), 100_000);
}

#[cfg(feature = "optimism")]
#[test]
fn optimism_empty_alloy_block() {
    use pevm::chain::PevmOptimism;
    common::test_independent_raw_transfers(&PevmOptimism::mainnet(), 0);
}

#[cfg(feature = "optimism")]
#[test]
fn optimism_one_tx_alloy_block() {
    use pevm::chain::PevmOptimism;
    common::test_independent_raw_transfers(&PevmOptimism::mainnet(), 1);
}

#[cfg(feature = "optimism")]
#[test]
fn optimism_independent_raw_transfers() {
    use pevm::chain::PevmOptimism;
    common::test_independent_raw_transfers(&PevmOptimism::mainnet(), 100_000);
}
