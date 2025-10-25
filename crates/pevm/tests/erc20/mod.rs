//! ERC-20 testing module

/// This module provides ERC-20 contract functionality.
pub mod contract;

use std::collections::HashMap;
use alloy_primitives::{B256};
use alloy_rpc_types_eth::AccessListItem;
use rand::distributions::{Distribution, WeightedIndex};
use rand::prelude::ThreadRng;
use rand::thread_rng;
use contract::ERC20Token;
use pevm::{Bytecodes, ChainState, EvmAccount};
use revm::primitives::{uint, Address, TransactTo, TxEnv, U256};
use crate::p2p::{TX_FROM, TX_TO};

/// The maximum amount of gas that can be used for a transaction in this configuration.
pub const GAS_LIMIT: u64 = 100_000;

/// An estimated amount of gas that is expected to be consumed by typical transactions.
pub const ESTIMATED_GAS_USED : u64 = 29_738;

/// Sometimes we want duplicates to test
/// dependent transactions, sometimes we want to guarantee non-duplicates
/// for independent benchmarks.
fn generate_addresses(length: usize) -> Vec<Address> {
    (0..length).map(|_| Address::new(rand::random())).collect()
}

/// Generates a cluster of blockchain transactions for testing or simulation purposes.
pub fn generate_cluster(
    num_families: usize,
    num_people_per_family: usize,
    num_transfers_per_person: usize,
) -> (ChainState, Bytecodes, Vec<TxEnv>) {
    let families: Vec<Vec<Address>> = (0..num_families)
        .map(|_| generate_addresses(num_people_per_family))
        .collect();

    let people_addresses: Vec<Address> = families.clone().into_iter().flatten().collect();

    let gld_address = Address::new(rand::random());

    let gld_account = ERC20Token::new("Gold Token", "GLD", 18, 222_222_000_000_000_000_000_000u128)
        .add_balances(&people_addresses, uint!(1_000_000_000_000_000_000_U256))
        .build();

    let mut state = ChainState::from_iter([(gld_address, gld_account)]);
    let mut txs = Vec::new();

    for person in &people_addresses {
        state.insert(
            *person,
            EvmAccount {
                balance: uint!(4_567_000_000_000_000_000_000_U256),
                ..EvmAccount::default()
            },
        );
    }

    for nonce in 0..num_transfers_per_person {
        for family in &families {
            for person in family {
                let recipient = family[(rand::random::<usize>()) % (family.len())];
                let calldata = ERC20Token::transfer(recipient, U256::from(rand::random::<u8>()));

                txs.push(TxEnv {
                    caller: *person,
                    gas_limit: GAS_LIMIT,
                    gas_price: U256::from(0xb2d05e07u64),
                    transact_to: TransactTo::Call(gld_address),
                    data: calldata,
                    nonce: Some(nonce as u64),
                    ..TxEnv::default()
                })
            }
        }
    }

    let mut bytecodes = Bytecodes::default();
    for account in state.values_mut() {
        let code = account.code.take();
        if let Some(code) = code {
            bytecodes.insert(account.code_hash.unwrap(), code);
        }
    }

    (state, bytecodes, txs)
}

/// Generates a cluster of blockchain transactions for testing or simulation purposes.
/// Follows chiron random distribution
pub fn generate_chiron_cluster(
    num_tx: usize,
) -> (ChainState, Bytecodes, Vec<TxEnv>) {
    let families: Vec<Address> = generate_addresses(52_000);

    let gld_address = Address::new(rand::random());

    let gld_account = ERC20Token::new("Gold Token", "GLD", 18, 222_222_000_000_000_000_000_000u128)
        .add_balances(&families, uint!(1_000_000_000_000_000_000_U256))
        .build();

    let mut state = ChainState::from_iter([(gld_address, gld_account)]);
    let mut txs = Vec::new();

    for person in &families {
        state.insert(
            *person,
            EvmAccount {
                balance: uint!(4_567_000_000_000_000_000_000_U256),
                ..EvmAccount::default()
            },
        );
    }

    let p2p_receiver_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_TO).unwrap();
    let p2p_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_FROM).unwrap();
    let mut rng: ThreadRng = thread_rng();

    let mut sender_map = HashMap::new();

    // todo add signature analysis to workload
    
    for _ in 0..num_tx {
        let recipient = families[p2p_receiver_distribution.sample(&mut rng) % families.len()];
        let calldata = ERC20Token::transfer(recipient, U256::from(rand::random::<u8>()));
        let person = families[p2p_sender_distribution.sample(&mut rng) % families.len()];
        let nonce = sender_map.get(&person).unwrap_or(&0);

        txs.push(TxEnv {
            caller: person,
            gas_limit: GAS_LIMIT,
            gas_price: U256::from(0xb2d05e07u64),
            transact_to: TransactTo::Call(gld_address),
            data: calldata,
            nonce: Some(*nonce),
            access_list: vec!(AccessListItem {address: person, storage_keys: vec!(B256::ZERO)}, AccessListItem {address: recipient, storage_keys: vec!(B256::ZERO)}),
            ..TxEnv::default()
        });

        sender_map.insert(person, nonce+1);
    }

    let mut bytecodes = Bytecodes::default();
    for account in state.values_mut() {
        let code = account.code.take();
        if let Some(code) = code {
            bytecodes.insert(account.code_hash.unwrap(), code);
        }
    }

    (state, bytecodes, txs)
}