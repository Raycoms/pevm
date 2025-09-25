use std::cmp::max;
use pevm::{Bytecodes, EvmAccount};
use revm::primitives::{Address, U256, B256, TransactTo, TxEnv, AccessListItem};
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use alloy_primitives::{hex, U128, U160};
use crate::chiron::contract::Chiron;
use crate::chiron::sol::{COST_DISTR, LEN_DISTR, RES_DISTR};
use rand::distributions::{Distribution, WeightedIndex};

const GAS_LIMIT: u64 = 10_000_00;

#[path = "../data/solana_distribution.rs"]
pub mod sol;

pub mod contract;

fn generate_addresses(length: usize) -> Vec<Address> {
    (0..length).map(|_| Address::new(rand::random())).collect()
}

/// Generate only `exchange` workload
pub fn generate_exchange(num_tx: usize) -> (HashMap<Address, EvmAccount>, Bytecodes, Vec<TxEnv>) {
    let accounts: Vec<Address> = generate_addresses(num_tx);
    let chiron_address = Address::new(rand::random());

    let chiron_account = Chiron::new().build();
    let mut state = HashMap::from([(chiron_address, chiron_account)]);
    let mut txs = Vec::new();

    for account in &accounts {
        state.insert(
            *account,
            EvmAccount {
                balance: U256::from(U128::MAX),
                ..EvmAccount::default()
            },
        );
    }

    let mut rng = thread_rng();
    let mut sender_map = HashMap::new();

    for _ in 0..num_tx {
        // todo take based on rng distribution.
        let idx = rng.gen_range(0..accounts.len());
        let person = accounts[idx];

        let nonce = sender_map.get(&person).unwrap_or(&0);
        let calldata = Chiron::exchange(U256::from(idx));

        txs.push(TxEnv {
            caller: person,
            gas_limit: GAS_LIMIT,
            gas_price: U256::from(1),
            transact_to: TransactTo::Call(chiron_address),
            data: calldata,
            nonce: Some(*nonce),
            access_list: vec![AccessListItem {
                address: person,
                storage_keys: vec![B256::from(U256::from(idx))],
            }],
            ..TxEnv::default()
        });

        sender_map.insert(person, nonce + 1);
    }

    let mut bytecodes = Bytecodes::default();
    for account in state.values_mut() {
        if let Some(code) = account.code.take() {
            bytecodes.insert(account.code_hash.unwrap(), code);
        }
    }

    (state, bytecodes, txs)
}

/// Generate only `exchangetwo` workload
pub fn generate_exchange_two(num_tx: usize) -> (HashMap<Address, EvmAccount>, Bytecodes, Vec<TxEnv>) {
    let accounts: Vec<Address> = generate_addresses(num_tx);
    let chiron_address = Address::new(rand::random());

    let chiron_account = Chiron::new().build();
    let mut state = HashMap::from([(chiron_address, chiron_account)]);
    let mut txs = Vec::new();

    for account in &accounts {
        state.insert(
            *account,
            EvmAccount {
                balance: U256::from(1_000_000_000_000_000_000u128),
                ..EvmAccount::default()
            },
        );
    }

    let mut rng = thread_rng();
    let mut sender_map = HashMap::new();

    for _ in 0..num_tx {
        // todo take based on rng distribution.
        let sender_idx = rng.gen_range(0..accounts.len());
        let person = accounts[sender_idx];
        let nonce = sender_map.get(&person).unwrap_or(&0);

        let receiver_idx = rng.gen_range(0..accounts.len()) ;
        let calldata = Chiron::exchange_two(U256::from(sender_idx), U256::from(receiver_idx));

        txs.push(TxEnv {
            caller: person,
            gas_limit: GAS_LIMIT,
            gas_price: U256::from(1),
            transact_to: TransactTo::Call(chiron_address),
            data: calldata,
            nonce: Some(*nonce),
            access_list: vec![AccessListItem {
                address: person,
                storage_keys: vec![B256::from(U256::from(sender_idx)), B256::from(U256::from(receiver_idx))],
            }],
            ..TxEnv::default()
        });

        sender_map.insert(person, nonce + 1);
    }

    let mut bytecodes = Bytecodes::default();
    for account in state.values_mut() {
        if let Some(code) = account.code.take() {
            bytecodes.insert(account.code_hash.unwrap(), code);
        }
    }

    (state, bytecodes, txs)
}

/// Generate only `loop_exchange` workload
pub fn generate_loop_exchange(num_tx: usize) -> (HashMap<Address, EvmAccount>, Bytecodes, Vec<TxEnv>) {
    let accounts: Vec<Address> = generate_addresses(num_tx);
    let chiron_address = Address::new(rand::random());

    let chiron_account = Chiron::new().build();
    let mut state = HashMap::from([(chiron_address, chiron_account)]);
    let mut txs = Vec::new();

    for account in &accounts {
        state.insert(
            *account,
            EvmAccount {
                balance: U256::from(1_000_000_000_000_000_000u128),
                ..EvmAccount::default()
            },
        );
    }

    let mut rng = thread_rng();
    let mut sender_map = HashMap::new();
    let res_distribution: WeightedIndex<f64> = WeightedIndex::new(&RES_DISTR).unwrap();

    println!("start gen");


    for _ in 0..num_tx {
        let person = accounts[rng.gen_range(0..accounts.len())];
        let nonce = sender_map.get(&person).unwrap_or(&0);

        let cost_sample = COST_DISTR[rand::thread_rng().gen_range(0..COST_DISTR.len())];
        let write_len_sample = LEN_DISTR[rand::thread_rng().gen_range(0..LEN_DISTR.len())] as usize;
        let mut writes= Vec::new();
        let mut size_param = 0;
        while size_param < write_len_sample {
            size_param += 1;
            writes.push(res_distribution.sample(&mut rng));
        }

        let length_sample = (cost_sample.round() as u64 / 10).max(1);
        let length = U256::from(max(1, length_sample));

        let calldata = Chiron::loop_exchange(length, &writes.clone());

        let mut write_keys:Vec<AccessListItem> = Vec::new();
        for write in writes {
            assert!(write <= u64::MAX as usize, "write key too large for u64");
            write_keys.push(AccessListItem {
                address: Address::from(U160::from(write)),
                storage_keys: vec!(B256::ZERO),
            });
        }

        write_keys.push(AccessListItem {
            address: person.clone(),
            storage_keys: vec!(B256::ZERO),
        });

        txs.push(TxEnv {
            caller: person,
            gas_limit: GAS_LIMIT * length_sample,
            gas_price: U256::from(1),
            transact_to: TransactTo::Call(chiron_address),
            data: calldata,
            nonce: Some(*nonce),
            access_list: write_keys,
            ..TxEnv::default()
        });

        sender_map.insert(person, nonce + 1);
    }

    println!("end gen");


    let mut bytecodes = Bytecodes::default();
    for account in state.values_mut() {
        if let Some(code) = account.code.take() {
            bytecodes.insert(account.code_hash.unwrap(), code);
        }
    }

    (state, bytecodes, txs)
}
