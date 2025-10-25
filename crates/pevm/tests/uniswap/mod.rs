//! Uniswap testing module

/// contract module
pub mod contract;

use alloy_rpc_types_eth::AccessListItem;
use rand::prelude::ThreadRng;
use rand::{thread_rng, Rng, RngCore, SeedableRng};
use rand::distributions::{Distribution, WeightedIndex};
use rand::rngs::StdRng;
use crate::erc20::contract::ERC20Token;
use contract::{SingleSwap, SwapRouter, UniswapV3Factory, UniswapV3Pool, WETH9};
use pevm::{Bytecodes, ChainState, EvmAccount};
use revm::primitives::{fixed_bytes, uint, Address, Bytes, TransactTo, TxEnv, B256, U256};

/// Avg Uniswap distribution.
pub const AVG : [f64; 267] = [1.0859375, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.7734375, 3.0, 3.0, 3.0, 3.0, 3.0, 3.19921875, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.34765625, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.91015625, 6.0, 6.0, 6.0, 6.0, 6.0, 6.6328125, 7.0, 7.0, 7.0, 7.0, 7.0, 7.74609375, 8.0, 8.0, 8.0, 8.0, 8.0, 8.875, 9.0, 9.0, 9.0, 9.0, 9.28515625, 10.0, 10.0, 10.0, 10.0, 10.0, 10.86328125, 11.0, 11.0, 11.0, 11.0, 11.76171875, 12.0, 12.0, 12.0, 12.171875, 13.0, 13.0, 13.0, 13.0, 13.58203125, 14.0, 14.0, 14.0, 14.0, 14.96484375, 15.0, 15.0, 15.0, 15.9140625, 16.0, 16.0, 16.0, 16.8203125, 17.0, 17.0, 17.0, 17.8125, 18.0, 18.0, 18.1015625, 19.0, 19.0, 19.0, 19.65234375, 20.0, 20.0, 20.0, 20.92578125, 21.0, 21.0, 21.5859375, 22.0, 22.0, 22.37109375, 23.0, 23.0, 23.43359375, 24.0, 24.0, 24.703125, 25.0, 25.0, 25.94921875, 26.0, 26.28125, 27.0, 27.0, 27.796875, 28.0, 28.37109375, 29.0, 29.1171875, 30.0, 30.0, 30.83984375, 31.0, 31.734375, 32.0, 32.7109375, 33.0, 33.68359375, 34.0, 34.8515625, 35.07421875, 36.0, 36.29296875, 37.0, 37.6171875, 38.07421875, 39.0, 39.53515625, 40.04296875, 41.0, 41.546875, 42.13671875, 43.0, 43.76953125, 44.40625, 45.09765625, 46.0, 46.796875, 47.47265625, 48.19140625, 49.0, 49.9296875, 50.734375, 51.671875, 52.52734375, 53.46875, 54.3515625, 55.40234375, 56.4375, 57.39453125, 58.515625, 59.6015625, 60.75390625, 61.92578125, 63.1171875, 64.25390625, 65.46484375, 66.85546875, 68.40625, 69.76953125, 71.34375, 72.57421875, 74.04296875, 75.609375, 77.13671875, 78.5390625, 80.24609375, 81.80859375, 83.64453125, 85.5703125, 87.49609375, 89.453125, 91.47265625, 93.5546875, 95.5859375, 97.9140625, 100.1796875, 102.76953125, 105.1953125, 107.55078125, 109.9921875, 112.80859375, 115.58203125, 118.4921875, 121.4296875, 124.4296875, 127.515625, 130.91796875, 134.51953125, 137.72265625, 141.3671875, 145.08203125, 148.984375, 152.99609375, 157.17578125, 161.515625, 166.09375, 170.84765625, 175.98046875, 180.90625, 186.2109375, 191.67578125, 197.83203125, 204.0078125, 210.03125, 216.25, 223.0078125, 230.359375, 237.37109375, 245.17578125, 253.41796875, 262.25, 270.17578125, 278.76953125, 288.41015625, 297.8515625, 308.51953125, 319.328125, 330.10546875, 341.83984375, 356.16796875, 369.60546875, 383.78515625, 399.296875, 414.51171875, 431.42578125, 449.48828125, 467.6953125, 490.42578125, 514.87109375, 539.19140625, 567.421875, 597.92578125, 634.40625, 668.5703125, 709.82421875, 757.7421875, 810.15234375, 869.328125, 939.359375, 1028.2890625, 1133.63671875, 1258.80078125, 1395.26953125, 1573.75390625, 1803.3984375, 2125.69921875, 2600.92578125, 3387.18359375, 5117.73046875, 20296.42578125];
/// Contended bursty uniswap distribution.
pub const BURSTY : [f64; 330] = [1.0, 1.1875, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.6875, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.25, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0625, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.5625, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.9375, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.25, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.75, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.9375, 10.0, 10.0, 10.0, 10.0, 10.4375, 11.0, 11.0, 11.0, 11.0, 11.0, 11.4375, 12.0, 12.0, 12.0, 12.0, 12.5625, 13.0, 13.0, 13.0, 13.0625, 14.0, 14.0, 14.0, 14.0, 14.5625, 15.0, 15.0, 15.0, 15.0, 15.375, 16.0, 16.0, 16.0, 16.0, 16.75, 17.0, 17.0, 17.0, 17.0, 17.625, 18.0, 18.0, 18.0, 18.6875, 19.0, 19.0, 19.0, 19.9375, 20.0, 20.0, 20.0, 20.5, 21.0, 21.0, 21.0, 21.0, 21.9375, 22.0, 22.0, 22.0, 22.9375, 23.0, 23.0, 23.0, 23.8125, 24.0, 24.0, 24.8125, 25.0, 25.0, 25.125, 26.0, 26.0, 26.5, 27.0, 27.0, 27.5, 28.0, 28.0, 28.375, 29.0, 29.1875, 30.0, 30.75, 31.0, 31.5625, 32.0, 32.75, 33.0, 33.0625, 34.0, 34.0, 34.875, 35.0, 35.5625, 36.0, 36.0625, 37.0, 37.0, 37.75, 38.0, 38.4375, 39.0, 39.75, 40.0, 40.4375, 41.0625, 42.0, 42.3125, 43.0, 43.6875, 44.0, 44.9375, 45.625, 46.0, 46.9375, 47.0, 47.625, 48.375, 49.0, 49.5625, 50.0625, 51.0, 52.0, 52.875, 53.3125, 54.25, 55.25, 56.0, 56.5625, 57.0, 58.0625, 59.0625, 60.0, 61.0, 61.875, 62.5, 63.625, 64.75, 65.625, 66.8125, 68.1875, 69.5, 70.6875, 71.25, 72.625, 73.4375, 74.75, 76.0625, 77.25, 78.0, 79.375, 80.3125, 81.1875, 82.625, 84.0625, 85.875, 87.5625, 89.5625, 90.8125, 92.5625, 94.0, 95.625, 97.5625, 99.1875, 100.375, 102.1875, 103.75, 105.3125, 106.375, 108.1875, 109.9375, 112.125, 114.75, 117.25, 120.375, 122.8125, 124.875, 126.6875, 128.375, 131.625, 135.5625, 138.0, 140.5, 143.1875, 146.75, 150.8125, 153.75, 156.8125, 159.3125, 161.75, 165.375, 168.5, 173.625, 177.75, 181.5625, 184.1875, 188.1875, 191.9375, 196.4375, 201.625, 206.1875, 211.625, 218.1875, 225.5, 231.5625, 238.625, 244.5625, 254.5, 263.0, 268.0, 273.625, 279.125, 286.125, 296.8125, 307.3125, 313.875, 321.25, 329.0, 337.125, 348.75, 362.5, 373.0625, 383.25, 395.375, 406.75, 420.25, 431.8125, 446.6875, 465.375, 484.1875, 504.6875, 528.625, 551.3125, 570.8125, 585.3125, 604.5625, 629.5, 661.6875, 693.25, 732.625, 770.75, 815.4375, 863.375, 919.875, 978.375, 1045.5625, 1130.8125, 1258.4375, 1370.875, 1530.25, 1682.3125, 1929.4375, 2207.4375, 2527.6875, 3190.0625, 4102.125, 6946.9375, 17255.875, 39644.5, 100004.0625];

/// The maximum amount of gas that can be used for a transaction in this configuration.
pub const GAS_LIMIT: u64 = 800_000;

/// An estimated amount of gas that is expected to be consumed by typical transactions.
pub const ESTIMATED_GAS_USED: u64 = 155_934;

/// Generates a simulated transaction cluster for testing.
pub fn generate_cluster(
    num_people: usize,
    num_swaps_per_person: usize,
) -> (ChainState, Bytecodes, Vec<TxEnv>) {
    // TODO: Better randomness control. Sometimes we want duplicates to test
    // dependent transactions, sometimes we want to guarantee non-duplicates
    // for independent benchmarks.
    let people_addresses: Vec<Address> = (0..num_people)
        .map(|_| Address::new(rand::random()))
        .collect();

    // make sure dai_address < usdc_address
    let (dai_address, usdc_address) = {
        let x = Address::new(rand::random());
        let y = Address::new(rand::random());
        (std::cmp::min(x, y), std::cmp::max(x, y))
    };

    let pool_init_code_hash = B256::new(rand::random());
    let swap_router_address = Address::new(rand::random());
    let single_swap_address = Address::new(rand::random());
    let weth9_address = Address::new(rand::random());
    let owner = Address::new(rand::random());
    let factory_address = Address::new(rand::random());
    let nonfungible_position_manager_address = Address::new(rand::random());
    let pool_address = UniswapV3Pool::new(dai_address, usdc_address, factory_address)
        .get_address(factory_address, pool_init_code_hash);

    let weth9_account = WETH9::new().build();

    let dai_account = ERC20Token::new("DAI", "DAI", 18, 222_222_000_000_000_000_000_000u128)
        .add_balances(&[pool_address], uint!(111_111_000_000_000_000_000_000_U256))
        .add_balances(&people_addresses, uint!(1_000_000_000_000_000_000_U256))
        .add_allowances(
            &people_addresses,
            single_swap_address,
            uint!(1_000_000_000_000_000_000_U256),
        )
        .build();

    let usdc_account = ERC20Token::new("USDC", "USDC", 18, 222_222_000_000_000_000_000_000u128)
        .add_balances(&[pool_address], uint!(111_111_000_000_000_000_000_000_U256))
        .add_balances(&people_addresses, uint!(1_000_000_000_000_000_000_U256))
        .add_allowances(
            &people_addresses,
            single_swap_address,
            uint!(1_000_000_000_000_000_000_U256),
        )
        .build();

    let factory_account = UniswapV3Factory::new(owner)
        .add_pool(dai_address, usdc_address, pool_address)
        .build(factory_address);

    let pool_account = UniswapV3Pool::new(dai_address, usdc_address, factory_address)
        .add_position(
            nonfungible_position_manager_address,
            -600000,
            600000,
            [
                uint!(0x00000000000000000000000000000000000000000000178756e190b388651605_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
            ],
        )
        .add_tick(
            -600000,
            [
                uint!(0x000000000000178756e190b388651605000000000000178756e190b388651605_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                uint!(0x0100000001000000000000000000000000000000000000000000000000000000_U256),
            ],
        )
        .add_tick(
            600000,
            [
                uint!(0xffffffffffffe878a91e6f4c779ae9fb000000000000178756e190b388651605_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                uint!(0x0100000000000000000000000000000000000000000000000000000000000000_U256),
            ],
        )
        .build(pool_address);

    let swap_router_account = SwapRouter::new(weth9_address, factory_address, pool_init_code_hash).build();
    let single_swap_account = SingleSwap::new(swap_router_address, dai_address, usdc_address).build();

    let mut state = ChainState::from_iter([
        (weth9_address, weth9_account),
        (dai_address, dai_account),
        (usdc_address, usdc_account),
        (factory_address, factory_account),
        (pool_address, pool_account),
        (swap_router_address, swap_router_account),
        (single_swap_address, single_swap_account),
    ]);

    for person in &people_addresses {
        state.insert(
            *person,
            EvmAccount {
                balance: uint!(4_567_000_000_000_000_000_000_U256),
                ..EvmAccount::default()
            },
        );
    }

    let mut txs = Vec::new();

    // sellToken0(uint256): c92b0891
    // sellToken1(uint256): 6b055260
    // buyToken0(uint256,uint256): 8dc33f82
    // buyToken1(uint256,uint256): b2db18a2
    for nonce in 0..num_swaps_per_person {
        for person in &people_addresses {
            let data_bytes: Vec<u8> = match nonce % 4 {
                0 => [
                    &fixed_bytes!("c92b0891")[..],
                    &B256::from(U256::from(2000))[..],
                ]
                .concat(),
                1 => [
                    &fixed_bytes!("6b055260")[..],
                    &B256::from(U256::from(2000))[..],
                ]
                .concat(),
                2 => [
                    &fixed_bytes!("8dc33f82")[..],
                    &B256::from(U256::from(1000))[..],
                    &B256::from(U256::from(2000))[..],
                ]
                .concat(),
                3 => [
                    &fixed_bytes!("b2db18a2")[..],
                    &B256::from(U256::from(1000))[..],
                    &B256::from(U256::from(2000))[..],
                ]
                .concat(),
                _ => Default::default(),
            };

            txs.push(TxEnv {
                caller: *person,
                gas_limit: GAS_LIMIT,
                gas_price: U256::from(0xb2d05e07u64),
                transact_to: TransactTo::Call(single_swap_address),
                data: Bytes::from(data_bytes),
                nonce: Some(nonce as u64),
                ..TxEnv::default()
            })
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

/// Generates an uniswap trade for a specific trade pair.
pub fn generate_trading_history(
    block_size: usize,
    bursty: bool
) -> (ChainState, Bytecodes, Vec<TxEnv>) {

    let benchmark_collection : &[f64];
    if bursty {
        benchmark_collection = &BURSTY;
    } else {
        benchmark_collection = &AVG;
    }

    let uniswap_distribution: WeightedIndex<f64> = WeightedIndex::new(benchmark_collection).unwrap();

    let mut origin_rng: ThreadRng = thread_rng();

    let seed = origin_rng.next_u64();

    let mut rng = StdRng::seed_from_u64(seed);

    println!("seed {}", seed);

    // 0.0714 ms atm per uniswap trade. This seems little?

    let mut pairs = Vec::new();
    for _ in 0..benchmark_collection.len() {
        pairs.push((Address::new(rng.gen()), Address::new(rng.gen())));
    }

    let pool_init_code_hash = B256::from([
        0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00,
        0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00,
        0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00,
        0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00,
    ]);

    let swap_router_address = Address::new(rng.gen());
    let weth9_address = Address::new(rng.gen());
    let owner = Address::new(rng.gen());
    let factory_address = Address::new(rng.gen());
    let nonfungible_position_manager_address = Address::new(rng.gen());

    let people_addresses: Vec<Address> = (0..block_size)
        .map(|_| Address::new(rng.gen()))
        .collect();

    let mut factory_account = UniswapV3Factory::new(owner);
    let swap_router_account =
        SwapRouter::new(weth9_address, factory_address, pool_init_code_hash).build();

    let mut swap_addresses = Vec::new();
    let mut state = ChainState::default();
    for (coin1, coin2) in pairs.iter() {
        let pool_address = UniswapV3Pool::new(*coin1, *coin2, factory_address)
            .get_address(factory_address, pool_init_code_hash);

        let single_swap_address = Address::new(rng.gen());

        let dai_account = ERC20Token::new(&coin1.to_string()[0..16], &coin1.to_string()[0..16], 18, 222_222_000_000_000_000_000_000u128)
            .add_balances(&[pool_address], uint!(111_111_000_000_000_000_000_000_U256))
            .add_balances(&people_addresses, uint!(1_000_000_000_000_000_000_U256))
            .add_allowances(
                &people_addresses,
                single_swap_address,
                uint!(1_000_000_000_000_000_000_U256),
            )
            .build();

        let usdc_account = ERC20Token::new(&coin2.to_string()[0..16], &coin2.to_string()[0..16], 18, 222_222_000_000_000_000_000_000u128)
            .add_balances(&[pool_address], uint!(111_111_000_000_000_000_000_000_U256))
            .add_balances(&people_addresses, uint!(1_000_000_000_000_000_000_U256))
            .add_allowances(
                &people_addresses,
                single_swap_address,
                uint!(1_000_000_000_000_000_000_U256),
            )
            .build();

        factory_account.add_pool(*coin1, *coin2, pool_address);

        let pool_account = UniswapV3Pool::new(*coin1, *coin2, factory_address)
            .add_position(
                nonfungible_position_manager_address,
                -600000,
                600000,
                [
                    uint!(0x00000000000000000000000000000000000000000000178756e190b388651605_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                ],
            )
            .add_tick(
                -600000,
                [
                    uint!(0x000000000000178756e190b388651605000000000000178756e190b388651605_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                    uint!(0x0100000001000000000000000000000000000000000000000000000000000000_U256),
                ],
            )
            .add_tick(
                600000,
                [
                    uint!(0xffffffffffffe878a91e6f4c779ae9fb000000000000178756e190b388651605_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                    uint!(0x0000000000000000000000000000000000000000000000000000000000000000_U256),
                    uint!(0x0100000000000000000000000000000000000000000000000000000000000000_U256),
                ],
            )
            .build(pool_address);

        let single_swap_account = SingleSwap::new(swap_router_address, *coin1, *coin2).build();
        swap_addresses.push(single_swap_address.clone());
        state.insert(*coin1, dai_account);
        state.insert(*coin2, usdc_account);
        state.insert(pool_address, pool_account);
        state.insert(single_swap_address, single_swap_account);
    }

    state.insert(swap_router_address, swap_router_account);
    state.insert(factory_address, factory_account.build(factory_address));

    let weth9_account = WETH9::new().build();
    state.insert(weth9_address, weth9_account);

    for person in &people_addresses {
        state.insert(
            *person,
            EvmAccount {
                balance: uint!(4_567_000_000_000_000_000_000_U256),
                ..EvmAccount::default()
            },
        );
    }

    let mut txs = Vec::new();

    // sellToken0(uint256): c92b0891
    // sellToken1(uint256): 6b055260
    // buyToken0(uint256,uint256): 8dc33f82
    // buyToken1(uint256,uint256): b2db18a2
    let mut idx = 0;
    for person in &people_addresses {

        let pair = swap_addresses.get(uniswap_distribution.sample(&mut rng)).unwrap();
        idx+=1;
        let data_bytes: Vec<u8> = match idx % 4 {
            0 => [
                &fixed_bytes!("c92b0891")[..],
                &B256::from(U256::from(2000))[..],
            ]
                .concat(),
            1 => [
                &fixed_bytes!("6b055260")[..],
                &B256::from(U256::from(2000))[..],
            ]
                .concat(),
            2 => [
                &fixed_bytes!("8dc33f82")[..],
                &B256::from(U256::from(1000))[..],
                &B256::from(U256::from(2000))[..],
            ]
                .concat(),
            3 => [
                &fixed_bytes!("b2db18a2")[..],
                &B256::from(U256::from(1000))[..],
                &B256::from(U256::from(2000))[..],
            ]
                .concat(),
            _ => Default::default(),
        };

        txs.push(TxEnv {
            caller: *person,
            gas_limit: GAS_LIMIT,
            gas_price: U256::from(0xb2d05e07u64),
            transact_to: TransactTo::Call(pair.clone()),
            data: Bytes::from(data_bytes),
            nonce: Some(0 as u64),
            access_list: vec!(AccessListItem {address: pair.clone(), storage_keys: vec!(B256::ZERO)}),
            ..TxEnv::default()
        })
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
