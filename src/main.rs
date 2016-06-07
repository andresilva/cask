extern crate cask;
extern crate rand;

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use cask::Cask;
use rand::{Rng, SeedableRng, XorShiftRng};

fn main() {
    let cask = Cask::open("test.db", false);
    println!("opened db");

    let seed = [1, 2, 3, 4];

    const N_THREADS: usize = 8;
    const WRITE_PROBABILITY: f64 = 0.1;
    const DURATION_SECS: u64 = 10;

    let base_value = rand::thread_rng().gen::<usize>();
    println!("{}", base_value);

    let mut threads = Vec::new();
    let mut txs = Vec::new();
    for id in 1..N_THREADS + 1 {
        let (tx, rx) = mpsc::channel();
        let cask = cask.clone();
        let vec = vec![1u8; 4096];
        let mut rng: XorShiftRng = SeedableRng::from_seed(seed);

        let t = thread::spawn(move || {
            let mut i = 0;
            loop {
                match rx.try_recv() {
                    Ok(_) => break,
                    _ => {}
                }

                let r = rng.next_f64();
                if r < WRITE_PROBABILITY {
                    let key = (id * i).to_string().as_bytes().to_vec();
                    cask.put(key, &vec);
                } else {
                    let key = ((base_value + (id * i)) * r as usize).to_string();
                    cask.get(key.as_bytes());
                }

                i += 1
            }
        });
        threads.push(t);
        txs.push(tx);
    }

    thread::sleep(Duration::from_secs(DURATION_SECS));

    for tx in txs {
        tx.send(()).unwrap();
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
