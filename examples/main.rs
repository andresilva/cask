extern crate cask;
extern crate env_logger;
extern crate log;
extern crate rand;
extern crate time;

use std::env;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use env_logger::LogBuilder;
use rand::{Rng, SeedableRng, XorShiftRng};
use log::LogRecord;

use cask::Cask;

fn init_logger() {
    let format = |record: &LogRecord| {
        let ts = time::strftime("%Y-%m-%d %H:%M:%S,%f", &time::now()).unwrap();
        format!("{} {} {} {}",
                &ts[..ts.len() - 6],
                record.level(),
                record.location().module_path(),
                record.args())
    };

    let mut builder = LogBuilder::new();
    builder.format(format);

    if let Ok(s) = env::var("RUST_LOG") {
        builder.parse(&s);
    }

    builder.init().unwrap();
}

fn main() {
    init_logger();

    let cask = Cask::open("test.db", false).unwrap();

    let seed = [1, 2, 3, 4];

    const N_THREADS: usize = 8;
    const WRITE_PROBABILITY: f64 = 0.1;
    const DURATION_SECS: u64 = 10;

    let base_value = rand::thread_rng().gen::<usize>();

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
                if let Ok(_) = rx.try_recv() {
                    break;
                }

                let r = rng.next_f64();
                if r < WRITE_PROBABILITY {
                    let key = (id * i).to_string();
                    cask.put(key, &vec).unwrap();
                } else {
                    let key = ((base_value + (id * i)) * r as usize).to_string();
                    cask.get(key).unwrap();
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
