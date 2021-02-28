mod core;
mod threadpool;
mod ps_protocol;

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::ps_protocol::handle_connection;
use crate::threadpool::ThreadPool;

fn main() {
    let dict = Box::new(core::BigIntDict { entries: vec![1, 5, 7, 2311] });
    let mut avc = core::MainAttributeValueContainer::<i64> { data: Vec::new(), dict: dict };
    avc.data.push(1);
    avc.data.push(2);
    avc.data.push(0);
    avc.data.push(0);
    avc.data.push(1);
    avc.data.push(3);
    avc.data.push(1);
    avc.data.push(0);
    avc.data.push(1);
    let db = Arc::new(RwLock::new(core::Database { avc: RwLock::new(Box::new(avc)) }));
    // avc lookup test
    {
        let db = db.read().unwrap();
        let avc = db.avc.read().unwrap();
        for i in 0..avc.len() {
            println!("{}", avc.lookup(i));
        }
    }

    let pool = Arc::new(Mutex::new(ThreadPool::new(4)));
    let pool_clone = pool.clone();
    let db_clone = db.clone();
    let ps_protocol_listener = move || {
        let pool = pool_clone;
        let listener = TcpListener::bind("127.0.0.1:5432").unwrap();
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let db = db_clone.clone();
            pool.lock().unwrap().execute(move || {
                handle_connection(stream, db)
            });
        }
    };
    pool.lock().unwrap().execute(ps_protocol_listener);

    loop {}
}