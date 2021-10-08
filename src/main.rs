mod core;
mod threadpool;
mod lqp;
mod ps_protocol;
mod query;
mod transaction;

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::fs::{self};

use crate::ps_protocol::handle_connection;
use crate::threadpool::ThreadPool;
use crate::core::AttributeValueContainer;
use crate::transaction::TransactionManager;

fn insert_into_sorted_vec(val: i64, vec: &mut Vec<i64>) {
    let mut low = 0;
    let mut high = vec.len();
    while high != low {
        let center = (low + high) / 2;
        if center < vec.len() {
            if vec[center] == val {
                return;
            } else if vec[center] < val {
                low = center + 1;
            } else {
                high = center;
            }
        }
    }
    vec.insert(low, val);
}

#[cfg(test)]
mod tests {
    #[test]
    fn insert_into_sorted_vec() {
        let mut vec = Vec::new();
        super::insert_into_sorted_vec(1, &mut vec);
        assert_eq!(vec, vec![1]);
        super::insert_into_sorted_vec(0, &mut vec);
        assert_eq!(vec, vec![0, 1]);
        super::insert_into_sorted_vec(0, &mut vec);
        assert_eq!(vec, vec![0, 1]);
        super::insert_into_sorted_vec(2, &mut vec);
        assert_eq!(vec, vec![0, 1, 2]);
    }
}

fn main() {
    // load TPC-H data
    //  NOTE: for now, only importing integer columns from LINEITEM as a first step
    let mut dicts: HashMap<String, Vec<i64>> = HashMap::new();
    dicts.insert(String::from("L_ORDERKEY"), Vec::new());
    dicts.insert(String::from("L_PARTKEY"), Vec::new());
    dicts.insert(String::from("L_SUPPKEY"), Vec::new());
    dicts.insert(String::from("L_LINENUMBER"), Vec::new());
    dicts.insert(String::from("L_QUANTITY"), Vec::new());
    let mut dvs: HashMap<String, Vec<i32>> = HashMap::new();
    dvs.insert(String::from("L_ORDERKEY"), Vec::new());
    dvs.insert(String::from("L_PARTKEY"), Vec::new());
    dvs.insert(String::from("L_SUPPKEY"), Vec::new());
    dvs.insert(String::from("L_LINENUMBER"), Vec::new());
    dvs.insert(String::from("L_QUANTITY"), Vec::new());
    for f in fs::read_dir("tpc-h/sf1").unwrap() {
        let f = f.unwrap();
        let path = f.path();
        if path.is_file() {
            if path.extension().unwrap().to_str().unwrap() == "tbl" {
                let tbl_name = path.file_stem().unwrap().to_str().unwrap();
                println!("{}", tbl_name);
                if tbl_name == "lineitem" {
                    for (i, line) in fs::read_to_string(path).unwrap().lines().enumerate() {
                        for (j, value) in line.trim_matches('|').split('|').enumerate() {
                            let col = match j {
                                0 => Some("L_ORDERKEY"),
                                1 => Some("L_PARTKEY"),
                                2 => Some("L_SUPPKEY"),
                                3 => Some("L_LINENUMBER"),
                                4 => Some("L_QUANTITY"),
                                _ => None
                            };

                            if let Some(col_name) = col {
                                insert_into_sorted_vec(i64::from_str_radix(value, 10).unwrap(), dicts.get_mut(col_name).unwrap());
                            }
                        }
                        if i % 100000 == 0 {
                            println!("{}", i);
                        }
                    }
                }
            }
        }
    }
    println!("{:?}", dicts.get("L_QUANTITY").unwrap());

    let dict = Box::new(core::BigIntDict { entries: vec![1, 5, 7, 2311] });
    let mut avc = core::MainAttributeValueContainer::<i64> { data: Vec::new(), dict: dict };
    avc.data.push(1);
    avc.data.push(2);
    avc.data.push(0);
    avc.data.push(0);
    avc.data.push(avc.null_value_id() as u32);
    avc.data.push(1);
    avc.data.push(3);
    avc.data.push(1);
    avc.data.push(0);
    avc.data.push(avc.null_value_id() as u32);
    avc.data.push(1);
    let db = Arc::new(RwLock::new(core::Database { transaction_manager: Mutex::new(TransactionManager {}), avc: RwLock::new(Box::new(avc)) }));
    // avc lookup test
    //{
    //    let db = db.read().unwrap();
    //    let avc = db.avc.read().unwrap();
    //    for i in 0..avc.len() {
    //        println!("{:?}", avc.lookup(i));
    //    }
    //}

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