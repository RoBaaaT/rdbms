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
use crate::core::ValueId;
use crate::transaction::TransactionManager;

fn main() {
    // load TPC-H data
    //  NOTE: for now, only importing integer columns from LINEITEM as a first step
    let mut columns: HashMap<String, Vec<i64>> = HashMap::new();
    columns.insert(String::from("L_ORDERKEY"), Vec::new());
    columns.insert(String::from("L_PARTKEY"), Vec::new());
    columns.insert(String::from("L_SUPPKEY"), Vec::new());
    columns.insert(String::from("L_LINENUMBER"), Vec::new());
    columns.insert(String::from("L_QUANTITY"), Vec::new());
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
                                columns.get_mut(col_name).unwrap().push(i64::from_str_radix(value, 10).unwrap());
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

    // domain encoding of columns
    for (name, column) in columns.iter() {
        let mut column_with_indices: Vec<(usize, i64)> = column.iter().enumerate().map(|(i, val)| (i, *val)).collect();
        column_with_indices.sort_by_key(|(_i, val)| *val);
        let mut dict = Vec::new();
        let mut current: Option<i64> = None;
        let mut dv: Vec<ValueId> = vec![0; column_with_indices.len()];
        for (i, val) in column_with_indices.iter() {
            if current == Some(*val) {
                dv[*i] = (dict.len() - 1) as ValueId;
            } else {
                dict.push(*val);
                current = Some(*val);
                dv[*i] = (dict.len() - 1) as ValueId;
            }
        }
        let avc = core::MainAttributeValueContainer::<i64> { data: dv, dict: Box::new(core::BigIntDict { entries: dict }) };
        // print first 5 "rows"
        println!("{:?}: {:?},{:?},{:?},{:?},{:?}", name, avc.lookup(0), avc.lookup(1), avc.lookup(2), avc.lookup(3), avc.lookup(4));
    }

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