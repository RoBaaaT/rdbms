use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use std::convert::TryInto;
use std::collections::HashMap;
use std::str;
use std::sync::Arc;
use std::sync::RwLock;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::core::Database;
use crate::lqp::LQP;

pub fn handle_connection(mut stream: TcpStream, db: Arc<RwLock<Database>>) {
    let mut parameters = HashMap::new();
    let mut buffer = [0; 1024];
    let mut len_buffer = [0; 4];

    stream.read_exact(&mut len_buffer).unwrap();
    let mut message_len = u32::from_be_bytes(len_buffer) as usize;
    stream.read(&mut buffer).unwrap();
    let mut protocol_major_version: u16 = u16::from_be_bytes(buffer[0..2].try_into().unwrap());
    let mut protocol_minor_version: u16 = u16::from_be_bytes(buffer[2..4].try_into().unwrap());

    if protocol_major_version == 1234 {
        match protocol_minor_version {
            5678 => { // CancelRequest
                // TODO: handle the request
                return // close the connection
            }
            5679 => { // SSLRequest
                // we do not support connection encryption yet
                stream.write(&['N' as u8]).unwrap();

                stream.read_exact(&mut len_buffer).unwrap();
                message_len = u32::from_be_bytes(len_buffer) as usize - 4;
                stream.read_exact(&mut buffer[0..message_len]).unwrap();
                protocol_major_version = u16::from_be_bytes(buffer[0..2].try_into().unwrap());
                protocol_minor_version = u16::from_be_bytes(buffer[2..4].try_into().unwrap());
            }
            5680 => { // GSSENCRequest
                // we do not support connection encryption yet
                stream.write(&['N' as u8]).unwrap();

                stream.read_exact(&mut len_buffer).unwrap();
                message_len = u32::from_be_bytes(len_buffer) as usize - 4;
                stream.read_exact(&mut buffer[0..message_len]).unwrap();
                protocol_major_version = u16::from_be_bytes(buffer[0..2].try_into().unwrap());
                protocol_minor_version = u16::from_be_bytes(buffer[2..4].try_into().unwrap());
            }
            _ => {
                // TODO: handle this by sending an error response
            }
        };
    }

    // TODO: check protocol version

    let mut read_offset: usize = 4;
    let mut tmp_key = None;
    while read_offset < message_len && buffer[read_offset] != 0 {
        for i in read_offset..message_len {
            if buffer[i] == 0 {
                let val = String::from_utf8(buffer[read_offset..i].to_vec()).unwrap();
                match tmp_key {
                    None => tmp_key = Some(val),
                    Some(key) => {
                        parameters.insert(key, val);
                        tmp_key = None;
                    }
                }
                read_offset = i + 1;
                break;
            }
        }
    }
    println!("Connection with version {}.{}", protocol_major_version, protocol_minor_version);
    println!("Parameters: {:?}", parameters);

    // request password
    //stream.write(&['R' as u8, 0, 0, 0, 8, 0, 0, 0, 3]).unwrap();

    // send AuthenticationOk
    send_protocol_message(&mut stream, 'R', &(0 as u32).to_be_bytes()).unwrap();
    // send ParameterStatus
    send_protocol_message(&mut stream, 'S', "client_encoding\0WIN1252\0".as_bytes()).unwrap();
    // send BackendKeyData
    send_protocol_message(&mut stream, 'K', "abcdefgh".as_bytes()).unwrap();
    // send ReadyForQuery
    send_protocol_message(&mut stream, 'Z', &['I' as u8]).unwrap();

    loop {
        let mut type_buffer = [0; 1];
        stream.read_exact(&mut type_buffer).unwrap();
        stream.read_exact(&mut len_buffer).unwrap();
        message_len = u32::from_be_bytes(len_buffer) as usize - 4;
        let mut message_content = vec![0; message_len];
        stream.read_exact(message_content.as_mut_slice()).unwrap();
        let message_type = type_buffer[0] as char;
        match message_type {
            'Q' => {
                let db = db.read().unwrap();
                // for now just use a new TransactionContext for each incoming query message
                // TODO: proper handling of BEGIN/COMMIT/ROLLBACK/ABORT
                let transaction_context = db.transaction_manager.lock().unwrap().new_transaction_context();

                // get the query string
                let query_string = match str::from_utf8(&message_content[0..message_len - 1]) {
                    Ok(v) => v,
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                };
                let dialect = GenericDialect {};
                match Parser::parse_sql(&dialect, query_string) {
                    Ok(statements) => {
                        for statement in statements {
                            let lqp = LQP::from(&statement);
                            println!("Parsed SQL: {:?}", statement);
                            println!("LQP: {:?}", lqp);
                            // RowDescription
                            //                                   OID         ANUM  TYPE_OID    TYPLENTYPMOD      FORMAT_CODE
                            let row_desc_buf = [0, 2, 'i' as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0, 'v' as u8, 'a' as u8, 'l' as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0];
                            send_protocol_message(&mut stream, 'T', &row_desc_buf).unwrap();
                            // read some dummy data from db
                            let avc = db.avc.read().unwrap();
                            for i in 0..avc.len() {
                                // DataRow
                                let mut data_row_buf = Vec::<u8>::new();
                                data_row_buf.push(0);
                                data_row_buf.push(2);
                                // value 1
                                let val_str = i.to_string();
                                let val_str_b = val_str.as_bytes();
                                data_row_buf.extend_from_slice(&(val_str_b.len() as u32).to_be_bytes());
                                data_row_buf.extend_from_slice(val_str_b);
                                // value 2
                                match avc.lookup(i) {
                                    None => data_row_buf.extend_from_slice(&(-1 as i32).to_be_bytes()),
                                    Some(val) => {
                                        let val_str = val.to_string();
                                        let val_str_b = val_str.as_bytes();
                                        data_row_buf.extend_from_slice(&(val_str_b.len() as u32).to_be_bytes());
                                        data_row_buf.extend_from_slice(val_str_b);
                                    }
                                }
                                send_protocol_message(&mut stream, 'D', &data_row_buf).unwrap();
                            }
                            // CommandComplete
                            send_protocol_message(&mut stream, 'C', "SELECT\0".as_bytes()).unwrap();
                        }
                    }
                    Err(err) => {
                        println!("Invalid query: {:?}", err);
                        // TODO: send error message
                    }
                }
                // ReadyForQuery
                send_protocol_message(&mut stream, 'Z', &['I' as u8]).unwrap();
            },
            'X' => break,
            _ => {
                println!("Unhandled message: {}", message_type);
                continue;
            }
        };
    }
    println!("Client disconnected");
}

fn send_protocol_message(stream: &mut TcpStream, message_type: char, buf: &[u8]) -> io::Result<usize> {
    let message_len = buf.len() + 4;
    if message_len > u32::MAX as usize {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let mut result = stream.write(&[message_type as u8])?;
    result += stream.write(&(message_len as u32).to_be_bytes())?;
    result += stream.write(buf)?;
    return Ok(result);
}
