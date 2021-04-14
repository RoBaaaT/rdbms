use std::io;
use std::io::prelude::*;
use std::fmt;
use std::fs::File;
use std::net::TcpStream;
use std::convert::TryInto;
use std::collections::HashMap;
use std::str;
use std::sync::Arc;
use std::sync::RwLock;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

use crate::core::Database;
use crate::lqp::{LQP, LQPError};

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
    if let Some(enc) = parameters.get("client_encoding") {
        let mut msg = Vec::new();
        msg.extend("client_encoding\0".as_bytes());
        msg.extend(enc.as_bytes());
        msg.push(0);
        send_protocol_message(&mut stream, 'S', msg.as_slice()).unwrap();
    }
    // send BackendKeyData
    send_protocol_message(&mut stream, 'K', "abcdefgh".as_bytes()).unwrap();
    // send ReadyForQuery
    send_protocol_message(&mut stream, 'Z', &['I' as u8]).unwrap();

    // this is set to true if an error was encountered while processing the extended query flow (parse/bind/describe/execute/sync)
    //  if set to true, incoming messages are discarded until the next sync message is encountered
    let mut error_state = false;
    loop {
        let mut type_buffer = [0; 1];
        stream.read_exact(&mut type_buffer).unwrap();
        stream.read_exact(&mut len_buffer).unwrap();
        message_len = u32::from_be_bytes(len_buffer) as usize - 4;
        let mut message_content = vec![0; message_len];
        stream.read_exact(message_content.as_mut_slice()).unwrap();
        let message_type = type_buffer[0] as char;
        match message_type {
            'P' => { // parse
                if error_state {
                    continue;
                }
                let (prepared_statement, ps_bytes) = read_string(&message_content).unwrap();
                if prepared_statement.len() > 0 {
                    // TODO: prepared statement support
                    send_error_response(&mut stream, ProtocolError::with_detail(ErrorSeverity::Error, String::from("42000"), String::from("Unsupported"), String::from("Named prepared statements are not yet supported"))).unwrap();
                    error_state = true;
                    continue;
                }
                let (query_string, q_bytes) = read_string(&message_content[ps_bytes..message_len]).unwrap();
                // parameter data types
                let offset = ps_bytes + q_bytes;
                let pdt_count = u16::from_be_bytes(message_content[offset..offset + 2].try_into().unwrap());
                println!("P: {}, Q: {}", prepared_statement, query_string);
                if pdt_count > 0 {
                    // TODO: parameter support
                    send_error_response(&mut stream, ProtocolError::with_detail(ErrorSeverity::Error, String::from("42000"), String::from("Unsupported"), String::from("Parameters are not yet supported"))).unwrap();
                    error_state = true;
                    continue;
                }
                // TODO: parse and store as prepared statement
                // ParseComplete
                send_protocol_message(&mut stream, '1', &[]).unwrap();
            },
            'B' => { // bind
                if error_state {
                    continue;
                }
                // TODO: handle message contents
                // BindComplete
                send_protocol_message(&mut stream, '2', &[]).unwrap();
            },
            'D' => { // describe
                if error_state {
                    continue;
                }
                // TODO: respond with a proper ParameterDescription message
                // ParameterDescription (0 parameters)
                send_protocol_message(&mut stream, 't', &(0 as u16).to_be_bytes()).unwrap();
            },
            'E' => { // execute
                if error_state {
                    continue;
                }
                let (prepared_statement, ps_bytes) = read_string(&message_content).unwrap();
                let max_rows = u32::from_be_bytes(message_content[ps_bytes..ps_bytes + 4].try_into().unwrap()) as usize;
                println!("Execute: '{}' (max {} rows)", prepared_statement, max_rows);
                // TODO: handle execute
            },
            'S' => { // sync
                // TODO: handle transaction commit/abort
                error_state = false;
                // ReadyForQuery
                send_protocol_message(&mut stream, 'Z', &['I' as u8]).unwrap();

            },
            'Q' => {
                let db = db.read().unwrap();
                // for now just use a new TransactionContext for each incoming query message
                // TODO: proper handling of BEGIN/COMMIT/ROLLBACK/ABORT
                let _transaction_context = db.transaction_manager.lock().unwrap().new_transaction_context();

                // get the query string
                let (query_string, _) = read_string(&message_content).unwrap();
                let dialect = GenericDialect {};
                match Parser::parse_sql(&dialect, query_string) {
                    Ok(statements) => {
                        for statement in statements {
                            let lqp = LQP::from(&statement);
                            println!("Parsed SQL: {:?}", statement);
                            match lqp {
                                Ok(lqp) => {
                                    println!("LQP: {:?}", lqp);
                                    // TEMPORARY: write the LQP to file as a dot graph
                                    let mut file = File::create("lqp.dot").unwrap();
                                    file.write_all(lqp.get_dot_graph().as_bytes()).unwrap();

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
                                },
                                Err(err) => {
                                    println!("LQP creation error: {:?}", err);
                                    send_error_response(&mut stream, ProtocolError::from(err)).unwrap();
                                }
                            }
                        }
                    }
                    Err(err) => {
                        println!("Syntax error: {:?}", err);
                        send_error_response(&mut stream, ProtocolError::from(err)).unwrap();
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

#[allow(dead_code)]
enum ErrorSeverity {
    Error,
    Fatal,
    Panic
}

impl fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorSeverity::Error =>  write!(f, "ERROR"),
            ErrorSeverity::Fatal =>  write!(f, "FATAL"),
            ErrorSeverity::Panic =>  write!(f, "PANIC")
        }
    }
}

struct ProtocolError {
    severity: ErrorSeverity,
    sqlstate: String,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    position: Option<usize>,
    internal_position: Option<usize>,
    internal_query: Option<String>,
    r#where: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
    data_type: Option<String>,
    constraint: Option<String>,
    file: Option<String>,
    line: Option<String>,
    routine: Option<String>
}

impl ProtocolError {
    fn with_detail(severity: ErrorSeverity, sqlstate: String, message: String, detail: String) -> Self {
        ProtocolError {
            severity,
            sqlstate,
            message,
            detail: Some(detail),
            hint: None,
            position: None,
            internal_position: None,
            internal_query: None,
            r#where: None,
            schema: None,
            table: None,
            column: None,
            data_type: None,
            constraint: None,
            file: None,
            line: None,
            routine: None
        }
    }
}

impl From<ParserError> for ProtocolError {
    fn from(err: ParserError) -> Self {
        let message = match err {
            ParserError::TokenizerError(message) => message,
            ParserError::ParserError(message) => message
        };
        ProtocolError::with_detail(ErrorSeverity::Error, String::from("42601"), String::from("Syntax error"), message)
    }
}

impl From<LQPError> for ProtocolError {
    fn from(err: LQPError) -> Self {
        ProtocolError::with_detail(ErrorSeverity::Error, String::from("42000"), String::from("LQP error"), err.to_string())
    }
}

// Err(true) indicates UTF-8 error, Err(false) indicates no string was found in buf
fn read_string(buf: &[u8]) -> Result<(&str, usize), bool> {
    let mut len = None;
    for i in 0..buf.len() {
        if buf[i] == 0 {
            len = Some(i);
            break;
        }
    }
    if let Some(len) = len {
        match str::from_utf8(&buf[0..len]) {
            Ok(result) => Ok((result, len + 1)),
            Err(_) => Err(true),
        }
    } else {
        Err(false)
    }
}

fn send_error_response(stream: &mut TcpStream, err: ProtocolError) -> io::Result<usize> {
    let mut buf = Vec::<u8>::new();
    buf.push('S' as u8);
    buf.extend_from_slice(err.severity.to_string().as_bytes());
    buf.push(0);
    buf.push('V' as u8);
    buf.extend_from_slice(err.severity.to_string().as_bytes());
    buf.push(0);
    buf.push('C' as u8);
    buf.extend_from_slice(err.sqlstate.as_bytes());
    buf.push(0);
    buf.push('M' as u8);
    buf.extend_from_slice(err.message.as_bytes());
    buf.push(0);
    if let Some(detail) = err.detail {
        buf.push('D' as u8);
        buf.extend_from_slice(detail.as_bytes());
        buf.push(0);
    }
    if let Some(hint) = err.hint {
        buf.push('H' as u8);
        buf.extend_from_slice(hint.as_bytes());
        buf.push(0);
    }
    if let Some(position) = err.position {
        buf.push('P' as u8);
        buf.extend_from_slice(position.to_string().as_bytes());
        buf.push(0);
    }
    if let Some(internal_position) = err.internal_position {
        buf.push('p' as u8);
        buf.extend_from_slice(internal_position.to_string().as_bytes());
        buf.push(0);
    }
    if let Some(internal_query) = err.internal_query {
        buf.push('D' as u8);
        buf.extend_from_slice(internal_query.as_bytes());
        buf.push(0);
    }
    if let Some(r#where) = err.r#where {
        buf.push('W' as u8);
        buf.extend_from_slice(r#where.as_bytes());
        buf.push(0);
    }
    if let Some(schema) = err.schema {
        buf.push('s' as u8);
        buf.extend_from_slice(schema.as_bytes());
        buf.push(0);
    }
    if let Some(table) = err.table {
        buf.push('t' as u8);
        buf.extend_from_slice(table.as_bytes());
        buf.push(0);
    }
    if let Some(column) = err.column {
        buf.push('c' as u8);
        buf.extend_from_slice(column.as_bytes());
        buf.push(0);
    }
    if let Some(data_type) = err.data_type {
        buf.push('d' as u8);
        buf.extend_from_slice(data_type.as_bytes());
        buf.push(0);
    }
    if let Some(constraint) = err.constraint {
        buf.push('n' as u8);
        buf.extend_from_slice(constraint.as_bytes());
        buf.push(0);
    }
    if let Some(file) = err.file {
        buf.push('F' as u8);
        buf.extend_from_slice(file.as_bytes());
        buf.push(0);
    }
    if let Some(line) = err.line {
        buf.push('L' as u8);
        buf.extend_from_slice(line.as_bytes());
        buf.push(0);
    }
    if let Some(routine) = err.routine {
        buf.push('R' as u8);
        buf.extend_from_slice(routine.as_bytes());
        buf.push(0);
    }
    buf.push(0);
    return send_protocol_message(stream, 'E', &buf)
}