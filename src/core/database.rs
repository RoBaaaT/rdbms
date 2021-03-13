use std::sync::{RwLock, Mutex};
use crate::transaction::TransactionManager;
use super::avc::AttributeValueContainer;

pub struct Database<'a> {
    pub transaction_manager: Mutex<TransactionManager>,
    pub avc: RwLock<Box<dyn AttributeValueContainer<i64> + 'a + Send + Sync>>
}