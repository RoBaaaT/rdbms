use std::sync::RwLock;
use super::avc::AttributeValueContainer;

pub struct Database<'a> {
    pub avc: RwLock<Box<dyn AttributeValueContainer<i64> + 'a + Send + Sync>>
}