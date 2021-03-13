

pub struct TransactionManager {

}

pub struct TransactionContext {
    // TODO: this will have MVCC data for an open transaction
}

impl TransactionManager {
    pub fn new_transaction_context(&mut self) -> TransactionContext {
        TransactionContext {}
    }
}