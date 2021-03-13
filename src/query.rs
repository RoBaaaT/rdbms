use std::sync::Arc;

use crate::transaction::TransactionContext;
use crate::lqp::LQP;


pub struct QueryContext {
    pub transaction: Arc<TransactionContext>,
    pub lqp: LQP
}