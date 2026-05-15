use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderEvent {
    pub order_id: String,
    pub user_id: String,
    pub amount: f64,
    pub status: String,
}

impl OrderEvent {
    pub fn new(
        order_id: impl Into<String>,
        user_id: impl Into<String>,
        amount: f64,
        status: impl Into<String>,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            user_id: user_id.into(),
            amount,
            status: status.into(),
        }
    }

    pub fn sample(index: u32) -> Self {
        Self {
            order_id: format!("order-{}", 100 + index),
            user_id: format!("user-{}", (index % 3) + 1),
            amount: 10.0 + (index as f64 * 7.5),
            status: "created".to_string(),
        }
    }
}
