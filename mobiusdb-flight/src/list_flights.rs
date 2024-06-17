use arrow_flight::Criteria;

pub enum FlightsKey {
    TableName(String),
}

impl From<Criteria> for FlightsKey {
    fn from(value: Criteria) -> Self {
        let s = String::from_utf8(value.expression.to_vec()).unwrap();
        FlightsKey::TableName(s)
    }
}
