use arrow_flight::FlightInfo;

use crate::list_flights::FlightsKey;

/**
 * 状态的定义
 */
#[allow(async_fn_in_trait)]
pub trait State {
    fn flight_list(&self, key: &FlightsKey) -> Vec<FlightInfo>;
}
