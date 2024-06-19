
//
// Schema { 
//     fields: [
//         Field { name: "item_name", data_type: String, nullable: false, metadata: {} }, 
//         Field { name: "item_vt", data_type: String, nullable: false,metadata: {} }, 
//         Field { name: "item_value", data_type: String, nullable: false, metadata: {} }
//         Field { name: "time", data_type: UInt64, nullable: false, metadata: {} }
//     ], 
//     metadata: {
//         db: db1,
//         table: table_name (这组数据的起始时间)
//     } 
// }
//
//

use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::Schema};
use serde::Serialize;





#[derive(Debug, Clone,Serialize)]
pub enum VT {
    Int,String,Bool,Float,Time,Long
}

#[derive(Debug, Clone,Serialize)]
pub struct Item {
    name: String,
    vt: VT,
    value: String,
    time: u64,
}

impl Item {
    fn new(name: String,
        vt: VT,
        value: String,
        time: u64) -> Self {
        Self {
            name,vt,value,time
        }
    }
}

#[derive(Debug)]
pub struct UniversalBatch {
    schema: Arc<Schema>,
    items: Vec<Item>,
}



#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{datatypes::*, json::ReaderBuilder};

    use crate::common::universal_batch::{Item, VT};


    
    fn general_schema() -> Schema {
        let schema = Schema::new(
            vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("vt", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, false),
                        Field::new("time", DataType::UInt64, false),
                    ]
        );
        println!("{:?}", schema);
        schema
    }


    #[test]
    fn build_batch() {
        let schema = general_schema();
        let vs = vec![
            Item::new("name".to_string(), VT::String, "value".to_string(), 1655374633672765),
            Item::new("name1".to_string(), VT::String, "value_1".to_string(), 1655374633673765),
            Item::new("name2".to_string(), VT::String, "value_2".to_string(), 1655374633674765),
        ];

        let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
        decoder.serialize(&vs).unwrap();

        let batch = decoder.flush().unwrap().unwrap();
        println!("batch: {:?}", batch);

        
    }
}

