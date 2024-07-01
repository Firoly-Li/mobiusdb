use anyhow::Result;
use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
};
use std::{collections::HashSet, sync::Arc};

/**
 * 结构完全相同的两个RecordBatch合并
 */
pub fn merge_full_batch(batch1: RecordBatch, batch2: RecordBatch) -> Result<RecordBatch> {
    let schema = merge_schema(&batch1.schema(), &batch2.schema());
    // 合并每一列的数据
    let mut columns = Vec::with_capacity(batch1.num_columns());
    for i in 0..batch1.num_columns() {
        let column1 = batch1.column(i);
        let column2 = batch2.column(i);
        let c = merge_arrays(column1, column2).unwrap();
        columns.push(c);
    }
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(batch)
}

fn merge_schema(s1: &Schema, s2: &Schema) -> Schema {
    if s1 == s2 {
        return s1.clone();
    }
    let schema1 = s1.fields().to_vec();
    let schema2 = s2.fields().to_vec();
    let mut combined_set: HashSet<_> = schema1.into_iter().collect();
    combined_set.extend(schema2.into_iter());
    let combined_schema: Vec<Arc<Field>> = combined_set.into_iter().collect();
    Schema::new(combined_schema)
}

fn merge_arrays(array1: &ArrayRef, array2: &ArrayRef) -> Result<ArrayRef> {
    match (array1.data_type(), array2.data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let array1 = array1.as_any().downcast_ref::<StringArray>().unwrap();
            let array2 = array2.as_any().downcast_ref::<StringArray>().unwrap();
            let merged = arrow::compute::concat(&[array1, array2])?;
            Ok(Arc::new(merged) as ArrayRef)
        }
        // 其他数据类型的合并逻辑
        _ => Err(arrow::error::ArrowError::InvalidArgumentError(
            "Unsupported data type for merging arrays.".to_string(),
        )
        .into()),
    }
}
