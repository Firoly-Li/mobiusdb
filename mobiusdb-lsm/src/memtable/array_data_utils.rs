use std::{collections::HashSet, sync::Arc};
use anyhow::Result;
use arrow::{array::{make_array, Array, ArrayData, RecordBatch, RecordBatchOptions}, compute::concat ,datatypes::{Field, Schema, SchemaRef}, error::ArrowError};






pub fn merge_batches<'a>(input_batches: impl IntoIterator<Item = &'a RecordBatch> + Clone) -> Result<RecordBatch> {
    let schemas: Vec<Arc<Schema>> = input_batches.clone().into_iter().map(|batch|batch.schema()).collect();
    if let Ok(schema) = merge_schemas(schemas.clone()) {
        let resp = merge_batches_with_schema(&Arc::new(schema), input_batches)?;
        Ok(resp)
    }else {
        Err(anyhow::Error::msg("message"))
    }
}





/**
 * 合并多个数组
 * schema: 合并后的schema
 */
pub fn merge_batches_with_schema<'a>(
    schema: &SchemaRef,
    input_batches: impl IntoIterator<Item = &'a RecordBatch>,
) -> Result<RecordBatch, ArrowError> {
    let fields = schema.fields().to_vec();
    // When schema is empty, sum the number of the rows of all batches
    if schema.fields().is_empty() {
        let num_rows: usize = input_batches.into_iter().map(RecordBatch::num_rows).sum();
        let mut options = RecordBatchOptions::default();
        options.row_count = Some(num_rows);
        return RecordBatch::try_new_with_options(schema.clone(), vec![], &options);
    }

    let batches: Vec<&RecordBatch> = input_batches.into_iter().collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let ctype = fields[i].clone();
        let arr = array(&batches, ctype);
        let array = arr.iter()
    .map(|arc| arc.as_ref() as &dyn Array)
    .collect::<Vec<_>>();
        let array = concat(&array)?;
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays);
    batch
}


fn array(batches: &Vec<&RecordBatch>,ctype: Arc<Field>) -> Vec<Arc<dyn Array>> {
    let arr = batches
                .iter()
                .map(|batch| {
                    let arr3 = if let Some(index) = batch.schema().fields().iter().position(|f| f == &ctype) {
                        let arr1 = batch.column(index).to_owned();
                        arr1
                    } else {
                        let arr2 = build_null_array(&ctype,batch.num_rows());
                        arr2
                    };
                    arr3
                })
                .collect::<Vec<_>>();
    arr
}

/**
 * 构建一个长度为length的空数组 
 */
pub fn build_null_array(ctype: &Arc<Field>, length: usize)-> Arc<dyn Array>{
    let data_type = ctype.data_type();
    let array_data = ArrayData::new_null(data_type, length);
    let array = make_array(array_data);
    array
}

/**
 * 合并两个schema
 */
pub fn merge_schema(s1: &Schema,s2: &Schema) -> Schema {
    let schema1 = s1.fields().to_vec();
    let schema2 = s2.fields().to_vec();
    let mut combined_set: HashSet<_> = schema1.into_iter().collect();
    combined_set.extend(schema2.into_iter());
    let combined_schema: Vec<Arc<Field>> = combined_set.into_iter().collect();
    Schema::new(combined_schema)
}

pub fn merge_schemas(schemas: impl IntoIterator<Item = SchemaRef>) -> Result<Schema>{
    let mut hash_set = HashSet::new();
    schemas.into_iter().for_each(|schema|{
        let vecs: HashSet<_> = schema.fields().to_vec().into_iter().collect();
        hash_set.extend(vecs);
    });
    let combined_schema: Vec<Arc<Field>> = hash_set.into_iter().collect();
    Ok(Schema::new(combined_schema))
}