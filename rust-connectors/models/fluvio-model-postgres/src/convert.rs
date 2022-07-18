use crate::{Column, DeleteBody, InsertBody, RelationBody, TruncateBody, UpdateBody};
use postgres_types::Type;
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub fn to_table_trucate(relations: &BTreeMap<u32, RelationBody>, trunk: TruncateBody) -> String {
    let table_names: Vec<String> = trunk
        .rel_ids
        .iter()
        .filter_map(|rel_id| relations.get(rel_id).map(|table| table.name.clone()))
        .collect();
    let mut sql = format!("TRUNCATE {}", table_names.join(","));
    if trunk.options == 1 {
        sql.push_str(" CASCADE");
    } else if trunk.options == 2 {
        sql.push_str(" RESTART IDENTITY");
    }
    sql
}
pub fn to_table_alter(new_table: &RelationBody, old_table: &RelationBody) -> Vec<String> {
    let mut alters: Vec<String> = Vec::new();
    if new_table.name != old_table.name {
        alters.push(format!(
            "ALTER TABLE {} RENAME TO {}",
            old_table.name, new_table.name
        ));
    }
    match new_table.columns.len().cmp(&old_table.columns.len()) {
        Ordering::Equal => {
            for (new_col, old_col) in new_table.columns.iter().zip(old_table.columns.iter()) {
                if new_col.name != old_col.name {
                    alters.push(format!(
                        "ALTER TABLE {}.{} RENAME COLUMN {} TO {}",
                        new_table.namespace, new_table.name, old_col.name, new_col.name
                    ));
                }
                if new_col.type_id != old_col.type_id {
                    let type_id = new_col.type_id as u32;
                    let column_type = if let Some(column_type) = Type::from_oid(type_id) {
                        column_type
                    } else {
                        tracing::error!("Failed to find type id: {:?}", new_col.type_id);
                        continue;
                    };
                    let column_type = column_type.name();
                    alters.push(format!(
                        "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}",
                        new_table.namespace, new_table.name, new_col.name, column_type
                    ));
                }
            }
        }
        Ordering::Greater => {
            // This is a ADD column
            let old_cols: Vec<String> = old_table.columns.iter().map(|c| c.name.clone()).collect();
            let new_columns: Vec<Column> = new_table
                .columns
                .clone()
                .into_iter()
                .filter(|col| !old_cols.contains(&col.name))
                .collect();
            for column in new_columns {
                let type_id = column.type_id as u32;
                let column_type = if let Some(column_type) = Type::from_oid(type_id) {
                    column_type
                } else {
                    tracing::error!("Failed to find type id: {:?}", column.type_id);
                    continue;
                };
                let column_type = column_type.name();
                alters.push(format!(
                    "ALTER TABLE {}.{} ADD COLUMN {} {}",
                    new_table.namespace, new_table.name, column.name, column_type
                ));
            }
        }
        Ordering::Less => {
            // This is a DROP column
            let new_cols: Vec<String> = new_table.columns.iter().map(|c| c.name.clone()).collect();
            let deleted_columns: Vec<Column> = old_table
                .columns
                .clone()
                .into_iter()
                .filter(|col| !new_cols.contains(&col.name))
                .collect();
            for i in deleted_columns {
                alters.push(format!(
                    "ALTER TABLE {}.{} DROP COLUMN {}",
                    new_table.namespace, new_table.name, i.name
                ));
            }
        }
    }
    alters
}
pub fn to_update(table: &RelationBody, update: &UpdateBody) -> String {
    let filter_tuple = match (&update.key_tuple, &update.old_tuple) {
        (Some(tuple), None) => tuple,
        (None, Some(tuple)) => tuple,
        (Some(tuple), Some(_old_tuple)) => {
            tracing::info!("Delete had both key_tuple and old_tuple {:?}", update);
            tuple
        }
        other => {
            unreachable!("This delete case was not handled {:?}", other);
        }
    };
    let mut update_vals: Vec<String> = Vec::new();
    for (column, new_data) in table.columns.iter().zip(update.new_tuple.0.iter()) {
        let val: String = if let Ok(val) = new_data.try_into() {
            val
        } else {
            tracing::error!("Uncaugh tuple type {:?}", new_data);
            continue;
        };
        update_vals.push(format!("{}={}", column.name, val));
    }

    let mut where_vals: Vec<String> = Vec::new();
    for (column, filter) in table.columns.iter().zip(filter_tuple.0.iter()) {
        let val: String = if let Ok(val) = filter.try_into() {
            val
        } else {
            tracing::error!("Uncaugh tuple type {:?}", filter);
            continue;
        };
        where_vals.push(format!("{}={}", column.name, val));
    }
    format!(
        "UPDATE {}.{} SET {} WHERE {}",
        table.namespace,
        table.name,
        update_vals.join(","),
        where_vals.join(" AND ")
    )
}
pub fn to_delete(table: &RelationBody, delete: &DeleteBody) -> String {
    let mut where_clauses: Vec<String> = Vec::new();
    let tuple = match (&delete.key_tuple, &delete.old_tuple) {
        (Some(tuple), None) => tuple,
        (None, Some(tuple)) => tuple,
        (Some(tuple), Some(_old_tuple)) => {
            tracing::info!("Delete had both key_tuple and old_tuple {:?}", delete);
            tuple
        }
        other => {
            unreachable!("This delete case was not handled {:?}", other);
        }
    };
    for (column, tuple) in table.columns.iter().zip(tuple.0.iter()) {
        let val: String = if let Ok(val) = tuple.try_into() {
            val
        } else {
            tracing::error!("Uncaugh tuple type {:?}", tuple);
            continue;
        };
        where_clauses.push(format!("{}={}", column.name, val));
    }
    format!(
        "DELETE FROM {}.{} WHERE {}",
        table.namespace,
        table.name,
        where_clauses.join(" AND ")
    )
}
pub fn to_table_insert(table: &RelationBody, insert: &InsertBody) -> String {
    let mut values: Vec<String> = Vec::new();
    let mut col_names: Vec<String> = Vec::new();
    for (column, tuple) in table.columns.iter().zip(insert.tuple.0.iter()) {
        let val: String = if let Ok(val) = tuple.try_into() {
            val
        } else {
            tracing::error!("Uncaugh tuple type {:?}", tuple);
            continue;
        };
        values.push(val);
        col_names.push(column.name.clone());
    }
    let values = values.join(",");
    let columns = col_names.join(",");
    format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        table.namespace, table.name, columns, values
    )
}

pub fn to_table_create(table: &RelationBody) -> String {
    let mut primary_keys: Vec<String> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    for column in table.columns.iter() {
        let type_id = column.type_id as u32;

        let column_type = if let Some(column_type) = Type::from_oid(type_id) {
            column_type
        } else {
            tracing::error!("Failed to find type id: {:?}", column.type_id);
            continue;
        };
        let column_type = column_type.name();
        let column_name = &column.name;
        if column.flags == 1 {
            primary_keys.push(column_name.clone());
        }
        columns.push(format!("{} {}", column_name, column_type));
    }
    if !primary_keys.is_empty() {
        columns.push(format!("PRIMARY KEY ({})", primary_keys.join(",")));
    }
    let columns = columns.join(",");
    format!(
        "CREATE TABLE {}.{} ({})",
        table.namespace, table.name, columns
    )
}
