use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::CatalogBuilder;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{SqlCatalog, SqlCatalogBuilder};
use pgiceberg_common::config::IcebergWriterConfig;
use pgiceberg_common::metadata::MetadataStore;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Initialize the SQLite-backed Iceberg catalog.
pub async fn init_catalog(config: &IcebergWriterConfig) -> anyhow::Result<SqlCatalog> {
    std::fs::create_dir_all(&config.warehouse_path)?;

    let catalog = SqlCatalogBuilder::default()
        .uri(format!("sqlite://{}", config.catalog_db_path.display()))
        .warehouse_location(format!("file://{}", config.warehouse_path.display()))
        .load("pgiceberg", HashMap::new())
        .await?;

    info!(
        warehouse = %config.warehouse_path.display(),
        "Iceberg catalog initialized"
    );
    Ok(catalog)
}

/// Ensure an Iceberg table exists for a source Postgres table.
///
/// Column metadata is re-discovered from the source Postgres on every startup,
/// making this service stateless.
pub async fn ensure_iceberg_table(
    catalog: &SqlCatalog,
    metadata: &MetadataStore,
    schema_name: &str,
    table_name: &str,
    pk_columns: &[String],
) -> anyhow::Result<Table> {
    let namespace = NamespaceIdent::new(schema_name.to_string());

    if catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .is_ok()
    {
        info!(namespace = schema_name, "Created Iceberg namespace");
    }

    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    if catalog.table_exists(&table_ident).await? {
        return catalog.load_table(&table_ident).await.map_err(Into::into);
    }

    let columns = metadata.discover_columns(schema_name, table_name).await?;

    let mut fields = Vec::new();
    let mut pk_field_ids = Vec::new();
    let mut field_id = 1i32;

    for (col_name, udt_name, nullable) in &columns {
        let iceberg_type = pg_udt_to_iceberg_type(udt_name);
        let required = !nullable || pk_columns.contains(col_name);

        let field = if required {
            Arc::new(NestedField::required(field_id, col_name, iceberg_type))
        } else {
            Arc::new(NestedField::optional(field_id, col_name, iceberg_type))
        };

        if pk_columns.contains(col_name) {
            pk_field_ids.push(field_id);
        }

        fields.push(field);
        field_id += 1;
    }

    let iceberg_schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(pk_field_ids)
        .build()?;

    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .build();

    let table = catalog.create_table(&namespace, creation).await?;
    info!(
        schema = schema_name,
        table = table_name,
        pk = ?pk_columns,
        "Created Iceberg table"
    );

    Ok(table)
}

fn pg_udt_to_iceberg_type(udt_name: &str) -> Type {
    match udt_name {
        "bool" => Type::Primitive(PrimitiveType::Boolean),
        "int2" => Type::Primitive(PrimitiveType::Int),
        "int4" => Type::Primitive(PrimitiveType::Int),
        "int8" => Type::Primitive(PrimitiveType::Long),
        "float4" => Type::Primitive(PrimitiveType::Float),
        "float8" => Type::Primitive(PrimitiveType::Double),
        "numeric" => Type::Primitive(PrimitiveType::Decimal {
            precision: 38,
            scale: 18,
        }),
        "text" | "varchar" | "bpchar" | "name" => Type::Primitive(PrimitiveType::String),
        "bytea" => Type::Primitive(PrimitiveType::Binary),
        "date" => Type::Primitive(PrimitiveType::Date),
        "timestamp" => Type::Primitive(PrimitiveType::Timestamp),
        "timestamptz" => Type::Primitive(PrimitiveType::Timestamptz),
        "time" | "timetz" => Type::Primitive(PrimitiveType::Time),
        "uuid" => Type::Primitive(PrimitiveType::Uuid),
        "json" | "jsonb" => Type::Primitive(PrimitiveType::String),
        "inet" | "cidr" | "macaddr" => Type::Primitive(PrimitiveType::String),
        _ if udt_name.starts_with('_') => {
            let inner = &udt_name[1..];
            let inner_type = pg_udt_to_iceberg_type(inner);
            Type::List(iceberg::spec::ListType {
                element_field: Arc::new(NestedField::optional(0, "element", inner_type)),
            })
        }
        _ => Type::Primitive(PrimitiveType::String),
    }
}
