//! Flink's catalog model for the binder: catalogs hold databases, databases hold tables, views
//! and functions; temporary objects shadow permanent ones (`CatalogManager`).
//!
//! This module defines the [`Catalog`] trait the binder resolves against and an in-memory
//! implementation (`generic_in_memory`). Persistent catalogs (the Iceberg REST catalog `loams`,
//! JDBC, the Postgres catalog store) implement the same trait in `loams-stream-plan` (FC1 Task
//! 19, FC2), FC1 ruling R4.2.

use std::collections::BTreeMap;

use crate::types::LogicalType;

/// A fully qualified object name: `catalog.database.object`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectIdentifier {
    pub catalog: String,
    pub database: String,
    pub object: String,
}

impl std::fmt::Display for ObjectIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}`.`{}`.`{}`",
            self.catalog, self.database, self.object
        )
    }
}

/// How a column gets its value (Flink's `Column` subclasses).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnKind {
    Physical,
    /// `name AS expr`; the expression's SQL text.
    Computed(String),
    /// `name type METADATA [FROM 'key'] [VIRTUAL]`.
    Metadata {
        key: Option<String>,
        is_virtual: bool,
    },
}

/// A resolved column.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedColumn {
    pub name: String,
    pub ty: LogicalType,
    pub kind: ColumnKind,
}

/// A resolved table schema: columns, the watermark and the primary key.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResolvedSchema {
    pub columns: Vec<ResolvedColumn>,
    /// `(rowtime column, watermark expression text)`.
    pub watermark: Option<(String, String)>,
    pub primary_key: Option<Vec<String>>,
}

impl ResolvedSchema {
    pub fn column(&self, name: &str) -> Option<&ResolvedColumn> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// A table or view in a catalog.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogObject {
    Table {
        schema: ResolvedSchema,
        options: BTreeMap<String, String>,
        partition_keys: Vec<String>,
        comment: Option<String>,
    },
    View {
        schema: ResolvedSchema,
        /// The expanded query text.
        query: String,
    },
}

impl CatalogObject {
    pub fn schema(&self) -> &ResolvedSchema {
        match self {
            CatalogObject::Table { schema, .. } | CatalogObject::View { schema, .. } => schema,
        }
    }
    pub fn is_view(&self) -> bool {
        matches!(self, CatalogObject::View { .. })
    }
}

/// A catalog error worded as Flink's `CatalogException` subclasses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogError(pub String);

/// Flink's `Catalog` interface, the part the binder needs.
pub trait Catalog: Send + Sync + std::fmt::Debug {
    fn default_database(&self) -> &str;
    fn list_databases(&self) -> Vec<String>;
    fn database_exists(&self, database: &str) -> bool;
    fn create_database(
        &mut self,
        database: &str,
        ignore_if_exists: bool,
    ) -> Result<(), CatalogError>;
    fn drop_database(
        &mut self,
        database: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<(), CatalogError>;
    fn list_objects(&self, database: &str) -> Vec<String>;
    fn get_object(&self, database: &str, name: &str) -> Option<&CatalogObject>;
    fn create_object(
        &mut self,
        database: &str,
        name: &str,
        object: CatalogObject,
        ignore_if_exists: bool,
    ) -> Result<(), CatalogError>;
    fn drop_object(&mut self, database: &str, name: &str) -> bool;
    fn list_functions(&self, database: &str) -> Vec<String>;
    fn create_function(&mut self, database: &str, name: &str, class: &str);
    fn drop_function(&mut self, database: &str, name: &str) -> bool;
}

/// `GenericInMemoryCatalog`.
#[derive(Debug, Clone)]
pub struct InMemoryCatalog {
    name: String,
    default_database: String,
    databases: BTreeMap<String, Database>,
}

#[derive(Debug, Clone, Default)]
struct Database {
    objects: BTreeMap<String, CatalogObject>,
    functions: BTreeMap<String, String>,
}

impl InMemoryCatalog {
    pub fn new(name: impl Into<String>, default_database: impl Into<String>) -> Self {
        let default_database = default_database.into();
        let mut databases = BTreeMap::new();
        databases.insert(default_database.clone(), Database::default());
        InMemoryCatalog {
            name: name.into(),
            default_database,
            databases,
        }
    }
}

impl Catalog for InMemoryCatalog {
    fn default_database(&self) -> &str {
        &self.default_database
    }
    fn list_databases(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }
    fn database_exists(&self, database: &str) -> bool {
        self.databases.contains_key(database)
    }
    fn create_database(
        &mut self,
        database: &str,
        ignore_if_exists: bool,
    ) -> Result<(), CatalogError> {
        if self.databases.contains_key(database) {
            if ignore_if_exists {
                return Ok(());
            }
            return Err(CatalogError(format!(
                "Database {database} already exists in Catalog {}.",
                self.name
            )));
        }
        self.databases
            .insert(database.to_owned(), Database::default());
        Ok(())
    }
    fn drop_database(
        &mut self,
        database: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<(), CatalogError> {
        let Some(db) = self.databases.get(database) else {
            if ignore_if_not_exists {
                return Ok(());
            }
            return Err(CatalogError(format!(
                "Database {database} does not exist in Catalog {}.",
                self.name
            )));
        };
        if !cascade && (!db.objects.is_empty() || !db.functions.is_empty()) {
            return Err(CatalogError(format!(
                "Database {database} in catalog {} is not empty.",
                self.name
            )));
        }
        self.databases.remove(database);
        Ok(())
    }
    fn list_objects(&self, database: &str) -> Vec<String> {
        self.databases
            .get(database)
            .map(|d| d.objects.keys().cloned().collect())
            .unwrap_or_default()
    }
    fn get_object(&self, database: &str, name: &str) -> Option<&CatalogObject> {
        self.databases.get(database)?.objects.get(name)
    }
    fn create_object(
        &mut self,
        database: &str,
        name: &str,
        object: CatalogObject,
        ignore_if_exists: bool,
    ) -> Result<(), CatalogError> {
        let catalog = self.name.clone();
        let Some(db) = self.databases.get_mut(database) else {
            return Err(CatalogError(format!(
                "Database {database} does not exist in Catalog {catalog}."
            )));
        };
        if db.objects.contains_key(name) {
            if ignore_if_exists {
                return Ok(());
            }
            let kind = if object.is_view() { "View" } else { "Table" };
            return Err(CatalogError(format!(
                "{kind} (or view) {database}.{name} already exists in Catalog {catalog}."
            )));
        }
        db.objects.insert(name.to_owned(), object);
        Ok(())
    }
    fn drop_object(&mut self, database: &str, name: &str) -> bool {
        self.databases
            .get_mut(database)
            .is_some_and(|d| d.objects.remove(name).is_some())
    }
    fn list_functions(&self, database: &str) -> Vec<String> {
        self.databases
            .get(database)
            .map(|d| d.functions.keys().cloned().collect())
            .unwrap_or_default()
    }
    fn create_function(&mut self, database: &str, name: &str, class: &str) {
        if let Some(db) = self.databases.get_mut(database) {
            db.functions.insert(name.to_owned(), class.to_owned());
        }
    }
    fn drop_function(&mut self, database: &str, name: &str) -> bool {
        self.databases
            .get_mut(database)
            .is_some_and(|d| d.functions.remove(name).is_some())
    }
}
