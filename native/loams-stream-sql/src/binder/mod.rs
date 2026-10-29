//! The binder: resolves names against Flink's catalog model and types statements with Flink's
//! rules (FC1 Task 4; design 82 §7.1 step 2).
//!
//! A [`Binder`] is one SQL session: the current catalog and database, temporary objects and
//! functions, and the session configuration (`SET`). [`Binder::bind`] takes one parsed
//! [`Statement`], checks it and applies its catalog effects, and returns a [`Bound`] summary
//! (for queries, the output schema). It fails with a [`BindError`]: either a validation error
//! worded as Flink's, or a named [`Feature`] Loams Stream does not support yet (D2268).
//!
//! The binder is deliberately permissive where Flink's coercion rules are wide (comparisons
//! between numeric and character types, for example): rejecting a statement Flink accepts is
//! worse than accepting one it rejects, because the oracle catches the latter (§15).

mod catalog;
mod expr;
mod functions;

use std::collections::BTreeMap;

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Spanned};
use sqlparser::tokenizer::Span;

pub use catalog::{
    Catalog, CatalogError, CatalogObject, ColumnKind, InMemoryCatalog, ObjectIdentifier,
    ResolvedColumn, ResolvedSchema,
};
pub use expr::Scope;

use crate::ast::*;
use crate::error::Feature;
use crate::types::LogicalType;

/// A binding failure.
#[derive(Debug, Clone, PartialEq)]
pub struct BindError {
    pub message: String,
    /// Set when the statement is valid Flink SQL that Loams Stream does not support yet.
    pub feature: Option<Feature>,
    pub span: Span,
}

impl BindError {
    pub fn validation(message: impl Into<String>, span: Span) -> Self {
        BindError {
            message: message.into(),
            feature: None,
            span,
        }
    }
    pub fn unsupported(feature: Feature, span: Span) -> Self {
        BindError {
            message: format!("{} is not supported by Loams Stream yet", feature.name),
            feature: Some(feature),
            span,
        }
    }
}

impl std::fmt::Display for BindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BindError {}

pub type BindResult<T> = Result<T, BindError>;

/// A named output column.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputColumn {
    pub name: String,
    pub ty: LogicalType,
}

/// What a bound statement is.
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    /// A query with its output schema.
    Query(Vec<OutputColumn>),
    /// `INSERT` (or a statement set of them) into the named tables.
    Insert(Vec<ObjectIdentifier>),
    /// A catalog change (DDL).
    Ddl,
    /// A session change (`SET`, `RESET`, `USE`).
    Session,
    /// A statement answered from the catalog (`SHOW`, `DESCRIBE`), or a client command.
    Info,
    /// `EXPLAIN` of a bound statement.
    Explain(Box<Bound>),
}

/// A function registered with `CREATE FUNCTION`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserFunction {
    pub class: String,
    pub language: Option<FunctionLanguage>,
}

/// One SQL session's binding state.
#[derive(Debug)]
pub struct Binder {
    catalogs: BTreeMap<String, Box<dyn Catalog>>,
    current_catalog: String,
    current_database: String,
    temporary_objects: BTreeMap<ObjectIdentifier, CatalogObject>,
    temporary_functions: BTreeMap<ObjectIdentifier, UserFunction>,
    system_functions: BTreeMap<String, UserFunction>,
    config: BTreeMap<String, String>,
    in_statement_set: bool,
    /// Catalogs created with a type Loams Stream has no factory for: using them names the feature.
    unsupported_catalogs: BTreeMap<String, String>,
}

impl Default for Binder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn ident_value(ident: &Ident) -> String {
    ident.value.clone()
}

pub(crate) fn name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .collect()
}

impl Binder {
    /// A session with Flink's defaults: `default_catalog.default_database`.
    pub fn new() -> Self {
        let mut catalogs: BTreeMap<String, Box<dyn Catalog>> = BTreeMap::new();
        catalogs.insert(
            "default_catalog".into(),
            Box::new(InMemoryCatalog::new("default_catalog", "default_database")),
        );
        Binder {
            catalogs,
            current_catalog: "default_catalog".into(),
            current_database: "default_database".into(),
            temporary_objects: BTreeMap::new(),
            temporary_functions: BTreeMap::new(),
            system_functions: BTreeMap::new(),
            config: BTreeMap::new(),
            in_statement_set: false,
            unsupported_catalogs: BTreeMap::new(),
        }
    }

    pub fn current_catalog(&self) -> &str {
        &self.current_catalog
    }
    pub fn current_database(&self) -> &str {
        &self.current_database
    }
    pub fn config(&self) -> &BTreeMap<String, String> {
        &self.config
    }

    /// Registers a catalog (as `CREATE CATALOG` does).
    pub fn register_catalog(&mut self, name: impl Into<String>, catalog: Box<dyn Catalog>) {
        self.catalogs.insert(name.into(), catalog);
    }

    /// Qualifies `name` with the current catalog and database (`CatalogManager.qualifyIdentifier`).
    pub fn qualify(&self, name: &ObjectName) -> BindResult<ObjectIdentifier> {
        let parts = name_parts(name);
        let span = name.span();
        match parts.as_slice() {
            [o] => Ok(ObjectIdentifier {
                catalog: self.current_catalog.clone(),
                database: self.current_database.clone(),
                object: o.clone(),
            }),
            [d, o] => Ok(ObjectIdentifier {
                catalog: self.current_catalog.clone(),
                database: d.clone(),
                object: o.clone(),
            }),
            [c, d, o] => Ok(ObjectIdentifier {
                catalog: c.clone(),
                database: d.clone(),
                object: o.clone(),
            }),
            _ => Err(BindError::validation(
                format!("Object identifier can have at most 3 parts: {name}"),
                span,
            )),
        }
    }

    /// Looks a table or view up: temporary objects first, then the catalog.
    pub fn lookup(&self, id: &ObjectIdentifier) -> Option<&CatalogObject> {
        if let Some(o) = self.temporary_objects.get(id) {
            return Some(o);
        }
        self.catalogs
            .get(&id.catalog)?
            .get_object(&id.database, &id.object)
    }

    /// Looks a user function up by name: temporary system, temporary catalog, then catalog.
    pub fn user_function(&self, name: &ObjectName) -> Option<UserFunction> {
        let parts = name_parts(name);
        if parts.len() == 1
            && let Some(f) = self.system_functions.get(&parts[0].to_lowercase())
        {
            return Some(f.clone());
        }
        let id = self.qualify(name).ok()?;
        if let Some(f) = self.temporary_functions.get(&id) {
            return Some(f.clone());
        }
        let catalog = self.catalogs.get(&id.catalog)?;
        catalog
            .list_functions(&id.database)
            .iter()
            .any(|f| f.eq_ignore_ascii_case(&id.object))
            .then(|| UserFunction {
                class: String::new(),
                language: None,
            })
    }

    fn catalog_mut(&mut self, name: &str, span: Span) -> BindResult<&mut Box<dyn Catalog>> {
        self.catalogs
            .get_mut(name)
            .ok_or_else(|| BindError::validation(format!("Catalog {name} does not exist."), span))
    }

    fn not_found(id: &ObjectIdentifier, span: Span) -> BindError {
        BindError::validation(
            format!(
                "Object '{}' not found within '{}.{}'",
                id.object, id.catalog, id.database
            ),
            span,
        )
    }

    /// Binds one statement and applies its effects to the session.
    pub fn bind(&mut self, statement: &Statement) -> BindResult<Bound> {
        match statement {
            Statement::Query(q) => Ok(Bound::Query(expr::bind_query(self, &q.query)?)),
            Statement::Insert(insert) => self.bind_insert(insert).map(|id| Bound::Insert(vec![id])),
            Statement::StatementSet(set) => {
                let mut ids = vec![];
                for insert in &set.inserts {
                    ids.push(self.bind_insert(insert)?);
                }
                Ok(Bound::Insert(ids))
            }
            Statement::BeginStatementSet(_) => {
                self.in_statement_set = true;
                Ok(Bound::Session)
            }
            Statement::EndStatementSet(_) => {
                self.in_statement_set = false;
                Ok(Bound::Session)
            }
            Statement::Execute(e) => self.bind(&e.statement),
            Statement::Explain(e) => {
                Ok(Bound::Explain(Box::new(self.bind_explained(&e.statement)?)))
            }
            Statement::CreateTable(t) => self.bind_create_table(t).map(|_| Bound::Ddl),
            Statement::CreateView(v) => self.bind_create_view(v).map(|_| Bound::Ddl),
            Statement::CreateDatabase(d) => self.bind_create_database(d).map(|_| Bound::Ddl),
            Statement::CreateCatalog(c) => self.bind_create_catalog(c).map(|_| Bound::Ddl),
            Statement::CreateFunction(f) => self.bind_create_function(f).map(|_| Bound::Ddl),
            Statement::Drop(d) => self.bind_drop(d).map(|_| Bound::Ddl),
            Statement::AlterTable(a) => self.bind_alter_table(a).map(|_| Bound::Ddl),
            Statement::AlterView(a) => {
                let id = self.qualify(&a.name)?;
                if self.lookup(&id).is_none() {
                    return Err(Self::not_found(&id, a.span));
                }
                Ok(Bound::Ddl)
            }
            Statement::AlterDatabase(_)
            | Statement::AlterCatalog(_)
            | Statement::AlterFunction(_) => Ok(Bound::Ddl),
            Statement::Set(s) => {
                if let Some((k, v)) = &s.assignment {
                    if k.value == "table.sql-dialect" && v.value.eq_ignore_ascii_case("hive") {
                        return Err(BindError::unsupported(Feature::HIVE_DIALECT, s.span));
                    }
                    self.config.insert(k.value.clone(), v.value.clone());
                }
                Ok(Bound::Session)
            }
            Statement::Reset(r) => {
                match &r.key {
                    Some(k) => {
                        self.config.remove(&k.value);
                    }
                    None => self.config.clear(),
                }
                Ok(Bound::Session)
            }
            Statement::Use(u) => self.bind_use(u).map(|_| Bound::Session),
            Statement::Show(s) => self.bind_show(s).map(|_| Bound::Info),
            Statement::Describe(d) => self.bind_describe(d).map(|_| Bound::Info),
            Statement::LoadModule(m) => Err(BindError::unsupported(Feature::MODULES, m.span)),
            Statement::UnloadModule(m) => Err(BindError::unsupported(Feature::MODULES, m.span)),
            Statement::Call(c) => Err(BindError::unsupported(Feature::PROCEDURES, c.span)),
            Statement::CompilePlan(c) => {
                Err(BindError::unsupported(Feature::COMPILED_PLANS, c.span))
            }
            Statement::ExecutePlan(c) => {
                Err(BindError::unsupported(Feature::COMPILED_PLANS, c.span))
            }
            Statement::AnalyzeTable(a) => {
                Err(BindError::unsupported(Feature::ANALYZE_TABLE, a.span))
            }
            Statement::Truncate(t) => {
                let id = self.qualify(&t.table)?;
                if self.lookup(&id).is_none() {
                    return Err(Self::not_found(&id, t.span));
                }
                Err(BindError::unsupported(
                    Feature::ROW_LEVEL_MODIFICATION,
                    t.span,
                ))
            }
            Statement::Delete(d) | Statement::Update(d) => Err(BindError::unsupported(
                Feature::ROW_LEVEL_MODIFICATION,
                d.span,
            )),
            Statement::StopJob(_)
            | Statement::Help(_)
            | Statement::Clear(_)
            | Statement::Quit { .. } => Ok(Bound::Info),
            #[allow(unreachable_patterns)]
            other => Err(BindError::validation(
                format!("Unsupported statement {other:?}"),
                Span::empty(),
            )),
        }
    }

    fn bind_explained(&mut self, statement: &Statement) -> BindResult<Bound> {
        // EXPLAIN validates without applying catalog effects of DDL (Flink only explains DML
        // and queries).
        match statement {
            Statement::Query(_)
            | Statement::Insert(_)
            | Statement::StatementSet(_)
            | Statement::Execute(_) => self.bind(statement),
            other => self.bind(other),
        }
    }

    fn bind_insert(&mut self, insert: &Insert) -> BindResult<ObjectIdentifier> {
        let id = self.qualify(&insert.table)?;
        let Some(target) = self.lookup(&id).cloned() else {
            return Err(Self::not_found(&id, insert.span));
        };
        if target.is_view() {
            return Err(BindError::validation(
                format!("The view {id} cannot be used as a sink table"),
                insert.span,
            ));
        }
        let source = expr::bind_query(self, &insert.source)?;
        let writable: Vec<&ResolvedColumn> = target
            .schema()
            .columns
            .iter()
            .filter(|c| match &c.kind {
                ColumnKind::Physical => true,
                ColumnKind::Metadata { is_virtual, .. } => !is_virtual,
                ColumnKind::Computed(_) => false,
            })
            .collect();
        let expected = if insert.columns.is_empty() {
            writable.len() - insert.partition.len().min(writable.len())
        } else {
            insert.columns.len()
        };
        if source.len() != expected && insert.columns.is_empty() && insert.partition.is_empty() {
            return Err(BindError::validation(
                format!(
                    "Column types of query result and sink for '{id}' do not match.\nCause: Different number of columns."
                ),
                insert.span,
            ));
        }
        Ok(id)
    }

    fn bind_create_table(&mut self, t: &CreateTable) -> BindResult<()> {
        let id = self.qualify(&t.name)?;
        if !t.temporary {
            let catalog = self.catalogs.get(&id.catalog).ok_or_else(|| {
                BindError::validation(format!("Catalog {} does not exist.", id.catalog), t.span)
            })?;
            if !catalog.database_exists(&id.database) {
                return Err(BindError::validation(
                    format!("Could not execute CreateTable in path {id}"),
                    t.span,
                ));
            }
        }
        let mut schema = ResolvedSchema::default();
        // LIKE: start from the source table's schema.
        if let Some(like) = &t.like {
            let source_id = self.qualify(&like.source)?;
            let Some(source) = self.lookup(&source_id) else {
                return Err(Self::not_found(&source_id, t.span));
            };
            schema = source.schema().clone();
        }
        let mut physical_scope = Vec::new();
        for element in &t.elements {
            if let TableElement::Column(c) = element {
                let name = c.name.value.clone();
                let (ty, kind) = match &c.kind {
                    ColumnKind_::Physical { ty, .. } => (
                        LogicalType::from_syntax(ty)
                            .map_err(|e| BindError::validation(e.to_string(), c.span))?,
                        ColumnKind::Physical,
                    ),
                    ColumnKind_::Metadata {
                        ty,
                        from,
                        is_virtual,
                    } => (
                        LogicalType::from_syntax(ty)
                            .map_err(|e| BindError::validation(e.to_string(), c.span))?,
                        ColumnKind::Metadata {
                            key: from.as_ref().map(|l| l.value.clone()),
                            is_virtual: *is_virtual,
                        },
                    ),
                    ColumnKind_::Computed { .. } => continue,
                };
                if schema.column(&name).is_some() {
                    return Err(BindError::validation(
                        format!("A column named '{name}' already exists in the base table."),
                        c.span,
                    ));
                }
                physical_scope.push((name.clone(), ty.clone()));
                schema.columns.push(ResolvedColumn { name, ty, kind });
            }
        }
        // Computed columns are typed against the physical and metadata columns.
        for element in &t.elements {
            if let TableElement::Column(c) = element
                && let ColumnKind_::Computed { expr: e } = &c.kind
            {
                let scope = Scope::from_columns(None, &schema.columns);
                let ty = expr::type_expr(self, &scope, e)?;
                schema.columns.push(ResolvedColumn {
                    name: c.name.value.clone(),
                    ty,
                    kind: ColumnKind::Computed(e.to_string()),
                });
            }
        }
        for element in &t.elements {
            match element {
                TableElement::Watermark(w) => {
                    let parts = name_parts(&w.column);
                    let col = parts.last().cloned().unwrap_or_default();
                    let Some(pos) = schema.columns.iter().position(|c| c.name == col) else {
                        return Err(BindError::validation(
                            format!(
                                "The rowtime attribute field '{col}' is not defined in the table schema, at {}\nAvailable fields: [{}]",
                                w.span.start,
                                schema
                                    .columns
                                    .iter()
                                    .map(|c| format!("'{}'", c.name))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ),
                            w.span,
                        ));
                    };
                    let scope = Scope::from_columns(None, &schema.columns);
                    let wm_ty = expr::type_expr(self, &scope, &w.expr)?;
                    let column = &mut schema.columns[pos];
                    column.ty = expr::as_rowtime(&column.ty).ok_or_else(|| {
                        BindError::validation(
                            format!(
                                "Invalid data type of time field for watermark definition. The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), the supported precision 'p' is from 0 to 3, but the time field type is {}",
                                column.ty
                            ),
                            w.span,
                        )
                    })?;
                    if expr::as_rowtime(&wm_ty).is_none() {
                        return Err(BindError::validation(
                            format!(
                                "Invalid data type of expression for watermark definition. The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), the supported precision 'p' is from 0 to 3, but the watermark expression type is {wm_ty}"
                            ),
                            w.span,
                        ));
                    }
                    schema.watermark = Some((col, w.expr.to_string()));
                }
                TableElement::Constraint(c) if c.unique == UniqueSpec::PrimaryKey => {
                    let cols: Vec<String> = c.columns.iter().map(ident_value).collect();
                    for col in &cols {
                        if schema.column(col).is_none() {
                            return Err(BindError::validation(
                                format!("Primary key column '{col}' is not defined in the schema"),
                                c.span,
                            ));
                        }
                    }
                    schema.primary_key = Some(cols);
                }
                TableElement::Column(Column {
                    name,
                    kind:
                        ColumnKind_::Physical {
                            constraint: Some(c),
                            ..
                        },
                    ..
                }) if c.unique == UniqueSpec::PrimaryKey => {
                    schema.primary_key = Some(vec![name.value.clone()]);
                }
                _ => {}
            }
        }
        if let Some(pk) = &schema.primary_key {
            for col in schema.columns.iter_mut() {
                if pk.contains(&col.name) {
                    col.ty = col.ty.clone().not_null();
                }
            }
        }
        if let Some(q) = &t.as_query {
            let out = expr::bind_query(self, q)?;
            for c in out {
                if schema.column(&c.name).is_none() {
                    schema.columns.push(ResolvedColumn {
                        name: c.name,
                        ty: c.ty,
                        kind: ColumnKind::Physical,
                    });
                }
            }
        }
        let options = t
            .with
            .iter()
            .map(|p| (p.key.value.clone(), p.value.value.clone()))
            .collect();
        let object = CatalogObject::Table {
            schema,
            options,
            partition_keys: t.partitioned_by.iter().map(ident_value).collect(),
            comment: t.comment.as_ref().map(|c| c.value.clone()),
        };
        self.create(
            id,
            object,
            t.temporary,
            t.if_not_exists,
            t.mode != CreateMode::Create,
            t.span,
        )
    }

    fn create(
        &mut self,
        id: ObjectIdentifier,
        object: CatalogObject,
        temporary: bool,
        if_not_exists: bool,
        replace: bool,
        span: Span,
    ) -> BindResult<()> {
        if temporary {
            if self.temporary_objects.contains_key(&id) && !replace {
                if if_not_exists {
                    return Ok(());
                }
                let kind = if object.is_view() { "view" } else { "table" };
                return Err(BindError::validation(
                    format!("Temporary {kind} '{id}' already exists"),
                    span,
                ));
            }
            self.temporary_objects.insert(id, object);
            return Ok(());
        }
        let catalog = self.catalog_mut(&id.catalog.clone(), span)?;
        if replace {
            catalog.drop_object(&id.database, &id.object);
        }
        catalog
            .create_object(&id.database, &id.object, object, if_not_exists)
            .map_err(|e| {
                BindError::validation(
                    format!("Could not execute CreateTable in path {id}\nCause: {}", e.0),
                    span,
                )
            })
    }

    fn bind_create_view(&mut self, v: &CreateView) -> BindResult<()> {
        let id = self.qualify(&v.name)?;
        let out = expr::bind_query(self, &v.query)?;
        if !v.columns.is_empty() && v.columns.len() != out.len() {
            return Err(BindError::validation(
                format!(
                    "VIEW definition and input fields not match:\n\tDef fields: [{}].\n\tInput fields: [{}].",
                    v.columns
                        .iter()
                        .map(ident_value)
                        .collect::<Vec<_>>()
                        .join(", "),
                    out.iter()
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                v.span,
            ));
        }
        let columns = out
            .into_iter()
            .enumerate()
            .map(|(i, c)| ResolvedColumn {
                name: v.columns.get(i).map(ident_value).unwrap_or(c.name),
                ty: c.ty,
                kind: ColumnKind::Physical,
            })
            .collect();
        let object = CatalogObject::View {
            schema: ResolvedSchema {
                columns,
                ..Default::default()
            },
            query: v.query.to_string(),
        };
        self.create(
            id,
            object,
            v.temporary,
            v.if_not_exists,
            v.or_replace,
            v.span,
        )
    }

    fn bind_create_database(&mut self, d: &CreateDatabase) -> BindResult<()> {
        let parts = name_parts(&d.name);
        let (catalog, database) = match parts.as_slice() {
            [db] => (self.current_catalog.clone(), db.clone()),
            [c, db] => (c.clone(), db.clone()),
            _ => return Err(BindError::validation("Invalid database name", d.span)),
        };
        let catalog = self.catalog_mut(&catalog, d.span)?;
        catalog
            .create_database(&database, d.if_not_exists)
            .map_err(|e| {
                BindError::validation(
                    format!("Could not execute CREATE DATABASE: {}", e.0),
                    d.span,
                )
            })
    }

    fn bind_create_catalog(&mut self, c: &CreateCatalog) -> BindResult<()> {
        let name = c.name.value.clone();
        let ty = c
            .with
            .iter()
            .find(|p| p.key.value == "type")
            .map(|p| p.value.value.clone());
        match ty.as_deref() {
            Some("generic_in_memory") => {}
            Some("iceberg") => {
                return Err(BindError::unsupported(Feature::ICEBERG_CATALOG, c.span));
            }
            Some("jdbc") => return Err(BindError::unsupported(Feature::JDBC_CATALOG, c.span)),
            Some("hive") => return Err(BindError::unsupported(Feature::HIVE_DIALECT, c.span)),
            Some(other) => {
                // A catalog factory from a user jar (Flink's `CatalogFactory` SPI). Remember the
                // name so later statements that use the catalog name the same feature.
                self.unsupported_catalogs.insert(name, other.to_owned());
                return Err(BindError::unsupported(Feature::CUSTOM_CATALOGS, c.span));
            }
            None => {
                return Err(BindError::validation(
                    format!(
                        "Catalog options do not contain an option key 'type' for discovering a catalog. Catalog name: {name}"
                    ),
                    c.span,
                ));
            }
        }
        if self.catalogs.contains_key(&name) {
            if c.if_not_exists {
                return Ok(());
            }
            if !c.or_replace {
                return Err(BindError::validation(
                    format!("Could not execute CREATE CATALOG: Catalog {name} already exists."),
                    c.span,
                ));
            }
        }
        self.catalogs.insert(
            name.clone(),
            Box::new(InMemoryCatalog::new(name, "default")),
        );
        Ok(())
    }

    fn bind_create_function(&mut self, f: &CreateFunction) -> BindResult<()> {
        let function = UserFunction {
            class: f.class_name.value.clone(),
            language: f.language,
        };
        if f.temporary && f.system {
            let key = name_parts(&f.name).join(".");
            if self.system_functions.contains_key(&key) && !f.if_not_exists && !f.or_replace {
                return Err(BindError::validation(
                    format!(
                        "Could not register temporary system function. A function named '{key}' does already exist."
                    ),
                    f.span,
                ));
            }
            self.system_functions.insert(key.to_lowercase(), function);
            return Ok(());
        }
        let id = self.qualify(&f.name)?;
        if f.temporary {
            if self.temporary_functions.contains_key(&id) && !f.if_not_exists && !f.or_replace {
                return Err(BindError::validation(
                    format!(
                        "Could not register temporary catalog function. A function '{id}' does already exist."
                    ),
                    f.span,
                ));
            }
            self.temporary_functions.insert(id, function);
            return Ok(());
        }
        let catalog = self.catalog_mut(&id.catalog.clone(), f.span)?;
        if !catalog.database_exists(&id.database) {
            return Err(BindError::validation(
                format!(
                    "Could not execute CREATE CATALOG FUNCTION: (catalogFunction: [Optional[{}]], identifier: [{id}], ignoreIfExists: [{}], isTemporary: [false])",
                    f.class_name.value, f.if_not_exists
                ),
                f.span,
            ));
        }
        catalog.create_function(&id.database, &id.object, &f.class_name.value);
        Ok(())
    }

    fn bind_drop(&mut self, d: &Drop) -> BindResult<()> {
        match d.kind {
            DropKind::Catalog => {
                let name = name_parts(&d.name).join(".");
                if name == self.current_catalog {
                    return Err(BindError::validation(
                        "Cannot drop a catalog which is currently in use.".to_string(),
                        d.span,
                    ));
                }
                if self.catalogs.remove(&name).is_none() && !d.if_exists {
                    return Err(BindError::validation(
                        format!("Cannot drop a catalog which does not exist: {name}"),
                        d.span,
                    ));
                }
                Ok(())
            }
            DropKind::Database { cascade } => {
                let parts = name_parts(&d.name);
                let (catalog, database) = match parts.as_slice() {
                    [db] => (self.current_catalog.clone(), db.clone()),
                    [c, db] => (c.clone(), db.clone()),
                    _ => return Err(BindError::validation("Invalid database name", d.span)),
                };
                if catalog == self.current_catalog && database == self.current_database {
                    return Err(BindError::validation(
                        "Cannot drop a database which is currently in use.",
                        d.span,
                    ));
                }
                let catalog = self.catalog_mut(&catalog, d.span)?;
                catalog
                    .drop_database(&database, d.if_exists, cascade.unwrap_or(false))
                    .map_err(|e| {
                        BindError::validation(
                            format!("Could not execute DROP DATABASE: {}", e.0),
                            d.span,
                        )
                    })
            }
            DropKind::Table | DropKind::View => {
                let id = self.qualify(&d.name)?;
                let want_view = d.kind == DropKind::View;
                if d.temporary {
                    let found = self.temporary_objects.get(&id).map(|o| o.is_view());
                    return match found {
                        Some(is_view) if is_view == want_view => {
                            self.temporary_objects.remove(&id);
                            Ok(())
                        }
                        _ if d.if_exists => Ok(()),
                        _ => Err(BindError::validation(
                            format!(
                                "Temporary {} with identifier '{id}' does not exist.",
                                if want_view { "view" } else { "table" }
                            ),
                            d.span,
                        )),
                    };
                }
                if self.temporary_objects.contains_key(&id) {
                    return Err(BindError::validation(
                        format!(
                            "Temporary {} with identifier '{id}' exists. Drop it first before removing the permanent {}.",
                            if want_view { "view" } else { "table" },
                            if want_view { "view" } else { "table" }
                        ),
                        d.span,
                    ));
                }
                let exists = self
                    .catalogs
                    .get(&id.catalog)
                    .and_then(|c| c.get_object(&id.database, &id.object))
                    .map(|o| o.is_view());
                match exists {
                    Some(is_view) if is_view == want_view => {
                        self.catalog_mut(&id.catalog.clone(), d.span)?
                            .drop_object(&id.database, &id.object);
                        Ok(())
                    }
                    Some(_) => Err(BindError::validation(
                        format!(
                            "{} with identifier '{id}' does not exist.",
                            if want_view { "View" } else { "Table" }
                        ),
                        d.span,
                    )),
                    None if d.if_exists => Ok(()),
                    None => Err(BindError::validation(
                        format!(
                            "{} with identifier '{id}' does not exist.",
                            if want_view { "View" } else { "Table" }
                        ),
                        d.span,
                    )),
                }
            }
            DropKind::Function { system } => {
                if system {
                    let key = name_parts(&d.name).join(".").to_lowercase();
                    if self.system_functions.remove(&key).is_none() && !d.if_exists {
                        return Err(BindError::validation(
                            format!(
                                "Could not drop temporary system function. A function named '{key}' doesn't exist."
                            ),
                            d.span,
                        ));
                    }
                    return Ok(());
                }
                let id = self.qualify(&d.name)?;
                if d.temporary {
                    if self.temporary_functions.remove(&id).is_none() && !d.if_exists {
                        return Err(BindError::validation(
                            format!("Temporary catalog function {id} doesn't exist"),
                            d.span,
                        ));
                    }
                    return Ok(());
                }
                let removed = self
                    .catalogs
                    .get_mut(&id.catalog)
                    .is_some_and(|c| c.drop_function(&id.database, &id.object));
                if !removed && !d.if_exists {
                    return Err(BindError::validation(
                        format!("Function {id} doesn't exist."),
                        d.span,
                    ));
                }
                Ok(())
            }
        }
    }

    fn bind_alter_table(&mut self, a: &AlterTable) -> BindResult<()> {
        let id = self.qualify(&a.name)?;
        let Some(object) = self.lookup(&id).cloned() else {
            if a.if_exists {
                return Ok(());
            }
            return Err(BindError::validation(
                format!("Table {id} doesn't exist or is a temporary table."),
                a.span,
            ));
        };
        if object.is_view() {
            return Err(BindError::validation(
                "ALTER TABLE for a view is not allowed".to_string(),
                a.span,
            ));
        }
        if let AlterTableOperation::RenameTo(new) = &a.operation {
            let new_id = self.qualify(new)?;
            if self.temporary_objects.remove(&id).is_some() {
                self.temporary_objects.insert(new_id, object);
            } else {
                let catalog = self.catalog_mut(&id.catalog.clone(), a.span)?;
                catalog.drop_object(&id.database, &id.object);
                catalog
                    .create_object(&new_id.database, &new_id.object, object, false)
                    .map_err(|e| BindError::validation(e.0, a.span))?;
            }
        }
        Ok(())
    }

    fn bind_use(&mut self, u: &Use) -> BindResult<()> {
        match u {
            Use::Catalog { name, span } => {
                if self.unsupported_catalogs.contains_key(&name.value) {
                    return Err(BindError::unsupported(Feature::CUSTOM_CATALOGS, *span));
                }
                if !self.catalogs.contains_key(&name.value) {
                    return Err(BindError::validation(
                        format!("A catalog with name [{}] does not exist.", name.value),
                        *span,
                    ));
                }
                self.current_catalog = name.value.clone();
                self.current_database = self.catalogs[&name.value].default_database().to_owned();
                Ok(())
            }
            Use::Database { name, span } => {
                let parts = name_parts(name);
                let (catalog, database) = match parts.as_slice() {
                    [db] => (self.current_catalog.clone(), db.clone()),
                    [c, db] => (c.clone(), db.clone()),
                    _ => return Err(BindError::validation("Invalid database name", *span)),
                };
                let exists = self
                    .catalogs
                    .get(&catalog)
                    .is_some_and(|c| c.database_exists(&database));
                if !exists {
                    return Err(BindError::validation(
                        format!(
                            "A database with name [{database}] does not exist in the catalog: [{catalog}]."
                        ),
                        *span,
                    ));
                }
                self.current_catalog = catalog;
                self.current_database = database;
                Ok(())
            }
            Use::Modules { span, .. } => Err(BindError::unsupported(Feature::MODULES, *span)),
        }
    }

    fn bind_show(&mut self, s: &Show) -> BindResult<()> {
        match &s.object {
            ShowObject::Tables {
                from: Some((_, db)),
                ..
            }
            | ShowObject::Views {
                from: Some((_, db)),
                ..
            }
            | ShowObject::Functions {
                from: Some((_, db)),
                ..
            }
            | ShowObject::Procedures {
                from: Some((_, db)),
                ..
            } => {
                let parts = name_parts(db);
                let (catalog, database) = match parts.as_slice() {
                    [d] => (self.current_catalog.clone(), d.clone()),
                    [c, d] => (c.clone(), d.clone()),
                    _ => return Err(BindError::validation("Invalid database name", s.span)),
                };
                let Some(cat) = self.catalogs.get(&catalog) else {
                    return Err(BindError::validation(
                        format!("Catalog '{catalog}' not found"),
                        s.span,
                    ));
                };
                if !cat.database_exists(&database) {
                    return Err(BindError::validation(
                        format!("Database '{catalog}.{database}' not found"),
                        s.span,
                    ));
                }
                Ok(())
            }
            ShowObject::Databases {
                from: Some((_, c)), ..
            } => {
                let name = name_parts(c).join(".");
                if !self.catalogs.contains_key(&name) {
                    return Err(BindError::validation(
                        format!("Catalog '{name}' not found"),
                        s.span,
                    ));
                }
                Ok(())
            }
            ShowObject::Columns { table, .. }
            | ShowObject::CreateTable(table)
            | ShowObject::CreateView(table)
            | ShowObject::Partitions { table, .. } => {
                let id = self.qualify(table)?;
                let Some(object) = self.lookup(&id) else {
                    return Err(BindError::validation(
                        format!(
                            "Could not execute SHOW {}: table {id} does not exist.",
                            match &s.object {
                                ShowObject::Columns { .. } => "COLUMNS",
                                _ => "CREATE TABLE",
                            }
                        ),
                        s.span,
                    ));
                };
                if matches!(s.object, ShowObject::CreateView(_)) && !object.is_view() {
                    return Err(BindError::validation(
                        format!(
                            "SHOW CREATE VIEW is only supported for views, but {id} is a table. Please use SHOW CREATE TABLE instead."
                        ),
                        s.span,
                    ));
                }
                if matches!(s.object, ShowObject::CreateTable(_)) && object.is_view() {
                    return Err(BindError::validation(
                        format!(
                            "SHOW CREATE TABLE is only supported for tables, but {id} is a view. Please use SHOW CREATE VIEW instead."
                        ),
                        s.span,
                    ));
                }
                Ok(())
            }
            ShowObject::CreateCatalog(c) => {
                if !self.catalogs.contains_key(&c.value) {
                    return Err(BindError::validation(
                        format!(
                            "Cannot obtain metadata information from Catalog {}.",
                            c.value
                        ),
                        s.span,
                    ));
                }
                Ok(())
            }
            ShowObject::Modules { .. } => Ok(()),
            _ => Ok(()),
        }
    }

    fn bind_describe(&mut self, d: &Describe) -> BindResult<()> {
        match &d.object {
            DescribeObject::Table(t) => {
                let id = self.qualify(t)?;
                if self.lookup(&id).is_none() {
                    return Err(BindError::validation(
                        format!(
                            "Tables or views with the identifier '{}.{}.{}' doesn't exist.",
                            id.catalog, id.database, id.object
                        ),
                        d.span,
                    ));
                }
                Ok(())
            }
            DescribeObject::Catalog(c) => {
                if !self.catalogs.contains_key(&c.value) {
                    return Err(BindError::validation(
                        format!(
                            "Cannot obtain metadata information from Catalog {}.",
                            c.value
                        ),
                        d.span,
                    ));
                }
                Ok(())
            }
            DescribeObject::Database(db) => {
                let parts = name_parts(db);
                let (catalog, database) = match parts.as_slice() {
                    [x] => (self.current_catalog.clone(), x.clone()),
                    [c, x] => (c.clone(), x.clone()),
                    _ => return Err(BindError::validation("Invalid database name", d.span)),
                };
                if !self
                    .catalogs
                    .get(&catalog)
                    .is_some_and(|c| c.database_exists(&database))
                {
                    return Err(BindError::validation(
                        format!("Database {database} does not exist in Catalog {catalog}."),
                        d.span,
                    ));
                }
                Ok(())
            }
            DescribeObject::Function(_) | DescribeObject::Job(_) => Ok(()),
        }
    }

    /// The schema of an object visible from this session, for `FROM` resolution.
    pub(crate) fn resolve_table(
        &self,
        name: &ObjectName,
    ) -> BindResult<(ObjectIdentifier, ResolvedSchema)> {
        let id = self.qualify(name)?;
        match self.lookup(&id) {
            Some(o) => Ok((id, o.schema().clone())),
            None => Err(BindError::validation(
                format!("Object '{}' not found", name_parts(name).join(".")),
                name.span(),
            )),
        }
    }
}

use crate::ast::ColumnKind as ColumnKind_;
