//! The Loams Stream statement AST.
//!
//! Flink-specific statements are Loams nodes; queries, expressions, `DELETE` and `UPDATE` wrap
//! sqlparser's AST. Every node keeps its source span for D2268's errors (sqlparser's nodes give
//! theirs through `sqlparser::ast::Spanned`).

use sqlparser::ast::{Expr, Ident, ObjectName, Query};
use sqlparser::tokenizer::Span;

use crate::types::FlinkType;

/// A string literal with its span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    pub value: String,
    pub span: Span,
}

/// `'key' = 'value'` in a `WITH (...)`/`SET (...)` property list (`TableOption`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    pub key: Literal,
    pub value: Literal,
}

/// `[NOT] LIKE|ILIKE 'pattern'`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeFilter {
    pub not: bool,
    pub ilike: bool,
    pub pattern: Literal,
}

/// A hint: `NAME`, `NAME(a, b)`, `NAME('k' = 'v', ...)` or `NAME('a', 'b')` (Calcite's `SqlHint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hint {
    pub name: Ident,
    pub options: HintOptions,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HintOptions {
    None,
    Identifiers(Vec<Ident>),
    Literals(Vec<Literal>),
    /// Keys are identifiers or string literals; values are string literals.
    KeyValues(Vec<(HintKey, Literal)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HintKey {
    Identifier(Ident),
    Literal(Literal),
}

/// One parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A query: `SELECT`, `WITH`, `VALUES`, set operations (`OrderedQueryOrExpr`).
    Query(QueryStatement),
    Insert(Insert),
    CreateTable(Box<CreateTable>),
    CreateView(CreateView),
    CreateCatalog(CreateCatalog),
    CreateDatabase(CreateDatabase),
    CreateFunction(CreateFunction),
    Drop(Drop),
    AlterTable(AlterTable),
    AlterView(AlterView),
    AlterCatalog(AlterCatalog),
    AlterDatabase(AlterDatabase),
    AlterFunction(AlterFunction),
    Set(Set),
    Reset(Reset),
    Use(Use),
    Show(Show),
    Describe(Describe),
    Explain(Explain),
    /// `EXECUTE (STATEMENT SET ... | query | insert)`.
    Execute(Execute),
    /// `STATEMENT SET BEGIN insert; ... END` (after `EXECUTE`, `EXPLAIN` or `COMPILE PLAN`).
    StatementSet(StatementSet),
    BeginStatementSet(Span),
    EndStatementSet(Span),
    CompilePlan(CompilePlan),
    ExecutePlan(ExecutePlan),
    Call(Call),
    LoadModule(LoadModule),
    UnloadModule(UnloadModule),
    AnalyzeTable(AnalyzeTable),
    Truncate(Truncate),
    StopJob(StopJob),
    /// `DELETE FROM ...` (sqlparser's AST).
    Delete(DmlStatement),
    /// `UPDATE ... SET ...` (sqlparser's AST).
    Update(DmlStatement),
    /// `HELP` (Flink's `HelpOperationParseStrategy`).
    Help(Span),
    /// `CLEAR` (`ClearOperationParseStrategy`).
    Clear(Span),
    /// `QUIT` or `EXIT` (`QuitOperationParseStrategy`).
    Quit {
        exit: bool,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryStatement {
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DmlStatement {
    pub statement: Box<sqlparser::ast::Statement>,
    pub span: Span,
}

/// `RichSqlInsert`: `{INSERT | UPSERT} {INTO | OVERWRITE} t [/*+ hints */] [PARTITION (...)]
/// [(cols)] query`.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub upsert: bool,
    pub overwrite: bool,
    pub table: ObjectName,
    pub hints: Vec<Hint>,
    /// `PARTITION (k = literal, ...)`.
    pub partition: Vec<(Ident, Expr)>,
    pub columns: Vec<ObjectName>,
    pub source: Box<Query>,
    pub span: Span,
}

/// `CREATE [OR REPLACE]` versus `REPLACE` for tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateMode {
    Create,
    CreateOrReplace,
    /// `REPLACE TABLE ... AS` (`SqlReplaceTable`).
    Replace,
}

/// `SqlCreateTable`, `SqlCreateTableLike`, `SqlCreateTableAs`, `SqlReplaceTableAs`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub mode: CreateMode,
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    /// The column list was given (possibly empty, as in `CREATE TABLE t () WITH (...)`).
    pub has_columns: bool,
    pub elements: Vec<TableElement>,
    pub comment: Option<Literal>,
    pub distribution: Option<Distribution>,
    pub partitioned_by: Vec<Ident>,
    pub with: Vec<Property>,
    pub like: Option<TableLike>,
    pub as_query: Option<Box<Query>>,
    pub span: Span,
}

/// An element of a `CREATE TABLE` column list (`TableColumn`).
#[derive(Debug, Clone, PartialEq)]
pub enum TableElement {
    Column(Column),
    Constraint(TableConstraint),
    Watermark(Watermark),
    /// A bare column name, allowed only by CTAS/RTAS (`TableColumnsOrIdentifiers`).
    Identifier(ObjectName),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: Ident,
    pub kind: ColumnKind,
    pub comment: Option<Literal>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnKind {
    /// `name type [constraint]` (`RegularColumn`).
    Physical {
        ty: FlinkType,
        constraint: Option<ColumnConstraint>,
    },
    /// `name AS expr` (`ComputedColumn`).
    Computed { expr: Expr },
    /// `name type METADATA [FROM 'key'] [VIRTUAL]` (`MetadataColumn`).
    Metadata {
        ty: FlinkType,
        from: Option<Literal>,
        is_virtual: bool,
    },
}

/// `[CONSTRAINT name] PRIMARY KEY|UNIQUE [[NOT] ENFORCED]` after a column (`ColumnConstraint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnConstraint {
    pub name: Option<Ident>,
    pub unique: UniqueSpec,
    pub enforcement: Option<Enforcement>,
    pub span: Span,
}

/// `[CONSTRAINT name] PRIMARY KEY|UNIQUE (cols) [[NOT] ENFORCED]` (`TableConstraint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableConstraint {
    pub name: Option<Ident>,
    pub unique: UniqueSpec,
    pub columns: Vec<Ident>,
    pub enforcement: Option<Enforcement>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueSpec {
    PrimaryKey,
    Unique,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enforcement {
    Enforced,
    NotEnforced,
}

/// `WATERMARK FOR rowtime AS expr` (`Watermark`).
#[derive(Debug, Clone, PartialEq)]
pub struct Watermark {
    pub column: ObjectName,
    pub expr: Expr,
    pub span: Span,
}

/// `DISTRIBUTED BY [HASH|RANGE] (cols) [INTO n BUCKETS]` or `DISTRIBUTED INTO n BUCKETS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Distribution {
    pub kind: Option<String>,
    pub columns: Vec<Ident>,
    pub buckets: Option<u64>,
    pub span: Span,
}

/// `LIKE source [(INCLUDING|EXCLUDING|OVERWRITING ALL|CONSTRAINTS|... ...)]` (`SqlTableLike`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLike {
    pub source: ObjectName,
    pub options: Vec<(LikeStrategy, LikeFeature)>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LikeStrategy {
    Including,
    Excluding,
    Overwriting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LikeFeature {
    All,
    Constraints,
    Distribution,
    Generated,
    Metadata,
    Options,
    Partitions,
    Watermarks,
}

/// `CREATE [OR REPLACE] [TEMPORARY] VIEW [IF NOT EXISTS] v [(cols)] [COMMENT '...'] AS query`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateView {
    pub or_replace: bool,
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub comment: Option<Literal>,
    pub query: Box<Query>,
    pub span: Span,
}

/// `CREATE CATALOG [IF NOT EXISTS] c [COMMENT '...'] [WITH (...)]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCatalog {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub name: Ident,
    pub comment: Option<Literal>,
    pub with: Vec<Property>,
    pub span: Span,
}

/// `CREATE DATABASE [IF NOT EXISTS] [c.]d [COMMENT '...'] [WITH (...)]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub comment: Option<Literal>,
    pub with: Vec<Property>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionLanguage {
    Java,
    Scala,
    Sql,
    Python,
}

/// `CREATE [TEMPORARY [SYSTEM]] FUNCTION [IF NOT EXISTS] f AS 'class' [LANGUAGE ...]
/// [USING JAR '...', ...] [WITH (...)]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunction {
    pub or_replace: bool,
    pub temporary: bool,
    pub system: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub class_name: Literal,
    pub language: Option<FunctionLanguage>,
    pub jars: Vec<Literal>,
    pub with: Vec<Property>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropKind {
    Catalog,
    Table,
    View,
    /// `cascade` is `Some(true)` for `CASCADE`, `Some(false)` for `RESTRICT`.
    Database {
        cascade: Option<bool>,
    },
    Function {
        system: bool,
    },
}

/// `DROP [TEMPORARY] {CATALOG|TABLE|VIEW|DATABASE|[SYSTEM] FUNCTION} [IF EXISTS] name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drop {
    pub kind: DropKind,
    pub temporary: bool,
    pub if_exists: bool,
    pub name: ObjectName,
    pub span: Span,
}

/// `ALTER TABLE [IF EXISTS] t <operation>` (`SqlAlterTable`).
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub if_exists: bool,
    pub name: ObjectName,
    pub operation: AlterTableOperation,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    RenameTo(ObjectName),
    RenameColumn {
        from: ObjectName,
        to: ObjectName,
    },
    Reset(Vec<Literal>),
    Set(Vec<Property>),
    AddPartitions {
        if_not_exists: bool,
        partitions: Vec<(Vec<(Ident, Expr)>, Vec<Property>)>,
    },
    /// `ADD ...` / `MODIFY ...`; `parenthesized` records `ADD (a, b)` versus `ADD a`.
    AddOrModify {
        modify: bool,
        parenthesized: bool,
        items: Vec<AlterItem>,
    },
    DropPartitions {
        if_exists: bool,
        partitions: Vec<Vec<(Ident, Expr)>>,
    },
    DropColumns {
        parenthesized: bool,
        columns: Vec<ObjectName>,
    },
    DropPrimaryKey,
    DropConstraint(Ident),
    DropDistribution,
    DropWatermark,
}

/// An `ADD`/`MODIFY` item (`AlterTableAddOrModify`).
#[derive(Debug, Clone, PartialEq)]
pub enum AlterItem {
    Column {
        column: Column,
        position: Option<ColumnPosition>,
    },
    Constraint(TableConstraint),
    Watermark(Watermark),
    Distribution(Distribution),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnPosition {
    First,
    After(ObjectName),
}

/// `ALTER VIEW v RENAME TO v2` or `ALTER VIEW v AS query`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterView {
    pub name: ObjectName,
    pub operation: AlterViewOperation,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterViewOperation {
    RenameTo(ObjectName),
    As(Box<Query>),
}

/// `ALTER CATALOG c {SET (...) | RESET (...) | COMMENT '...'}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterCatalog {
    pub name: Ident,
    pub operation: AlterCatalogOperation,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterCatalogOperation {
    Set(Vec<Property>),
    Reset(Vec<Literal>),
    Comment(Literal),
}

/// `ALTER DATABASE d SET (...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub name: ObjectName,
    pub with: Vec<Property>,
    pub span: Span,
}

/// `ALTER [TEMPORARY [SYSTEM]] FUNCTION [IF EXISTS] f AS 'class' [LANGUAGE ...]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterFunction {
    pub temporary: bool,
    pub system: bool,
    pub if_exists: bool,
    pub name: ObjectName,
    pub class_name: Literal,
    pub language: Option<FunctionLanguage>,
    pub span: Span,
}

/// `SET` and `SET 'k' = 'v'` (`SqlSet`, `SetOperationParseStrategy`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Set {
    pub assignment: Option<(Literal, Literal)>,
    pub span: Span,
}

/// `RESET` and `RESET 'k'` (`SqlReset`, `ResetOperationParseStrategy`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reset {
    pub key: Option<Literal>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Use {
    /// `USE CATALOG c`.
    Catalog { name: Ident, span: Span },
    /// `USE [c.]d`.
    Database { name: ObjectName, span: Span },
    /// `USE MODULES m1, m2`.
    Modules { names: Vec<Ident>, span: Span },
}

/// `SHOW ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Show {
    pub object: ShowObject,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowObject {
    Catalogs {
        like: Option<LikeFilter>,
    },
    CurrentCatalog,
    CurrentDatabase,
    Databases {
        from: Option<(bool, ObjectName)>,
        like: Option<LikeFilter>,
    },
    Functions {
        user: bool,
        from: Option<(bool, ObjectName)>,
        like: Option<LikeFilter>,
    },
    Procedures {
        from: Option<(bool, ObjectName)>,
        like: Option<LikeFilter>,
    },
    Tables {
        from: Option<(bool, ObjectName)>,
        like: Option<LikeFilter>,
    },
    Views {
        from: Option<(bool, ObjectName)>,
        like: Option<LikeFilter>,
    },
    /// `SHOW COLUMNS FROM|IN t`; the flag is `true` for `IN`.
    Columns {
        r#in: bool,
        table: ObjectName,
        like: Option<LikeFilter>,
    },
    CreateTable(ObjectName),
    CreateView(ObjectName),
    CreateCatalog(Ident),
    Modules {
        full: bool,
    },
    Jars,
    Jobs,
    Partitions {
        table: ObjectName,
        partition: Option<Vec<(Ident, Expr)>>,
    },
}

/// `DESCRIBE`/`DESC ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Describe {
    /// `DESC` rather than `DESCRIBE`.
    pub desc: bool,
    pub extended: bool,
    pub object: DescribeObject,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeObject {
    Table(ObjectName),
    Catalog(Ident),
    Database(ObjectName),
    Function(ObjectName),
    Job(Literal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainDetail {
    EstimatedCost,
    ChangelogMode,
    JsonExecutionPlan,
    PlanAdvice,
}

/// `EXPLAIN [PLAN FOR | detail, ...] statement` (`SqlRichExplain`).
#[derive(Debug, Clone, PartialEq)]
pub struct Explain {
    pub plan_for: bool,
    pub details: Vec<ExplainDetail>,
    pub statement: Box<Statement>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Execute {
    pub statement: Box<Statement>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementSet {
    pub inserts: Vec<Insert>,
    pub span: Span,
}

/// `COMPILE PLAN 'path' [IF NOT EXISTS] FOR ...` and `COMPILE AND EXECUTE PLAN 'path' FOR ...`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompilePlan {
    pub and_execute: bool,
    pub path: Literal,
    pub if_not_exists: bool,
    pub statement: Box<Statement>,
    pub span: Span,
}

/// `EXECUTE PLAN 'path'`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutePlan {
    pub path: Literal,
    pub span: Span,
}

/// `CALL procedure(args)` (Calcite's `SqlProcedureCall`).
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub call: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadModule {
    pub name: Ident,
    pub with: Vec<Property>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnloadModule {
    pub name: Ident,
    pub span: Span,
}

/// `ANALYZE TABLE t [PARTITION (...)] COMPUTE STATISTICS [FOR COLUMNS a, b | FOR ALL COLUMNS]`.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTable {
    pub table: ObjectName,
    pub partition: Vec<(Ident, Option<Expr>)>,
    pub columns: Vec<Ident>,
    pub all_columns: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub table: ObjectName,
    pub span: Span,
}

/// `STOP JOB 'id' [WITH SAVEPOINT] [WITH DRAIN]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopJob {
    pub job_id: Literal,
    pub with_savepoint: bool,
    pub with_drain: bool,
    pub span: Span,
}

impl Statement {
    /// The statement's source span.
    pub fn span(&self) -> Span {
        match self {
            Statement::Query(s) => s.span,
            Statement::Insert(s) => s.span,
            Statement::CreateTable(s) => s.span,
            Statement::CreateView(s) => s.span,
            Statement::CreateCatalog(s) => s.span,
            Statement::CreateDatabase(s) => s.span,
            Statement::CreateFunction(s) => s.span,
            Statement::Drop(s) => s.span,
            Statement::AlterTable(s) => s.span,
            Statement::AlterView(s) => s.span,
            Statement::AlterCatalog(s) => s.span,
            Statement::AlterDatabase(s) => s.span,
            Statement::AlterFunction(s) => s.span,
            Statement::Set(s) => s.span,
            Statement::Reset(s) => s.span,
            Statement::Use(Use::Catalog { span, .. })
            | Statement::Use(Use::Database { span, .. })
            | Statement::Use(Use::Modules { span, .. }) => *span,
            Statement::Show(s) => s.span,
            Statement::Describe(s) => s.span,
            Statement::Explain(s) => s.span,
            Statement::Execute(s) => s.span,
            Statement::StatementSet(s) => s.span,
            Statement::BeginStatementSet(span)
            | Statement::EndStatementSet(span)
            | Statement::Help(span)
            | Statement::Clear(span) => *span,
            Statement::Quit { span, .. } => *span,
            Statement::CompilePlan(s) => s.span,
            Statement::ExecutePlan(s) => s.span,
            Statement::Call(s) => s.span,
            Statement::LoadModule(s) => s.span,
            Statement::UnloadModule(s) => s.span,
            Statement::AnalyzeTable(s) => s.span,
            Statement::Truncate(s) => s.span,
            Statement::StopJob(s) => s.span,
            Statement::Delete(s) | Statement::Update(s) => s.span,
        }
    }

    /// A stable name for the statement's kind, used for coverage counts.
    pub fn kind(&self) -> &'static str {
        match self {
            Statement::Query(_) => "QUERY",
            Statement::Insert(i) if i.upsert => "UPSERT",
            Statement::Insert(i) if i.overwrite => "INSERT OVERWRITE",
            Statement::Insert(_) => "INSERT INTO",
            Statement::CreateTable(t) if t.as_query.is_some() => "CREATE TABLE AS",
            Statement::CreateTable(t) if t.like.is_some() => "CREATE TABLE LIKE",
            Statement::CreateTable(_) => "CREATE TABLE",
            Statement::CreateView(_) => "CREATE VIEW",
            Statement::CreateCatalog(_) => "CREATE CATALOG",
            Statement::CreateDatabase(_) => "CREATE DATABASE",
            Statement::CreateFunction(_) => "CREATE FUNCTION",
            Statement::Drop(d) => match d.kind {
                DropKind::Catalog => "DROP CATALOG",
                DropKind::Table => "DROP TABLE",
                DropKind::View => "DROP VIEW",
                DropKind::Database { .. } => "DROP DATABASE",
                DropKind::Function { .. } => "DROP FUNCTION",
            },
            Statement::AlterTable(_) => "ALTER TABLE",
            Statement::AlterView(_) => "ALTER VIEW",
            Statement::AlterCatalog(_) => "ALTER CATALOG",
            Statement::AlterDatabase(_) => "ALTER DATABASE",
            Statement::AlterFunction(_) => "ALTER FUNCTION",
            Statement::Set(_) => "SET",
            Statement::Reset(_) => "RESET",
            Statement::Use(Use::Catalog { .. }) => "USE CATALOG",
            Statement::Use(Use::Database { .. }) => "USE",
            Statement::Use(Use::Modules { .. }) => "USE MODULES",
            Statement::Show(s) => match s.object {
                ShowObject::Catalogs { .. } => "SHOW CATALOGS",
                ShowObject::CurrentCatalog => "SHOW CURRENT CATALOG",
                ShowObject::CurrentDatabase => "SHOW CURRENT DATABASE",
                ShowObject::Databases { .. } => "SHOW DATABASES",
                ShowObject::Functions { .. } => "SHOW FUNCTIONS",
                ShowObject::Procedures { .. } => "SHOW PROCEDURES",
                ShowObject::Tables { .. } => "SHOW TABLES",
                ShowObject::Views { .. } => "SHOW VIEWS",
                ShowObject::Columns { .. } => "SHOW COLUMNS",
                ShowObject::CreateTable(_) => "SHOW CREATE TABLE",
                ShowObject::CreateView(_) => "SHOW CREATE VIEW",
                ShowObject::CreateCatalog(_) => "SHOW CREATE CATALOG",
                ShowObject::Modules { .. } => "SHOW MODULES",
                ShowObject::Jars => "SHOW JARS",
                ShowObject::Jobs => "SHOW JOBS",
                ShowObject::Partitions { .. } => "SHOW PARTITIONS",
            },
            Statement::Describe(d) => match d.object {
                DescribeObject::Table(_) => "DESCRIBE",
                DescribeObject::Catalog(_) => "DESCRIBE CATALOG",
                DescribeObject::Database(_) => "DESCRIBE DATABASE",
                DescribeObject::Function(_) => "DESCRIBE FUNCTION",
                DescribeObject::Job(_) => "DESCRIBE JOB",
            },
            Statement::Explain(_) => "EXPLAIN",
            Statement::Execute(_) => "EXECUTE",
            Statement::StatementSet(_) => "STATEMENT SET",
            Statement::BeginStatementSet(_) => "BEGIN STATEMENT SET",
            Statement::EndStatementSet(_) => "END",
            Statement::CompilePlan(c) if c.and_execute => "COMPILE AND EXECUTE PLAN",
            Statement::CompilePlan(_) => "COMPILE PLAN",
            Statement::ExecutePlan(_) => "EXECUTE PLAN",
            Statement::Call(_) => "CALL",
            Statement::LoadModule(_) => "LOAD MODULE",
            Statement::UnloadModule(_) => "UNLOAD MODULE",
            Statement::AnalyzeTable(_) => "ANALYZE TABLE",
            Statement::Truncate(_) => "TRUNCATE TABLE",
            Statement::StopJob(_) => "STOP JOB",
            Statement::Delete(_) => "DELETE",
            Statement::Update(_) => "UPDATE",
            Statement::Help(_) => "HELP",
            Statement::Clear(_) => "CLEAR",
            Statement::Quit { .. } => "QUIT",
        }
    }
}
