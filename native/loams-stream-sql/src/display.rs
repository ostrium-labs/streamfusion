//! Printing statements as Flink SQL. `parse(print(parse(sql)))` equals `parse(sql)` (the
//! `round_trips_display` test).

use std::fmt::{self, Display, Formatter};

use sqlparser::ast::{Expr, Ident};

use crate::ast::*;
use crate::cursor::quote;
use crate::render::{expr_to_string, hints_to_string, query_to_string};

fn comma<T: Display>(items: &[T]) -> String {
    items
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&quote(&self.value))
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.key, self.value)
    }
}

fn properties(props: &[Property]) -> String {
    format!("({})", comma(props))
}

fn with(f: &mut Formatter<'_>, props: &[Property], always: bool) -> fmt::Result {
    if always || !props.is_empty() {
        write!(f, " WITH {}", properties(props))?;
    }
    Ok(())
}

fn comment(f: &mut Formatter<'_>, comment: &Option<Literal>) -> fmt::Result {
    if let Some(c) = comment {
        write!(f, " COMMENT {c}")?;
    }
    Ok(())
}

impl Display for LikeFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.not {
            f.write_str("NOT ")?;
        }
        f.write_str(if self.ilike { "ILIKE " } else { "LIKE " })?;
        write!(f, "{}", self.pattern)
    }
}

fn partition_spec(spec: &[(Ident, Expr)]) -> String {
    let items: Vec<String> = spec
        .iter()
        .map(|(k, v)| format!("{k} = {}", expr_to_string(v)))
        .collect();
    format!("({})", items.join(", "))
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Query(q) => f.write_str(&query_to_string(&q.query)),
            Statement::Insert(i) => write!(f, "{i}"),
            Statement::CreateTable(t) => write!(f, "{t}"),
            Statement::CreateView(v) => {
                f.write_str("CREATE ")?;
                if v.or_replace {
                    f.write_str("OR REPLACE ")?;
                }
                if v.temporary {
                    f.write_str("TEMPORARY ")?;
                }
                f.write_str("VIEW ")?;
                if v.if_not_exists {
                    f.write_str("IF NOT EXISTS ")?;
                }
                write!(f, "{}", v.name)?;
                if !v.columns.is_empty() {
                    write!(f, " ({})", comma(&v.columns))?;
                }
                comment(f, &v.comment)?;
                write!(f, " AS {}", query_to_string(&v.query))
            }
            Statement::CreateCatalog(c) => {
                f.write_str("CREATE ")?;
                if c.or_replace {
                    f.write_str("OR REPLACE ")?;
                }
                f.write_str("CATALOG ")?;
                if c.if_not_exists {
                    f.write_str("IF NOT EXISTS ")?;
                }
                write!(f, "{}", c.name)?;
                comment(f, &c.comment)?;
                with(f, &c.with, false)
            }
            Statement::CreateDatabase(d) => {
                f.write_str("CREATE ")?;
                if d.or_replace {
                    f.write_str("OR REPLACE ")?;
                }
                f.write_str("DATABASE ")?;
                if d.if_not_exists {
                    f.write_str("IF NOT EXISTS ")?;
                }
                write!(f, "{}", d.name)?;
                comment(f, &d.comment)?;
                with(f, &d.with, false)
            }
            Statement::CreateFunction(c) => {
                f.write_str("CREATE ")?;
                if c.or_replace {
                    f.write_str("OR REPLACE ")?;
                }
                if c.temporary {
                    f.write_str("TEMPORARY ")?;
                }
                if c.system {
                    f.write_str("SYSTEM ")?;
                }
                f.write_str("FUNCTION ")?;
                if c.if_not_exists {
                    f.write_str("IF NOT EXISTS ")?;
                }
                write!(f, "{} AS {}", c.name, c.class_name)?;
                if let Some(l) = c.language {
                    write!(f, " LANGUAGE {l}")?;
                }
                if !c.jars.is_empty() {
                    let jars: Vec<String> = c.jars.iter().map(|j| format!("JAR {j}")).collect();
                    write!(f, " USING {}", jars.join(", "))?;
                }
                with(f, &c.with, false)
            }
            Statement::Drop(d) => {
                f.write_str("DROP ")?;
                if d.temporary {
                    f.write_str("TEMPORARY ")?;
                }
                f.write_str(match d.kind {
                    DropKind::Catalog => "CATALOG ",
                    DropKind::Table => "TABLE ",
                    DropKind::View => "VIEW ",
                    DropKind::Database { .. } => "DATABASE ",
                    DropKind::Function { system: true } => "SYSTEM FUNCTION ",
                    DropKind::Function { system: false } => "FUNCTION ",
                })?;
                if d.if_exists {
                    f.write_str("IF EXISTS ")?;
                }
                write!(f, "{}", d.name)?;
                match d.kind {
                    DropKind::Database {
                        cascade: Some(true),
                    } => f.write_str(" CASCADE"),
                    DropKind::Database {
                        cascade: Some(false),
                    } => f.write_str(" RESTRICT"),
                    _ => Ok(()),
                }
            }
            Statement::AlterTable(a) => write!(f, "{a}"),
            Statement::AlterView(a) => {
                write!(f, "ALTER VIEW {}", a.name)?;
                match &a.operation {
                    AlterViewOperation::RenameTo(n) => write!(f, " RENAME TO {n}"),
                    AlterViewOperation::As(q) => write!(f, " AS {}", query_to_string(q)),
                }
            }
            Statement::AlterCatalog(a) => {
                write!(f, "ALTER CATALOG {}", a.name)?;
                match &a.operation {
                    AlterCatalogOperation::Set(p) => write!(f, " SET {}", properties(p)),
                    AlterCatalogOperation::Reset(k) => write!(f, " RESET ({})", comma(k)),
                    AlterCatalogOperation::Comment(c) => write!(f, " COMMENT {c}"),
                }
            }
            Statement::AlterDatabase(a) => {
                write!(f, "ALTER DATABASE {} SET {}", a.name, properties(&a.with))
            }
            Statement::AlterFunction(a) => {
                f.write_str("ALTER ")?;
                if a.temporary {
                    f.write_str("TEMPORARY ")?;
                }
                if a.system {
                    f.write_str("SYSTEM ")?;
                }
                f.write_str("FUNCTION ")?;
                if a.if_exists {
                    f.write_str("IF EXISTS ")?;
                }
                write!(f, "{} AS {}", a.name, a.class_name)?;
                if let Some(l) = a.language {
                    write!(f, " LANGUAGE {l}")?;
                }
                Ok(())
            }
            Statement::Set(s) => match &s.assignment {
                None => f.write_str("SET"),
                Some((k, v)) => write!(f, "SET {k} = {v}"),
            },
            Statement::Reset(r) => match &r.key {
                None => f.write_str("RESET"),
                Some(k) => write!(f, "RESET {k}"),
            },
            Statement::Use(Use::Catalog { name, .. }) => write!(f, "USE CATALOG {name}"),
            Statement::Use(Use::Database { name, .. }) => write!(f, "USE {name}"),
            Statement::Use(Use::Modules { names, .. }) => write!(f, "USE MODULES {}", comma(names)),
            Statement::Show(s) => write!(f, "{}", s.object),
            Statement::Describe(d) => {
                f.write_str(if d.desc { "DESC" } else { "DESCRIBE" })?;
                let ext = if d.extended { " EXTENDED" } else { "" };
                match &d.object {
                    DescribeObject::Table(n) => write!(f, "{ext} {n}"),
                    DescribeObject::Catalog(n) => write!(f, " CATALOG{ext} {n}"),
                    DescribeObject::Database(n) => write!(f, " DATABASE{ext} {n}"),
                    DescribeObject::Function(n) => write!(f, " FUNCTION{ext} {n}"),
                    DescribeObject::Job(id) => write!(f, " JOB {id}"),
                }
            }
            Statement::Explain(e) => {
                f.write_str("EXPLAIN ")?;
                if e.plan_for {
                    f.write_str("PLAN FOR ")?;
                } else if !e.details.is_empty() {
                    write!(f, "{} ", comma(&e.details))?;
                }
                write!(f, "{}", e.statement)
            }
            Statement::Execute(e) => write!(f, "EXECUTE {}", e.statement),
            Statement::StatementSet(s) => {
                f.write_str("STATEMENT SET BEGIN\n")?;
                for insert in &s.inserts {
                    writeln!(f, "{insert};")?;
                }
                f.write_str("END")
            }
            Statement::BeginStatementSet(_) => f.write_str("BEGIN STATEMENT SET"),
            Statement::EndStatementSet(_) => f.write_str("END"),
            Statement::CompilePlan(c) => {
                f.write_str("COMPILE ")?;
                if c.and_execute {
                    f.write_str("AND EXECUTE ")?;
                }
                write!(f, "PLAN {}", c.path)?;
                if c.if_not_exists {
                    f.write_str(" IF NOT EXISTS")?;
                }
                write!(f, " FOR {}", c.statement)
            }
            Statement::ExecutePlan(e) => write!(f, "EXECUTE PLAN {}", e.path),
            Statement::Call(c) => write!(f, "CALL {}", expr_to_string(&c.call)),
            Statement::LoadModule(l) => {
                write!(f, "LOAD MODULE {}", l.name)?;
                with(f, &l.with, false)
            }
            Statement::UnloadModule(u) => write!(f, "UNLOAD MODULE {}", u.name),
            Statement::AnalyzeTable(a) => {
                write!(f, "ANALYZE TABLE {}", a.table)?;
                if !a.partition.is_empty() {
                    let items: Vec<String> = a
                        .partition
                        .iter()
                        .map(|(k, v)| match v {
                            Some(v) => format!("{k} = {}", expr_to_string(v)),
                            None => k.to_string(),
                        })
                        .collect();
                    write!(f, " PARTITION ({})", items.join(", "))?;
                }
                f.write_str(" COMPUTE STATISTICS")?;
                if a.all_columns {
                    f.write_str(" FOR ALL COLUMNS")?;
                } else if !a.columns.is_empty() {
                    write!(f, " FOR COLUMNS {}", comma(&a.columns))?;
                }
                Ok(())
            }
            Statement::Truncate(t) => write!(f, "TRUNCATE TABLE {}", t.table),
            Statement::StopJob(s) => {
                write!(f, "STOP JOB {}", s.job_id)?;
                if s.with_savepoint {
                    f.write_str(" WITH SAVEPOINT")?;
                }
                if s.with_drain {
                    f.write_str(" WITH DRAIN")?;
                }
                Ok(())
            }
            Statement::Delete(d) | Statement::Update(d) => write!(f, "{}", d.statement),
            Statement::Help(_) => f.write_str("HELP"),
            Statement::Clear(_) => f.write_str("CLEAR"),
            Statement::Quit { exit, .. } => f.write_str(if *exit { "EXIT" } else { "QUIT" }),
        }
    }
}

impl Display for FunctionLanguage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FunctionLanguage::Java => "JAVA",
            FunctionLanguage::Scala => "SCALA",
            FunctionLanguage::Sql => "SQL",
            FunctionLanguage::Python => "PYTHON",
        })
    }
}

impl Display for ExplainDetail {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ExplainDetail::EstimatedCost => "ESTIMATED_COST",
            ExplainDetail::ChangelogMode => "CHANGELOG_MODE",
            ExplainDetail::JsonExecutionPlan => "JSON_EXECUTION_PLAN",
            ExplainDetail::PlanAdvice => "PLAN_ADVICE",
        })
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(if self.upsert { "UPSERT " } else { "INSERT " })?;
        f.write_str(if self.overwrite {
            "OVERWRITE "
        } else {
            "INTO "
        })?;
        write!(f, "{}", self.table)?;
        if !self.hints.is_empty() {
            write!(f, " {}", hints_to_string(&self.hints))?;
        }
        if !self.partition.is_empty() {
            write!(f, " PARTITION {}", partition_spec(&self.partition))?;
        }
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        write!(f, " {}", query_to_string(&self.source))
    }
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self.mode {
            CreateMode::Create => "CREATE ",
            CreateMode::CreateOrReplace => "CREATE OR REPLACE ",
            CreateMode::Replace => "REPLACE ",
        })?;
        if self.temporary {
            f.write_str("TEMPORARY ")?;
        }
        f.write_str("TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if self.has_columns {
            f.write_str(" (\n")?;
            for (i, element) in self.elements.iter().enumerate() {
                let sep = if i + 1 < self.elements.len() { "," } else { "" };
                writeln!(f, "  {element}{sep}")?;
            }
            f.write_str(")")?;
        }
        comment(f, &self.comment)?;
        if let Some(d) = &self.distribution {
            write!(f, " DISTRIBUTED {d}")?;
        }
        if !self.partitioned_by.is_empty() {
            write!(f, " PARTITIONED BY ({})", comma(&self.partitioned_by))?;
        }
        with(f, &self.with, false)?;
        if let Some(like) = &self.like {
            write!(f, " LIKE {}", like.source)?;
            if !like.options.is_empty() {
                let options: Vec<String> = like
                    .options
                    .iter()
                    .map(|(s, feat)| format!("{s} {feat}"))
                    .collect();
                write!(f, " (\n  {}\n)", options.join("\n  "))?;
            }
        }
        if let Some(q) = &self.as_query {
            write!(f, " AS {}", query_to_string(q))?;
        }
        Ok(())
    }
}

impl Display for LikeStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LikeStrategy::Including => "INCLUDING",
            LikeStrategy::Excluding => "EXCLUDING",
            LikeStrategy::Overwriting => "OVERWRITING",
        })
    }
}

impl Display for LikeFeature {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LikeFeature::All => "ALL",
            LikeFeature::Constraints => "CONSTRAINTS",
            LikeFeature::Distribution => "DISTRIBUTION",
            LikeFeature::Generated => "GENERATED",
            LikeFeature::Metadata => "METADATA",
            LikeFeature::Options => "OPTIONS",
            LikeFeature::Partitions => "PARTITIONS",
            LikeFeature::Watermarks => "WATERMARKS",
        })
    }
}

impl Display for Distribution {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            return write!(f, "INTO {} BUCKETS", self.buckets.unwrap_or(0));
        }
        f.write_str("BY ")?;
        if let Some(kind) = &self.kind {
            write!(f, "{kind}")?;
        }
        write!(f, "({})", comma(&self.columns))?;
        if let Some(n) = self.buckets {
            write!(f, " INTO {n} BUCKETS")?;
        }
        Ok(())
    }
}

impl Display for TableElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TableElement::Column(c) => write!(f, "{c}"),
            TableElement::Constraint(c) => write!(f, "{c}"),
            TableElement::Watermark(w) => write!(f, "{w}"),
            TableElement::Identifier(n) => write!(f, "{n}"),
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        match &self.kind {
            ColumnKind::Physical { ty, constraint } => {
                write!(f, " {ty}")?;
                if let Some(c) = constraint {
                    if let Some(name) = &c.name {
                        write!(f, " CONSTRAINT {name}")?;
                    }
                    write!(f, " {}", c.unique)?;
                    if let Some(e) = c.enforcement {
                        write!(f, " {e}")?;
                    }
                }
            }
            ColumnKind::Computed { expr } => write!(f, " AS {}", expr_to_string(expr))?,
            ColumnKind::Metadata {
                ty,
                from,
                is_virtual,
            } => {
                write!(f, " {ty} METADATA")?;
                if let Some(from) = from {
                    write!(f, " FROM {from}")?;
                }
                if *is_virtual {
                    f.write_str(" VIRTUAL")?;
                }
            }
        }
        comment(f, &self.comment)
    }
}

impl Display for UniqueSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            UniqueSpec::PrimaryKey => "PRIMARY KEY",
            UniqueSpec::Unique => "UNIQUE",
        })
    }
}

impl Display for Enforcement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Enforcement::Enforced => "ENFORCED",
            Enforcement::NotEnforced => "NOT ENFORCED",
        })
    }
}

impl Display for TableConstraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "CONSTRAINT {name} ")?;
        }
        write!(f, "{} ({})", self.unique, comma(&self.columns))?;
        if let Some(e) = self.enforcement {
            write!(f, " {e}")?;
        }
        Ok(())
    }
}

impl Display for Watermark {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WATERMARK FOR {} AS {}",
            self.column,
            expr_to_string(&self.expr)
        )
    }
}

impl Display for AlterTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("ALTER TABLE ")?;
        if self.if_exists {
            f.write_str("IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        match &self.operation {
            AlterTableOperation::RenameTo(n) => write!(f, " RENAME TO {n}"),
            AlterTableOperation::RenameColumn { from, to } => write!(f, " RENAME {from} TO {to}"),
            AlterTableOperation::Reset(keys) => write!(f, " RESET ({})", comma(keys)),
            AlterTableOperation::Set(props) => write!(f, " SET {}", properties(props)),
            AlterTableOperation::AddPartitions {
                if_not_exists,
                partitions,
            } => {
                f.write_str(" ADD")?;
                if *if_not_exists {
                    f.write_str(" IF NOT EXISTS")?;
                }
                for (spec, props) in partitions {
                    write!(f, " PARTITION {}", partition_spec(spec))?;
                    with(f, props, false)?;
                }
                Ok(())
            }
            AlterTableOperation::AddOrModify {
                modify,
                parenthesized,
                items,
            } => {
                f.write_str(if *modify { " MODIFY " } else { " ADD " })?;
                if *parenthesized {
                    write!(f, "({})", comma(items))
                } else {
                    write!(f, "{}", comma(items))
                }
            }
            AlterTableOperation::DropPartitions {
                if_exists,
                partitions,
            } => {
                f.write_str(" DROP")?;
                if *if_exists {
                    f.write_str(" IF EXISTS")?;
                }
                let specs: Vec<String> = partitions
                    .iter()
                    .map(|s| format!("PARTITION {}", partition_spec(s)))
                    .collect();
                write!(f, " {}", specs.join(", "))
            }
            AlterTableOperation::DropColumns {
                parenthesized,
                columns,
            } => {
                if *parenthesized {
                    write!(f, " DROP ({})", comma(columns))
                } else {
                    write!(f, " DROP {}", comma(columns))
                }
            }
            AlterTableOperation::DropPrimaryKey => f.write_str(" DROP PRIMARY KEY"),
            AlterTableOperation::DropConstraint(n) => write!(f, " DROP CONSTRAINT {n}"),
            AlterTableOperation::DropDistribution => f.write_str(" DROP DISTRIBUTION"),
            AlterTableOperation::DropWatermark => f.write_str(" DROP WATERMARK"),
        }
    }
}

impl Display for AlterItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            AlterItem::Column { column, position } => {
                write!(f, "{column}")?;
                match position {
                    Some(ColumnPosition::First) => f.write_str(" FIRST"),
                    Some(ColumnPosition::After(c)) => write!(f, " AFTER {c}"),
                    None => Ok(()),
                }
            }
            AlterItem::Constraint(c) => write!(f, "{c}"),
            AlterItem::Watermark(w) => write!(f, "{w}"),
            AlterItem::Distribution(d) => write!(f, "DISTRIBUTION {d}"),
        }
    }
}

fn from_clause(from: &Option<(bool, sqlparser::ast::ObjectName)>) -> String {
    match from {
        Some((true, n)) => format!(" IN {n}"),
        Some((false, n)) => format!(" FROM {n}"),
        None => String::new(),
    }
}

fn like_clause(like: &Option<LikeFilter>) -> String {
    match like {
        Some(l) => format!(" {l}"),
        None => String::new(),
    }
}

impl Display for ShowObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ShowObject::Catalogs { like } => write!(f, "SHOW CATALOGS{}", like_clause(like)),
            ShowObject::CurrentCatalog => f.write_str("SHOW CURRENT CATALOG"),
            ShowObject::CurrentDatabase => f.write_str("SHOW CURRENT DATABASE"),
            ShowObject::Databases { from, like } => {
                write!(
                    f,
                    "SHOW DATABASES{}{}",
                    from_clause(from),
                    like_clause(like)
                )
            }
            ShowObject::Functions { user, from, like } => write!(
                f,
                "SHOW {}FUNCTIONS{}{}",
                if *user { "USER " } else { "" },
                from_clause(from),
                like_clause(like)
            ),
            ShowObject::Procedures { from, like } => {
                write!(
                    f,
                    "SHOW PROCEDURES{}{}",
                    from_clause(from),
                    like_clause(like)
                )
            }
            ShowObject::Tables { from, like } => {
                write!(f, "SHOW TABLES{}{}", from_clause(from), like_clause(like))
            }
            ShowObject::Views { from, like } => {
                write!(f, "SHOW VIEWS{}{}", from_clause(from), like_clause(like))
            }
            ShowObject::Columns { r#in, table, like } => write!(
                f,
                "SHOW COLUMNS {} {table}{}",
                if *r#in { "IN" } else { "FROM" },
                like_clause(like)
            ),
            ShowObject::CreateTable(n) => write!(f, "SHOW CREATE TABLE {n}"),
            ShowObject::CreateView(n) => write!(f, "SHOW CREATE VIEW {n}"),
            ShowObject::CreateCatalog(n) => write!(f, "SHOW CREATE CATALOG {n}"),
            ShowObject::Modules { full } => {
                write!(f, "SHOW {}MODULES", if *full { "FULL " } else { "" })
            }
            ShowObject::Jars => f.write_str("SHOW JARS"),
            ShowObject::Jobs => f.write_str("SHOW JOBS"),
            ShowObject::Partitions { table, partition } => {
                write!(f, "SHOW PARTITIONS {table}")?;
                if let Some(spec) = partition {
                    write!(f, " PARTITION {}", partition_spec(spec))?;
                }
                Ok(())
            }
        }
    }
}
