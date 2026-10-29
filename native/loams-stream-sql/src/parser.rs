//! Statement-level hooks: Flink's statements on top of sqlparser.
//!
//! Each `fn` names the production of Flink 2.2.1's `flink-table/flink-sql-parser`
//! (`src/main/codegen/includes/parserImpls.ftl`, listed in `src/main/codegen/data/Parser.tdd`) or
//! of Calcite's core grammar that it mirrors.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Ident, ObjectName, Query, Value, ValueWithSpan, VisitMut};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Span, Token};

use crate::ast::*;
use crate::cursor::{
    PResult, compound_identifier, eat, eat_all, expect, expect_tok, next_span, peek_is, peek_tok,
    peek_word, peek_words, simple_identifier, span_from, string_literal,
};
use crate::dialect::unsupported;
use crate::error::Feature;
use crate::lexer::HintSite;
use crate::render::QueryChecker;
use crate::types::parse_type;

pub(crate) struct StatementParser<'p, 'a> {
    pub p: &'p mut Parser<'a>,
    pub sites: Vec<HintSite>,
}

fn syntax<T>(message: impl Into<String>, span: Span) -> PResult<T> {
    Err(ParserError::ParserError(format!(
        "{} at Line: {}, Column: {}",
        message.into(),
        span.start.line,
        span.start.column
    )))
}

fn literal(p: &mut Parser) -> PResult<Literal> {
    let (value, span) = string_literal(p)?;
    Ok(Literal { value, span })
}

impl StatementParser<'_, '_> {
    /// Parses one statement and checks that it ends the token stream.
    pub(crate) fn statement_to_end(&mut self) -> PResult<Statement> {
        let statement = self.statement()?;
        let next = self.p.peek_token();
        if next.token != Token::EOF {
            return self.p.expected("end of statement", next);
        }
        Ok(statement)
    }

    /// Calcite's `SqlStmt` with Flink's `statementParserMethods` (Parser.tdd).
    pub(crate) fn statement(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        let Some(word) = peek_word(self.p, 0) else {
            if peek_tok(self.p, 0) == Token::LParen {
                return self.query_statement();
            }
            let found = self.p.peek_token();
            return self.p.expected("a statement", found);
        };
        match word.as_str() {
            "SELECT" | "WITH" | "VALUES" => self.query_statement(),
            "INSERT" | "UPSERT" => Ok(Statement::Insert(self.insert()?)),
            "CREATE" => self.create(),
            "REPLACE" => self.replace_table(),
            "DROP" => self.drop(),
            "ALTER" => self.alter(),
            "SET" => self.set(),
            "RESET" => self.reset(),
            "USE" => self.use_(),
            "SHOW" => self.show(),
            "DESCRIBE" | "DESC" => self.describe(),
            "EXPLAIN" => self.explain(),
            "EXECUTE" => self.execute(),
            "COMPILE" => self.compile(),
            "BEGIN" => {
                // SqlBeginStatementSet: BEGIN STATEMENT SET
                self.p.next_token();
                expect(self.p, "STATEMENT")?;
                expect(self.p, "SET")?;
                Ok(Statement::BeginStatementSet(span_from(self.p, start)))
            }
            "END" => {
                // SqlEndStatementSet: END
                self.p.next_token();
                Ok(Statement::EndStatementSet(span_from(self.p, start)))
            }
            "CALL" => self.call(),
            "LOAD" => self.load_module(),
            "UNLOAD" => self.unload_module(),
            "ANALYZE" => self.analyze_table(),
            "TRUNCATE" => self.truncate(),
            "STOP" => self.stop_job(),
            "ADD" if peek_is(self.p, 1, "JAR") => unsupported(Feature::ADD_JAR, start),
            "REMOVE" if peek_is(self.p, 1, "JAR") => unsupported(Feature::REMOVE_JAR, start),
            "DELETE" | "UPDATE" => self.dml(word == "DELETE"),
            _ => {
                let found = self.p.peek_token();
                self.p.expected("a statement", found)
            }
        }
    }

    // ---------------------------------------------------------------- queries

    /// Parses a query (Calcite's `OrderedQueryOrExpr(ACCEPT_QUERY)`) and attaches table hints.
    fn query(&mut self) -> PResult<Box<Query>> {
        let mut query = self.p.parse_query()?;
        let mut checker = QueryChecker {
            sites: &mut self.sites,
        };
        if let ControlFlow::Break(error) = query.visit(&mut checker) {
            return Err(error.into_parser_error());
        }
        Ok(query)
    }

    fn query_statement(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        let query = self.query()?;
        Ok(Statement::Query(QueryStatement {
            query,
            span: span_from(self.p, start),
        }))
    }

    /// `RichSqlInsert`.
    fn insert(&mut self) -> PResult<Insert> {
        let start = next_span(self.p);
        let upsert = if eat(self.p, "UPSERT") {
            true
        } else {
            expect(self.p, "INSERT")?;
            false
        };
        let overwrite = if eat(self.p, "OVERWRITE") {
            if upsert {
                return syntax(
                    "OVERWRITE expression is only used with INSERT statement.",
                    crate::cursor::last_span(self.p),
                );
            }
            true
        } else {
            expect(self.p, "INTO")?;
            false
        };
        let table = compound_identifier(self.p)?;
        let hints = QueryChecker {
            sites: &mut self.sites,
        }
        .take_hints(&table);
        let partition = if eat(self.p, "PARTITION") {
            self.partition_spec()?
        } else {
            vec![]
        };
        let columns = if peek_tok(self.p, 0) == Token::LParen
            && matches!(peek_tok(self.p, 1), Token::Word(_))
            && !matches!(
                peek_word(self.p, 1).as_deref(),
                Some("SELECT" | "WITH" | "VALUES")
            ) {
            self.p.next_token();
            let columns = self.p.parse_comma_separated(compound_identifier)?;
            expect_tok(self.p, &Token::RParen)?;
            columns
        } else {
            vec![]
        };
        let source = self.query()?;
        Ok(Insert {
            upsert,
            overwrite,
            table,
            hints,
            partition,
            columns,
            source,
            span: span_from(self.p, start),
        })
    }

    /// `PartitionSpecCommaList`: `(k = literal, ...)`.
    fn partition_spec(&mut self) -> PResult<Vec<(Ident, Expr)>> {
        expect_tok(self.p, &Token::LParen)?;
        let spec = self.p.parse_comma_separated(|p| {
            let key = simple_identifier(p)?;
            expect_tok(p, &Token::Eq)?;
            Ok((key, partition_literal(p)?))
        })?;
        expect_tok(self.p, &Token::RParen)?;
        Ok(spec)
    }

    /// `SqlDelete`/`SqlUpdate` (Calcite), delegated to sqlparser.
    fn dml(&mut self, delete: bool) -> PResult<Statement> {
        let start = next_span(self.p);
        let mut statement = self.p.parse_statement()?;
        let mut checker = QueryChecker {
            sites: &mut self.sites,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut checker) {
            return Err(error.into_parser_error());
        }
        let dml = DmlStatement {
            statement: Box::new(statement),
            span: span_from(self.p, start),
        };
        Ok(if delete {
            Statement::Delete(dml)
        } else {
            Statement::Update(dml)
        })
    }

    // ---------------------------------------------------------------- CREATE

    /// Calcite's `SqlCreate` with Flink's `SqlCreateExtended`.
    fn create(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "CREATE")?;
        let or_replace = eat_all(self.p, &["OR", "REPLACE"]);
        let temporary = eat(self.p, "TEMPORARY");
        let word = peek_word(self.p, 0).unwrap_or_default();
        match word.as_str() {
            "CATALOG" => self.create_catalog(start, or_replace),
            "TABLE" => {
                let mode = if or_replace {
                    CreateMode::CreateOrReplace
                } else {
                    CreateMode::Create
                };
                self.create_table(start, mode, temporary)
            }
            "VIEW" => self.create_view(start, or_replace, temporary),
            "DATABASE" => self.create_database(start, or_replace),
            "FUNCTION" | "SYSTEM" => self.create_function(start, or_replace, temporary),
            "MATERIALIZED" => unsupported(Feature::MATERIALIZED_TABLE, start),
            "MODEL" => unsupported(Feature::MODEL, start),
            _ => {
                let found = self.p.peek_token();
                self.p.expected(
                    "CATALOG, TABLE, VIEW, DATABASE or FUNCTION after CREATE",
                    found,
                )
            }
        }
    }

    /// `IfNotExistsOpt`.
    fn if_not_exists(&mut self) -> bool {
        eat_all(self.p, &["IF", "NOT", "EXISTS"])
    }

    /// `IfExistsOpt`.
    fn if_exists(&mut self) -> bool {
        eat_all(self.p, &["IF", "EXISTS"])
    }

    /// `[COMMENT 'text']`.
    fn comment(&mut self) -> PResult<Option<Literal>> {
        if eat(self.p, "COMMENT") {
            Ok(Some(literal(self.p)?))
        } else {
            Ok(None)
        }
    }

    /// `[WITH (props)]`.
    fn with_properties(&mut self) -> PResult<Vec<Property>> {
        if eat(self.p, "WITH") {
            self.properties()
        } else {
            Ok(vec![])
        }
    }

    /// `Properties`: `( ['k' = 'v' [, ...]] )`.
    fn properties(&mut self) -> PResult<Vec<Property>> {
        expect_tok(self.p, &Token::LParen)?;
        let mut properties = vec![];
        if !self.p.consume_token(&Token::RParen) {
            properties = self.p.parse_comma_separated(|p| {
                // TableOption
                let key = literal(p)?;
                expect_tok(p, &Token::Eq)?;
                let value = literal(p)?;
                Ok(Property { key, value })
            })?;
            expect_tok(self.p, &Token::RParen)?;
        }
        Ok(properties)
    }

    /// `PropertyKeys`: `( ['k' [, ...]] )`.
    fn property_keys(&mut self) -> PResult<Vec<Literal>> {
        expect_tok(self.p, &Token::LParen)?;
        let mut keys = vec![];
        if !self.p.consume_token(&Token::RParen) {
            keys = self.p.parse_comma_separated(literal)?;
            expect_tok(self.p, &Token::RParen)?;
        }
        Ok(keys)
    }

    /// `ParenthesizedSimpleIdentifierList`.
    fn simple_identifier_list(&mut self) -> PResult<Vec<Ident>> {
        expect_tok(self.p, &Token::LParen)?;
        let list = self.p.parse_comma_separated(simple_identifier)?;
        expect_tok(self.p, &Token::RParen)?;
        Ok(list)
    }

    /// `SqlCreateCatalog`.
    fn create_catalog(&mut self, start: Span, or_replace: bool) -> PResult<Statement> {
        expect(self.p, "CATALOG")?;
        let if_not_exists = self.if_not_exists();
        let name = simple_identifier(self.p)?;
        let comment = self.comment()?;
        let with = self.with_properties()?;
        Ok(Statement::CreateCatalog(CreateCatalog {
            or_replace,
            if_not_exists,
            name,
            comment,
            with,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlCreateDatabase`.
    fn create_database(&mut self, start: Span, or_replace: bool) -> PResult<Statement> {
        expect(self.p, "DATABASE")?;
        let if_not_exists = self.if_not_exists();
        let name = compound_identifier(self.p)?;
        let comment = self.comment()?;
        let with = self.with_properties()?;
        Ok(Statement::CreateDatabase(CreateDatabase {
            or_replace,
            if_not_exists,
            name,
            comment,
            with,
            span: span_from(self.p, start),
        }))
    }

    /// `[LANGUAGE JAVA|SCALA|SQL|PYTHON]`.
    fn language(&mut self) -> PResult<Option<(FunctionLanguage, Span)>> {
        if !eat(self.p, "LANGUAGE") {
            return Ok(None);
        }
        let span = next_span(self.p);
        let language = match peek_word(self.p, 0).as_deref() {
            Some("JAVA") => FunctionLanguage::Java,
            Some("SCALA") => FunctionLanguage::Scala,
            Some("SQL") => FunctionLanguage::Sql,
            Some("PYTHON") => FunctionLanguage::Python,
            _ => {
                let found = self.p.peek_token();
                return self.p.expected("JAVA, SCALA, SQL or PYTHON", found);
            }
        };
        self.p.next_token();
        Ok(Some((language, span)))
    }

    /// `SqlCreateFunction`.
    fn create_function(
        &mut self,
        start: Span,
        or_replace: bool,
        temporary: bool,
    ) -> PResult<Statement> {
        let system = if eat(self.p, "SYSTEM") {
            if !temporary {
                return syntax(
                    "CREATE SYSTEM FUNCTION is not supported, system functions can only be registered as temporary function, you can use CREATE TEMPORARY SYSTEM FUNCTION instead.",
                    crate::cursor::last_span(self.p),
                );
            }
            true
        } else {
            false
        };
        expect(self.p, "FUNCTION")?;
        let if_not_exists = self.if_not_exists();
        let name = if system {
            ObjectName::from(vec![simple_identifier(self.p)?])
        } else {
            compound_identifier(self.p)?
        };
        expect(self.p, "AS")?;
        let class_name = literal(self.p)?;
        let language = self.language()?;
        let mut jars = vec![];
        if eat(self.p, "USING") {
            if let Some((language @ (FunctionLanguage::Sql | FunctionLanguage::Python), span)) =
                language
            {
                let name = if language == FunctionLanguage::Sql {
                    "SQL"
                } else {
                    "PYTHON"
                };
                return syntax(
                    format!(
                        "CREATE FUNCTION USING JAR syntax is not applicable to {name} language."
                    ),
                    span,
                );
            }
            jars = self.p.parse_comma_separated(|p| {
                // SqlResourceInfo
                expect(p, "JAR")?;
                literal(p)
            })?;
        }
        let with = self.with_properties()?;
        Ok(Statement::CreateFunction(CreateFunction {
            or_replace,
            temporary,
            system,
            if_not_exists,
            name,
            class_name,
            language: language.map(|(l, _)| l),
            jars,
            with,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlCreateView`.
    fn create_view(
        &mut self,
        start: Span,
        or_replace: bool,
        temporary: bool,
    ) -> PResult<Statement> {
        expect(self.p, "VIEW")?;
        let if_not_exists = self.if_not_exists();
        let name = compound_identifier(self.p)?;
        let columns = if peek_tok(self.p, 0) == Token::LParen {
            self.simple_identifier_list()?
        } else {
            vec![]
        };
        let comment = self.comment()?;
        expect(self.p, "AS")?;
        let query = self.query()?;
        Ok(Statement::CreateView(CreateView {
            or_replace,
            temporary,
            if_not_exists,
            name,
            columns,
            comment,
            query,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlReplaceTable`: `REPLACE [TEMPORARY] TABLE ... AS query`.
    fn replace_table(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "REPLACE")?;
        let temporary = eat(self.p, "TEMPORARY");
        self.create_table(start, CreateMode::Replace, temporary)
    }

    /// `SqlCreateTable` (with `SqlCreateTableLike`, `SqlCreateTableAs`, `SqlReplaceTableAs`).
    fn create_table(
        &mut self,
        start: Span,
        mode: CreateMode,
        temporary: bool,
    ) -> PResult<Statement> {
        expect(self.p, "TABLE")?;
        let if_not_exists = self.if_not_exists();
        let name = compound_identifier(self.p)?;
        let mut has_columns = false;
        let mut elements = vec![];
        let mut columns_span = Span::empty();
        if peek_tok(self.p, 0) == Token::LParen {
            let open = next_span(self.p);
            self.p.next_token();
            has_columns = true;
            elements = self.table_elements()?;
            expect_tok(self.p, &Token::RParen)?;
            columns_span = span_from(self.p, open);
        }
        let identifiers_only = !elements.is_empty()
            && elements
                .iter()
                .all(|e| matches!(e, TableElement::Identifier(_)));
        let comment = self.comment()?;
        let distribution = if eat(self.p, "DISTRIBUTED") {
            Some(self.distribution()?)
        } else {
            None
        };
        let partitioned_by = if eat_all(self.p, &["PARTITIONED", "BY"]) {
            self.simple_identifier_list()?
        } else {
            vec![]
        };
        let with = self.with_properties()?;
        let mut like = None;
        let mut as_query = None;
        if mode != CreateMode::Replace && peek_is(self.p, 0, "LIKE") {
            let like_start = next_span(self.p);
            self.p.next_token();
            like = Some(self.table_like(like_start)?);
        } else if mode == CreateMode::Replace {
            expect(self.p, "AS")?;
            as_query = Some(self.query()?);
        } else if eat(self.p, "AS") {
            as_query = Some(self.query()?);
        }
        if identifiers_only && as_query.is_none() {
            return syntax(
                "Columns identifiers without types in the schema are supported on CTAS/RTAS statements only.",
                columns_span,
            );
        }
        Ok(Statement::CreateTable(Box::new(CreateTable {
            mode,
            temporary,
            if_not_exists,
            name,
            has_columns,
            elements,
            comment,
            distribution,
            partitioned_by,
            with,
            like,
            as_query,
            span: span_from(self.p, start),
        })))
    }

    /// `TableColumnsOrIdentifiers`.
    fn table_elements(&mut self) -> PResult<Vec<TableElement>> {
        if peek_tok(self.p, 0) == Token::RParen {
            return Ok(vec![]);
        }
        // LOOKAHEAD(2): a bare (compound) identifier list is for CTAS/RTAS.
        if self.is_identifier_list_item() {
            return self
                .p
                .parse_comma_separated(|p| Ok(TableElement::Identifier(compound_identifier(p)?)));
        }
        let mut watermark_seen = false;
        self.p.parse_comma_separated(|p| {
            let element = table_column(p)?;
            if let TableElement::Watermark(w) = &element {
                if watermark_seen {
                    return syntax(
                        "Multiple WATERMARK statements is not supported yet.",
                        w.span,
                    );
                }
                watermark_seen = true;
            }
            Ok(element)
        })
    }

    fn is_identifier_list_item(&self) -> bool {
        // `a`, `a.b` followed by `,` or `)`.
        let mut i = 0;
        loop {
            if !matches!(peek_tok(self.p, i), Token::Word(_)) {
                return false;
            }
            i += 1;
            match peek_tok(self.p, i) {
                Token::Period => i += 1,
                Token::Comma | Token::RParen => return true,
                _ => return false,
            }
        }
    }

    /// `SqlDistribution`.
    fn distribution(&mut self) -> PResult<Distribution> {
        distribution(self.p)
    }

    /// `SqlTableLike` and `SqlTableLikeOption`.
    fn table_like(&mut self, start: Span) -> PResult<TableLike> {
        let source = compound_identifier(self.p)?;
        let mut options = vec![];
        if self.p.consume_token(&Token::LParen) {
            loop {
                let strategy = match peek_word(self.p, 0).as_deref() {
                    Some("INCLUDING") => LikeStrategy::Including,
                    Some("EXCLUDING") => LikeStrategy::Excluding,
                    Some("OVERWRITING") => LikeStrategy::Overwriting,
                    _ => {
                        let found = self.p.peek_token();
                        return self
                            .p
                            .expected("INCLUDING, EXCLUDING or OVERWRITING", found);
                    }
                };
                self.p.next_token();
                let feature = match peek_word(self.p, 0).as_deref() {
                    Some("ALL") => LikeFeature::All,
                    Some("CONSTRAINTS") => LikeFeature::Constraints,
                    Some("DISTRIBUTION") => LikeFeature::Distribution,
                    Some("GENERATED") => LikeFeature::Generated,
                    Some("METADATA") => LikeFeature::Metadata,
                    Some("OPTIONS") => LikeFeature::Options,
                    Some("PARTITIONS") => LikeFeature::Partitions,
                    Some("WATERMARKS") => LikeFeature::Watermarks,
                    _ => {
                        let found = self.p.peek_token();
                        return self.p.expected(
                            "ALL, CONSTRAINTS, DISTRIBUTION, GENERATED, METADATA, OPTIONS, PARTITIONS or WATERMARKS",
                            found,
                        );
                    }
                };
                self.p.next_token();
                options.push((strategy, feature));
                if self.p.consume_token(&Token::RParen) {
                    break;
                }
            }
        }
        Ok(TableLike {
            source,
            options,
            span: span_from(self.p, start),
        })
    }

    // ---------------------------------------------------------------- DROP

    /// Calcite's `SqlDrop` with Flink's `SqlDropExtended`.
    fn drop(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "DROP")?;
        let temporary = eat(self.p, "TEMPORARY");
        let word = peek_word(self.p, 0).unwrap_or_default();
        let kind = match word.as_str() {
            "CATALOG" => DropKind::Catalog,
            "TABLE" => DropKind::Table,
            "VIEW" => DropKind::View,
            "DATABASE" => DropKind::Database { cascade: None },
            "FUNCTION" => DropKind::Function { system: false },
            "SYSTEM" => {
                self.p.next_token();
                if !peek_is(self.p, 0, "FUNCTION") {
                    let found = self.p.peek_token();
                    return self.p.expected("FUNCTION", found);
                }
                DropKind::Function { system: true }
            }
            "MATERIALIZED" => return unsupported(Feature::MATERIALIZED_TABLE, start),
            "MODEL" => return unsupported(Feature::MODEL, start),
            _ => {
                let found = self.p.peek_token();
                return self.p.expected(
                    "CATALOG, TABLE, VIEW, DATABASE or FUNCTION after DROP",
                    found,
                );
            }
        };
        self.p.next_token();
        let if_exists = self.if_exists();
        let name = compound_identifier(self.p)?;
        let kind = match kind {
            DropKind::Database { .. } => DropKind::Database {
                cascade: if eat(self.p, "CASCADE") {
                    Some(true)
                } else if eat(self.p, "RESTRICT") {
                    Some(false)
                } else {
                    None
                },
            },
            other => other,
        };
        Ok(Statement::Drop(Drop {
            kind,
            temporary,
            if_exists,
            name,
            span: span_from(self.p, start),
        }))
    }

    // ---------------------------------------------------------------- ALTER

    fn alter(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        match peek_word(self.p, 1).as_deref() {
            Some("TABLE") => self.alter_table(start),
            Some("VIEW") => self.alter_view(start),
            Some("CATALOG") => self.alter_catalog(start),
            Some("DATABASE") => self.alter_database(start),
            Some("FUNCTION" | "TEMPORARY") => self.alter_function(start),
            Some("MATERIALIZED") => unsupported(Feature::MATERIALIZED_TABLE, start),
            Some("MODEL") => unsupported(Feature::MODEL, start),
            _ => {
                self.p.next_token();
                let found = self.p.peek_token();
                self.p.expected(
                    "TABLE, VIEW, CATALOG, DATABASE or FUNCTION after ALTER",
                    found,
                )
            }
        }
    }

    /// `SqlAlterCatalog`.
    fn alter_catalog(&mut self, start: Span) -> PResult<Statement> {
        expect(self.p, "ALTER")?;
        expect(self.p, "CATALOG")?;
        let name = simple_identifier(self.p)?;
        let operation = if eat(self.p, "SET") {
            AlterCatalogOperation::Set(self.properties()?)
        } else if eat(self.p, "RESET") {
            AlterCatalogOperation::Reset(self.property_keys()?)
        } else if eat(self.p, "COMMENT") {
            AlterCatalogOperation::Comment(literal(self.p)?)
        } else {
            let found = self.p.peek_token();
            return self.p.expected("SET, RESET or COMMENT", found);
        };
        Ok(Statement::AlterCatalog(AlterCatalog {
            name,
            operation,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlAlterDatabase`.
    fn alter_database(&mut self, start: Span) -> PResult<Statement> {
        expect(self.p, "ALTER")?;
        expect(self.p, "DATABASE")?;
        let name = compound_identifier(self.p)?;
        expect(self.p, "SET")?;
        let with = self.properties()?;
        Ok(Statement::AlterDatabase(AlterDatabase {
            name,
            with,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlAlterFunction`.
    fn alter_function(&mut self, start: Span) -> PResult<Statement> {
        expect(self.p, "ALTER")?;
        let temporary = eat(self.p, "TEMPORARY");
        let system = temporary && eat(self.p, "SYSTEM");
        expect(self.p, "FUNCTION")?;
        let if_exists = self.if_exists();
        let name = compound_identifier(self.p)?;
        expect(self.p, "AS")?;
        let class_name = literal(self.p)?;
        let language = self.language()?.map(|(l, _)| l);
        Ok(Statement::AlterFunction(AlterFunction {
            temporary,
            system,
            if_exists,
            name,
            class_name,
            language,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlAlterView`.
    fn alter_view(&mut self, start: Span) -> PResult<Statement> {
        expect(self.p, "ALTER")?;
        expect(self.p, "VIEW")?;
        let name = compound_identifier(self.p)?;
        let operation = if eat_all(self.p, &["RENAME", "TO"]) {
            AlterViewOperation::RenameTo(compound_identifier(self.p)?)
        } else {
            expect(self.p, "AS")?;
            AlterViewOperation::As(self.query()?)
        };
        Ok(Statement::AlterView(AlterView {
            name,
            operation,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlAlterTable`.
    fn alter_table(&mut self, start: Span) -> PResult<Statement> {
        expect(self.p, "ALTER")?;
        expect(self.p, "TABLE")?;
        let if_exists = self.if_exists();
        let name = compound_identifier(self.p)?;
        let word = peek_word(self.p, 0).unwrap_or_default();
        let operation = match word.as_str() {
            "RENAME" => {
                self.p.next_token();
                if eat(self.p, "TO") {
                    AlterTableOperation::RenameTo(compound_identifier(self.p)?)
                } else {
                    let from = compound_identifier(self.p)?;
                    expect(self.p, "TO")?;
                    let to = compound_identifier(self.p)?;
                    AlterTableOperation::RenameColumn { from, to }
                }
            }
            "RESET" => {
                self.p.next_token();
                AlterTableOperation::Reset(self.property_keys()?)
            }
            "SET" => {
                self.p.next_token();
                AlterTableOperation::Set(self.properties()?)
            }
            "ADD" => {
                self.p.next_token();
                if peek_is(self.p, 0, "PARTITION") || peek_words(self.p, &["IF", "NOT", "EXISTS"]) {
                    // AlterTableAddPartition
                    let if_not_exists = self.if_not_exists();
                    let mut partitions = vec![];
                    while eat(self.p, "PARTITION") {
                        let spec = self.partition_spec()?;
                        let with = self.with_properties()?;
                        partitions.push((spec, with));
                    }
                    if partitions.is_empty() {
                        let found = self.p.peek_token();
                        return self.p.expected("PARTITION", found);
                    }
                    AlterTableOperation::AddPartitions {
                        if_not_exists,
                        partitions,
                    }
                } else {
                    self.add_or_modify(false)?
                }
            }
            "MODIFY" => {
                self.p.next_token();
                self.add_or_modify(true)?
            }
            "DROP" => {
                self.p.next_token();
                self.alter_table_drop()?
            }
            _ => {
                let found = self.p.peek_token();
                return self
                    .p
                    .expected("RENAME, RESET, SET, ADD, MODIFY or DROP", found);
            }
        };
        Ok(Statement::AlterTable(AlterTable {
            if_exists,
            name,
            operation,
            span: span_from(self.p, start),
        }))
    }

    /// `ADD|MODIFY (AlterTableAddOrModify | ( AlterTableAddOrModify, ... ))`.
    fn add_or_modify(&mut self, modify: bool) -> PResult<AlterTableOperation> {
        let parenthesized = self.p.consume_token(&Token::LParen);
        let items = if parenthesized {
            let items = self.p.parse_comma_separated(alter_item)?;
            expect_tok(self.p, &Token::RParen)?;
            items
        } else {
            vec![alter_item(self.p)?]
        };
        let watermarks = items
            .iter()
            .filter(|i| matches!(i, AlterItem::Watermark(_)))
            .count();
        if watermarks > 1 {
            return syntax(
                "Multiple WATERMARK statements is not supported yet.",
                crate::cursor::last_span(self.p),
            );
        }
        Ok(AlterTableOperation::AddOrModify {
            modify,
            parenthesized,
            items,
        })
    }

    /// The `DROP` branch of `SqlAlterTable`.
    fn alter_table_drop(&mut self) -> PResult<AlterTableOperation> {
        if peek_is(self.p, 0, "PARTITION") || peek_words(self.p, &["IF", "EXISTS"]) {
            // AlterTableDropPartitions
            let if_exists = self.if_exists();
            let mut partitions = vec![];
            loop {
                expect(self.p, "PARTITION")?;
                partitions.push(self.partition_spec()?);
                if !self.p.consume_token(&Token::Comma) {
                    break;
                }
            }
            return Ok(AlterTableOperation::DropPartitions {
                if_exists,
                partitions,
            });
        }
        if eat_all(self.p, &["PRIMARY", "KEY"]) {
            return Ok(AlterTableOperation::DropPrimaryKey);
        }
        if eat(self.p, "CONSTRAINT") {
            return Ok(AlterTableOperation::DropConstraint(simple_identifier(
                self.p,
            )?));
        }
        if eat(self.p, "DISTRIBUTION") {
            return Ok(AlterTableOperation::DropDistribution);
        }
        if eat(self.p, "WATERMARK") {
            return Ok(AlterTableOperation::DropWatermark);
        }
        if self.p.consume_token(&Token::LParen) {
            let columns = self.p.parse_comma_separated(compound_identifier)?;
            expect_tok(self.p, &Token::RParen)?;
            return Ok(AlterTableOperation::DropColumns {
                parenthesized: true,
                columns,
            });
        }
        Ok(AlterTableOperation::DropColumns {
            parenthesized: false,
            columns: vec![compound_identifier(self.p)?],
        })
    }

    // ---------------------------------------------------------------- SET, RESET, USE

    /// `SqlSet`: `SET ['key' = 'value']` (the unquoted forms go through `SetOperationParseStrategy`).
    fn set(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "SET")?;
        let assignment = if matches!(peek_tok(self.p, 0), Token::SingleQuotedString(_)) {
            let key = literal(self.p)?;
            expect_tok(self.p, &Token::Eq)?;
            let value = literal(self.p)?;
            Some((key, value))
        } else {
            None
        };
        if let Some((key, value)) = &assignment
            && key.value.eq_ignore_ascii_case("table.sql-dialect")
            && value.value.eq_ignore_ascii_case("hive")
        {
            return unsupported(Feature::HIVE_DIALECT, value.span);
        }
        Ok(Statement::Set(Set {
            assignment,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlReset`: `RESET ['key']`.
    fn reset(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "RESET")?;
        let key = if matches!(peek_tok(self.p, 0), Token::SingleQuotedString(_)) {
            Some(literal(self.p)?)
        } else {
            None
        };
        Ok(Statement::Reset(Reset {
            key,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlUseCatalog`, `SqlUseModules`, `SqlUseDatabase`.
    fn use_(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "USE")?;
        if peek_is(self.p, 0, "CATALOG") && matches!(peek_tok(self.p, 1), Token::Word(_)) {
            self.p.next_token();
            let name = simple_identifier(self.p)?;
            return Ok(Statement::Use(Use::Catalog {
                name,
                span: span_from(self.p, start),
            }));
        }
        if peek_is(self.p, 0, "MODULES") && matches!(peek_tok(self.p, 1), Token::Word(_)) {
            self.p.next_token();
            let names = self.p.parse_comma_separated(simple_identifier)?;
            return Ok(Statement::Use(Use::Modules {
                names,
                span: span_from(self.p, start),
            }));
        }
        let name = compound_identifier(self.p)?;
        Ok(Statement::Use(Use::Database {
            name,
            span: span_from(self.p, start),
        }))
    }

    // ---------------------------------------------------------------- SHOW, DESCRIBE

    /// `[(FROM|IN) compound]`; `true` for `IN`.
    fn show_from(&mut self) -> PResult<Option<(bool, ObjectName)>> {
        if eat(self.p, "FROM") {
            Ok(Some((false, compound_identifier(self.p)?)))
        } else if eat(self.p, "IN") {
            Ok(Some((true, compound_identifier(self.p)?)))
        } else {
            Ok(None)
        }
    }

    /// `[[NOT] LIKE 'p']`, plus `ILIKE` where the production allows it.
    fn show_like(&mut self, ilike_allowed: bool) -> PResult<Option<LikeFilter>> {
        let not = peek_is(self.p, 0, "NOT")
            && (peek_is(self.p, 1, "LIKE") || (ilike_allowed && peek_is(self.p, 1, "ILIKE")));
        if not {
            self.p.next_token();
        }
        let ilike = if eat(self.p, "LIKE") {
            false
        } else if ilike_allowed && eat(self.p, "ILIKE") {
            true
        } else {
            return Ok(None);
        };
        Ok(Some(LikeFilter {
            not,
            ilike,
            pattern: literal(self.p)?,
        }))
    }

    fn show(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "SHOW")?;
        let word = peek_word(self.p, 0).unwrap_or_default();
        let object = match word.as_str() {
            // SqlShowCatalogs
            "CATALOGS" => {
                self.p.next_token();
                ShowObject::Catalogs {
                    like: self.show_like(true)?,
                }
            }
            // SqlShowCurrentCatalogOrDatabase
            "CURRENT" => {
                self.p.next_token();
                if eat(self.p, "CATALOG") {
                    ShowObject::CurrentCatalog
                } else {
                    expect(self.p, "DATABASE")?;
                    ShowObject::CurrentDatabase
                }
            }
            // SqlShowDatabases
            "DATABASES" => {
                self.p.next_token();
                let from = self.show_from()?;
                if let Some((_, name)) = &from
                    && name.0.len() > 1
                {
                    let span = crate::cursor::name_span(name);
                    return syntax(
                        format!(
                            "Show databases from/in identifier [ {name} ] format error, catalog must be a single part identifier."
                        ),
                        span,
                    );
                }
                ShowObject::Databases {
                    from,
                    like: self.show_like(true)?,
                }
            }
            // SqlShowFunctions
            "USER" | "FUNCTIONS" => {
                let user = eat(self.p, "USER");
                expect(self.p, "FUNCTIONS")?;
                ShowObject::Functions {
                    user,
                    from: self.show_from()?,
                    like: self.show_like(true)?,
                }
            }
            // SqlShowProcedures
            "PROCEDURES" => {
                self.p.next_token();
                ShowObject::Procedures {
                    from: self.show_from()?,
                    like: self.show_like(true)?,
                }
            }
            // SqlShowTables
            "TABLES" => {
                self.p.next_token();
                ShowObject::Tables {
                    from: self.show_from()?,
                    like: self.show_like(false)?,
                }
            }
            "VIEWS" => {
                self.p.next_token();
                ShowObject::Views {
                    from: self.show_from()?,
                    like: self.show_like(false)?,
                }
            }
            "MATERIALIZED" => return unsupported(Feature::MATERIALIZED_TABLE, start),
            "MODELS" => return unsupported(Feature::MODEL, start),
            // SqlShowColumns
            "COLUMNS" => {
                self.p.next_token();
                let r#in = if eat(self.p, "IN") {
                    true
                } else {
                    expect(self.p, "FROM")?;
                    false
                };
                ShowObject::Columns {
                    r#in,
                    table: compound_identifier(self.p)?,
                    like: self.show_like(false)?,
                }
            }
            // SqlShowCreate
            "CREATE" => {
                self.p.next_token();
                match peek_word(self.p, 0).as_deref() {
                    Some("TABLE") => {
                        self.p.next_token();
                        ShowObject::CreateTable(compound_identifier(self.p)?)
                    }
                    Some("VIEW") => {
                        self.p.next_token();
                        ShowObject::CreateView(compound_identifier(self.p)?)
                    }
                    Some("CATALOG") => {
                        self.p.next_token();
                        ShowObject::CreateCatalog(simple_identifier(self.p)?)
                    }
                    Some("MODEL") => return unsupported(Feature::MODEL, start),
                    Some("MATERIALIZED") => return unsupported(Feature::MATERIALIZED_TABLE, start),
                    _ => {
                        let found = self.p.peek_token();
                        return self.p.expected("TABLE, VIEW or CATALOG", found);
                    }
                }
            }
            // SqlShowModules
            "FULL" | "MODULES" => {
                let full = eat(self.p, "FULL");
                expect(self.p, "MODULES")?;
                ShowObject::Modules { full }
            }
            // SqlShowJars
            "JARS" => {
                self.p.next_token();
                ShowObject::Jars
            }
            // SqlShowJobs
            "JOBS" => {
                self.p.next_token();
                ShowObject::Jobs
            }
            // SqlShowPartitions
            "PARTITIONS" => {
                self.p.next_token();
                let table = compound_identifier(self.p)?;
                let partition = if eat(self.p, "PARTITION") {
                    Some(self.partition_spec()?)
                } else {
                    None
                };
                ShowObject::Partitions { table, partition }
            }
            _ => {
                let found = self.p.peek_token();
                return self.p.expected("a SHOW statement", found);
            }
        };
        Ok(Statement::Show(Show {
            object,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlDescribeCatalog`, `SqlDescribeDatabase`, `SqlRichDescribeFunction`, `SqlDescribeJob`,
    /// `SqlRichDescribeTable`.
    fn describe(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        let desc = eat(self.p, "DESC");
        if !desc {
            expect(self.p, "DESCRIBE")?;
        }
        let word = peek_word(self.p, 0).unwrap_or_default();
        let named = |p: &Parser| {
            matches!(peek_tok(p, 1), Token::Word(_))
                || (peek_is(p, 1, "EXTENDED") && matches!(peek_tok(p, 2), Token::Word(_)))
        };
        let (extended, object) = match word.as_str() {
            "CATALOG" if named(self.p) => {
                self.p.next_token();
                let extended = eat(self.p, "EXTENDED");
                (
                    extended,
                    DescribeObject::Catalog(simple_identifier(self.p)?),
                )
            }
            "DATABASE" if named(self.p) => {
                self.p.next_token();
                let extended = eat(self.p, "EXTENDED");
                (
                    extended,
                    DescribeObject::Database(compound_identifier(self.p)?),
                )
            }
            "FUNCTION" if named(self.p) => {
                self.p.next_token();
                let extended = eat(self.p, "EXTENDED");
                (
                    extended,
                    DescribeObject::Function(compound_identifier(self.p)?),
                )
            }
            "MODEL" if named(self.p) => return unsupported(Feature::MODEL, start),
            "JOB" if matches!(peek_tok(self.p, 1), Token::SingleQuotedString(_)) => {
                self.p.next_token();
                (false, DescribeObject::Job(literal(self.p)?))
            }
            _ => {
                let extended = eat(self.p, "EXTENDED");
                (
                    extended,
                    DescribeObject::Table(compound_identifier(self.p)?),
                )
            }
        };
        Ok(Statement::Describe(Describe {
            desc,
            extended,
            object,
            span: span_from(self.p, start),
        }))
    }

    // ---------------------------------------------------------------- EXPLAIN, EXECUTE, COMPILE

    /// `SqlRichExplain` with `ParseExplainDetail`.
    fn explain(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "EXPLAIN")?;
        let mut plan_for = false;
        let mut details: Vec<ExplainDetail> = vec![];
        if eat_all(self.p, &["PLAN", "FOR"]) {
            plan_for = true;
        } else {
            loop {
                let detail = match peek_word(self.p, 0).as_deref() {
                    Some("ESTIMATED_COST") => ExplainDetail::EstimatedCost,
                    Some("CHANGELOG_MODE") => ExplainDetail::ChangelogMode,
                    Some("JSON_EXECUTION_PLAN") => ExplainDetail::JsonExecutionPlan,
                    Some("PLAN_ADVICE") => ExplainDetail::PlanAdvice,
                    _ => break,
                };
                let span = next_span(self.p);
                self.p.next_token();
                if details.contains(&detail) {
                    return syntax("Duplicate EXPLAIN DETAIL is not allowed.", span);
                }
                details.push(detail);
                if !self.p.consume_token(&Token::Comma) {
                    break;
                }
            }
        }
        let statement_start = next_span(self.p);
        let statement = match peek_word(self.p, 0).as_deref() {
            Some("REPLACE") => self.replace_table()?,
            Some("STATEMENT") => Statement::StatementSet(self.statement_set()?),
            Some("EXECUTE") => self.execute()?,
            Some("INSERT" | "UPSERT") => Statement::Insert(self.insert()?),
            Some("CREATE") => {
                let statement = self.create()?;
                let is_as = matches!(&statement, Statement::CreateTable(t) if t.as_query.is_some());
                if !is_as {
                    return syntax(
                        "Unsupported CREATE OR REPLACE statement for EXPLAIN. The statement must define a query using the AS clause (i.e. CTAS/RTAS statements).",
                        statement_start,
                    );
                }
                statement
            }
            _ => self.query_statement()?,
        };
        Ok(Statement::Explain(Explain {
            plan_for,
            details,
            statement: Box::new(statement),
            span: span_from(self.p, start),
        }))
    }

    /// `SqlStatementSet`: `STATEMENT SET BEGIN (insert ;)+ END`.
    fn statement_set(&mut self) -> PResult<StatementSet> {
        let start = next_span(self.p);
        expect(self.p, "STATEMENT")?;
        expect(self.p, "SET")?;
        expect(self.p, "BEGIN")?;
        let mut inserts = vec![];
        loop {
            if !matches!(peek_word(self.p, 0).as_deref(), Some("INSERT" | "UPSERT")) {
                let found = self.p.peek_token();
                if inserts.is_empty() {
                    return self.p.expected("INSERT or UPSERT", found);
                }
                break;
            }
            inserts.push(self.insert()?);
            expect_tok(self.p, &Token::SemiColon)?;
        }
        expect(self.p, "END")?;
        Ok(StatementSet {
            inserts,
            span: span_from(self.p, start),
        })
    }

    /// `SqlExecutePlan` and `SqlExecute`.
    fn execute(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "EXECUTE")?;
        if eat(self.p, "PLAN") {
            let path = literal(self.p)?;
            return Ok(Statement::ExecutePlan(ExecutePlan {
                path,
                span: span_from(self.p, start),
            }));
        }
        let statement = match peek_word(self.p, 0).as_deref() {
            Some("STATEMENT") => Statement::StatementSet(self.statement_set()?),
            Some("INSERT" | "UPSERT") => Statement::Insert(self.insert()?),
            _ => self.query_statement()?,
        };
        Ok(Statement::Execute(Execute {
            statement: Box::new(statement),
            span: span_from(self.p, start),
        }))
    }

    /// `SqlCompilePlan` and `SqlCompileAndExecutePlan`.
    fn compile(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "COMPILE")?;
        let and_execute = eat_all(self.p, &["AND", "EXECUTE"]);
        expect(self.p, "PLAN")?;
        let path = literal(self.p)?;
        let if_not_exists = !and_execute && self.if_not_exists();
        expect(self.p, "FOR")?;
        let statement = if peek_is(self.p, 0, "STATEMENT") {
            Statement::StatementSet(self.statement_set()?)
        } else {
            Statement::Insert(self.insert()?)
        };
        Ok(Statement::CompilePlan(CompilePlan {
            and_execute,
            path,
            if_not_exists,
            statement: Box::new(statement),
            span: span_from(self.p, start),
        }))
    }

    // ---------------------------------------------------------------- the rest

    /// Calcite's `SqlProcedureCall`: `CALL routine(args)`.
    fn call(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "CALL")?;
        let call = self.p.parse_expr()?;
        if !matches!(call, Expr::Function(_)) {
            return syntax("Expected a procedure call after CALL", start);
        }
        Ok(Statement::Call(Call {
            call,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlLoadModule`.
    fn load_module(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "LOAD")?;
        expect(self.p, "MODULE")?;
        let name = simple_identifier(self.p)?;
        let with = self.with_properties()?;
        Ok(Statement::LoadModule(LoadModule {
            name,
            with,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlUnloadModule`.
    fn unload_module(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "UNLOAD")?;
        expect(self.p, "MODULE")?;
        let name = simple_identifier(self.p)?;
        Ok(Statement::UnloadModule(UnloadModule {
            name,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlAnalyzeTable`.
    fn analyze_table(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "ANALYZE")?;
        expect(self.p, "TABLE")?;
        let table = compound_identifier(self.p)?;
        let mut partition = vec![];
        if eat(self.p, "PARTITION") {
            // ExtendedPartitionSpecCommaList
            expect_tok(self.p, &Token::LParen)?;
            partition = self.p.parse_comma_separated(|p| {
                let key = simple_identifier(p)?;
                let value = if p.consume_token(&Token::Eq) {
                    Some(partition_literal(p)?)
                } else {
                    None
                };
                Ok((key, value))
            })?;
            expect_tok(self.p, &Token::RParen)?;
        }
        expect(self.p, "COMPUTE")?;
        expect(self.p, "STATISTICS")?;
        let mut columns = vec![];
        let mut all_columns = false;
        if eat(self.p, "FOR") {
            if eat_all(self.p, &["ALL", "COLUMNS"]) {
                all_columns = true;
            } else {
                expect(self.p, "COLUMNS")?;
                columns = self.p.parse_comma_separated(simple_identifier)?;
            }
        }
        Ok(Statement::AnalyzeTable(AnalyzeTable {
            table,
            partition,
            columns,
            all_columns,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlTruncateTable`.
    fn truncate(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "TRUNCATE")?;
        expect(self.p, "TABLE")?;
        let table = compound_identifier(self.p)?;
        Ok(Statement::Truncate(Truncate {
            table,
            span: span_from(self.p, start),
        }))
    }

    /// `SqlStopJob`.
    fn stop_job(&mut self) -> PResult<Statement> {
        let start = next_span(self.p);
        expect(self.p, "STOP")?;
        expect(self.p, "JOB")?;
        let job_id = literal(self.p)?;
        let with_savepoint = eat_all(self.p, &["WITH", "SAVEPOINT"]);
        let drain_span = next_span(self.p);
        let with_drain = eat_all(self.p, &["WITH", "DRAIN"]);
        if with_drain && !with_savepoint {
            return syntax(
                "WITH DRAIN could only be used after WITH SAVEPOINT.",
                drain_span,
            );
        }
        Ok(Statement::StopJob(StopJob {
            job_id,
            with_savepoint,
            with_drain,
            span: span_from(self.p, start),
        }))
    }
}

/// Calcite's `Literal()` in a partition spec: strings, numbers, booleans, `NULL`, typed and
/// interval literals, with an optional sign.
fn partition_literal(p: &mut Parser) -> PResult<Expr> {
    let start = next_span(p);
    let expr = p.parse_prefix()?;
    fn is_literal(e: &Expr) -> bool {
        match e {
            Expr::Value(ValueWithSpan { value, .. }) => !matches!(value, Value::Placeholder(_)),
            Expr::TypedString(_) | Expr::Interval(_) => true,
            Expr::UnaryOp { expr, .. } => is_literal(expr),
            _ => false,
        }
    }
    if !is_literal(&expr) {
        return syntax("Expected a literal in the partition spec", start);
    }
    Ok(expr)
}

/// `SqlDistribution`: `INTO n BUCKETS` or `BY [HASH|RANGE] (cols) [INTO n BUCKETS]`.
fn distribution(p: &mut Parser) -> PResult<Distribution> {
    let start = next_span(p);
    let buckets = |p: &mut Parser| -> PResult<u64> {
        expect(p, "INTO")?;
        let n = p.parse_literal_uint()?;
        if n == 0 {
            return syntax("Bucket count must be a positive integer.", last_span_of(p));
        }
        expect(p, "BUCKETS")?;
        Ok(n)
    };
    if peek_is(p, 0, "INTO") {
        let n = buckets(p)?;
        return Ok(Distribution {
            kind: None,
            columns: vec![],
            buckets: Some(n),
            span: span_from(p, start),
        });
    }
    expect(p, "BY")?;
    let kind = match peek_word(p, 0).as_deref() {
        Some(k @ ("HASH" | "RANGE")) => {
            let k = k.to_string();
            p.next_token();
            Some(k)
        }
        _ => None,
    };
    expect_tok(p, &Token::LParen)?;
    let columns = p.parse_comma_separated(simple_identifier)?;
    expect_tok(p, &Token::RParen)?;
    let buckets = if peek_is(p, 0, "INTO") {
        Some(buckets(p)?)
    } else {
        None
    };
    Ok(Distribution {
        kind,
        columns,
        buckets,
        span: span_from(p, start),
    })
}

fn last_span_of(p: &Parser) -> Span {
    crate::cursor::last_span(p)
}

/// `TableColumn`: a typed, computed or metadata column, a table constraint or a watermark.
fn table_column(p: &mut Parser) -> PResult<TableElement> {
    if peek_is(p, 0, "WATERMARK") && peek_is(p, 1, "FOR") {
        return Ok(TableElement::Watermark(watermark(p)?));
    }
    if matches!(
        peek_word(p, 0).as_deref(),
        Some("CONSTRAINT" | "PRIMARY" | "UNIQUE")
    ) {
        return Ok(TableElement::Constraint(table_constraint(p)?));
    }
    Ok(TableElement::Column(column(p)?))
}

/// `Watermark`: `WATERMARK FOR rowtime AS expr`.
fn watermark(p: &mut Parser) -> PResult<Watermark> {
    let start = next_span(p);
    expect(p, "WATERMARK")?;
    expect(p, "FOR")?;
    let column = compound_identifier(p)?;
    expect(p, "AS")?;
    let expr = p.parse_expr()?;
    Ok(Watermark {
        column,
        expr,
        span: span_from(p, start),
    })
}

/// `ConstraintName`, `UniqueSpec`, `ConstraintEnforcement`.
fn constraint_head(p: &mut Parser) -> PResult<(Option<Ident>, UniqueSpec)> {
    let name = if eat(p, "CONSTRAINT") {
        Some(simple_identifier(p)?)
    } else {
        None
    };
    let unique = if eat_all(p, &["PRIMARY", "KEY"]) {
        UniqueSpec::PrimaryKey
    } else {
        expect(p, "UNIQUE")?;
        UniqueSpec::Unique
    };
    Ok((name, unique))
}

fn enforcement(p: &mut Parser) -> Option<Enforcement> {
    if eat(p, "ENFORCED") {
        Some(Enforcement::Enforced)
    } else if eat_all(p, &["NOT", "ENFORCED"]) {
        Some(Enforcement::NotEnforced)
    } else {
        None
    }
}

/// `TableConstraint`.
fn table_constraint(p: &mut Parser) -> PResult<TableConstraint> {
    let start = next_span(p);
    let (name, unique) = constraint_head(p)?;
    expect_tok(p, &Token::LParen)?;
    let columns = p.parse_comma_separated(simple_identifier)?;
    expect_tok(p, &Token::RParen)?;
    let enforcement = enforcement(p);
    Ok(TableConstraint {
        name,
        unique,
        columns,
        enforcement,
        span: span_from(p, start),
    })
}

/// `TypedColumn` (with `MetadataColumn`/`RegularColumn`) or `ComputedColumn`.
fn column(p: &mut Parser) -> PResult<Column> {
    let start = next_span(p);
    let name = simple_identifier(p)?;
    if p.peek_token_ref().token == Token::Period {
        let found = p.peek_token();
        return p.expected(
            "a column type (compound column names are not supported)",
            found,
        );
    }
    let kind = if peek_is(p, 0, "AS") {
        p.next_token();
        ColumnKind::Computed {
            expr: p.parse_expr()?,
        }
    } else {
        let ty = parse_type(p)?;
        if eat(p, "METADATA") {
            let from = if eat(p, "FROM") {
                Some(literal(p)?)
            } else {
                None
            };
            let is_virtual = eat(p, "VIRTUAL");
            ColumnKind::Metadata {
                ty,
                from,
                is_virtual,
            }
        } else {
            let constraint = if matches!(
                peek_word(p, 0).as_deref(),
                Some("CONSTRAINT" | "PRIMARY" | "UNIQUE")
            ) {
                let c_start = next_span(p);
                let (name, unique) = constraint_head(p)?;
                let enforcement = enforcement(p);
                Some(ColumnConstraint {
                    name,
                    unique,
                    enforcement,
                    span: span_from(p, c_start),
                })
            } else {
                None
            };
            ColumnKind::Physical { ty, constraint }
        }
    };
    let comment = if eat(p, "COMMENT") {
        Some(literal(p)?)
    } else {
        None
    };
    Ok(Column {
        name,
        kind,
        comment,
        span: span_from(p, start),
    })
}

/// `AlterTableAddOrModify`: a column with an optional position, a constraint, a watermark or
/// `DISTRIBUTION ...`.
fn alter_item(p: &mut Parser) -> PResult<AlterItem> {
    if peek_is(p, 0, "WATERMARK") && peek_is(p, 1, "FOR") {
        return Ok(AlterItem::Watermark(watermark(p)?));
    }
    if matches!(
        peek_word(p, 0).as_deref(),
        Some("CONSTRAINT" | "PRIMARY" | "UNIQUE")
    ) {
        return Ok(AlterItem::Constraint(table_constraint(p)?));
    }
    if peek_is(p, 0, "DISTRIBUTION") && matches!(peek_word(p, 1).as_deref(), Some("BY" | "INTO")) {
        p.next_token();
        return Ok(AlterItem::Distribution(distribution(p)?));
    }
    let column = column(p)?;
    let position = if eat(p, "FIRST") {
        Some(ColumnPosition::First)
    } else if eat(p, "AFTER") {
        Some(ColumnPosition::After(compound_identifier(p)?))
    } else {
        None
    };
    Ok(AlterItem::Column { column, position })
}
