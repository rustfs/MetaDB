//! Lightweight 20-Template SQL Execution Framework & Epoch-Aware Stateless Router (`metadb-proxy`).

pub mod router;
pub mod sql_lexer;
pub mod sql_templates;

pub use router::{ProxySqlResult, StatelessProxyRouter};
pub use sql_lexer::{SqlLexer, SqlParam, SqlToken};
pub use sql_templates::{MetadataSqlCommand, SqlTemplateParser};
