use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "context7_request_logs")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub api_key_id: i64,
    pub api_key_name: String,
    pub method: String,
    pub path: String,
    pub query: String,
    pub status_code: i32,
    pub success: bool,
    pub latency_ms: i64,
    pub error: Option<String>,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub client_source: Option<String>,
    pub client_ide: Option<String>,
    pub client_version: Option<String>,
    pub transport: Option<String>,
    pub started_at: DateTimeUtc,
    pub finished_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
