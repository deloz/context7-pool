use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "context7_minute_stats")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub api_key_id: i64,
    pub api_key_name: String,
    pub minute_at: DateTimeUtc,
    pub total_requests: i32,
    pub success_requests: i32,
    pub failed_requests: i32,
    pub status_2xx: i32,
    pub status_4xx: i32,
    pub status_5xx: i32,
    pub network_errors: i32,
    pub total_latency_ms: i64,
    pub max_latency_ms: i64,
    pub last_status_code: i32,
    pub last_error: Option<String>,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
