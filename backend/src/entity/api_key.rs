use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "api_keys")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub name: String,
    pub api_key: String,
    pub enabled: bool,
    pub health_status: String,
    pub failure_streak: i32,
    pub cooldown_until: Option<DateTimeUtc>,
    pub last_error: Option<String>,
    pub last_status_code: Option<i32>,
    pub last_success_at: Option<DateTimeUtc>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
