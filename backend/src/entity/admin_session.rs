use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "admin_sessions")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub token_hash: String,
    pub admin_user_id: i64,
    pub expires_at: DateTimeUtc,
    pub created_at: DateTimeUtc,
    pub last_used_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::admin_user::Entity",
        from = "Column::AdminUserId",
        to = "super::admin_user::Column::Id"
    )]
    AdminUser,
}

impl Related<super::admin_user::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::AdminUser.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
