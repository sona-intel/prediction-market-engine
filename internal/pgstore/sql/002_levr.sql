alter table levr_position add column if not exists fee_micro bigint not null default 0;
alter table levr_wallet add column if not exists won integer not null default 0;
alter table levr_position add column if not exists side smallint not null default 0;
