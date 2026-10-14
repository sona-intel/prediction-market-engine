create table if not exists venue (
    code text primary key,
    name text not null,
    quote text not null,
    wallets boolean not null,
    since timestamptz not null,
    last_ok timestamptz
);

create table if not exists market (
    venue text not null,
    id text not null,
    title text not null,
    kind text not null default '',
    close_ts timestamptz,
    status text not null default '',
    result text not null default '',
    primary key (venue, id)
);

create table if not exists market_snap (
    venue text not null,
    market_id text not null,
    ts timestamptz not null,
    yes_ppm integer not null check (yes_ppm between 0 and 1000000),
    volume_micro bigint not null default 0,
    oi_micro bigint not null default 0,
    vol24_micro bigint not null default 0,
    primary key (venue, market_id, ts)
);
create index if not exists market_snap_ts on market_snap (ts);

create table if not exists trade (
    venue text not null,
    id text not null,
    market_id text not null,
    ts timestamptz not null,
    side text not null,
    price_ppm integer not null check (price_ppm between 0 and 1000000),
    qty_micro bigint not null,
    usd_micro bigint not null,
    maker bytea,
    taker bytea,
    primary key (venue, id)
);
create index if not exists trade_large on trade (venue, usd_micro desc) where usd_micro >= 10000000000;
create index if not exists trade_taker on trade (venue, taker, ts) where taker is not null;
create index if not exists trade_market on trade (venue, market_id, ts);
create index if not exists trade_ts on trade (venue, ts);

create table if not exists hl_wallet (
    address bytea primary key,
    fills bigint not null,
    volume_micro bigint not null,
    gross_pnl bigint not null,
    markets integer not null,
    resolved integer not null,
    won integer not null,
    brier double precision,
    edge double precision,
    edge_se double precision,
    rated boolean not null default false,
    updated_at timestamptz not null default now()
);

create table if not exists levr_position (
    id text primary key,
    wallet bytea not null,
    market_id text not null,
    opened_ts timestamptz not null,
    stake_micro bigint not null,
    size_micro bigint not null,
    entry_ppm integer not null,
    liq_ppm integer,
    status text not null,
    closed_ts timestamptz,
    payout_micro bigint
);
create index if not exists levr_position_wallet on levr_position (wallet, opened_ts);

create table if not exists levr_wallet (
    address bytea primary key,
    positions integer not null,
    stake_micro bigint not null,
    size_micro bigint not null,
    liquidations integer not null,
    settled integer not null,
    realized_pnl bigint not null,
    updated_at timestamptz not null default now()
);

create table if not exists cursor (
    source text primary key,
    value text not null,
    updated_at timestamptz not null default now()
);
