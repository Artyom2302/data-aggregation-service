-- +goose Up
create table if not exists data_package(
    id UUID primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    payload int not null
);


-- +goose Down
drop table if exists data_package;
