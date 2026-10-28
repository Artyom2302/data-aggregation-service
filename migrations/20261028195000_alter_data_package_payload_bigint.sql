-- +goose Up
alter table data_package alter column payload type bigint;


-- +goose Down
alter table data_package alter column payload type int;
