create table accounts (
    id bigint(20) not null auto_increment,
    name varchar(100) not null,
    total_contract_value decimal not null default '0.0',
    last_analyzed_at timestamp not null default current_timestamp,
    analyzer_identifier varchar(36),
    primary key (id),
    unique key name (name)
)
engine = innodb
default charset = utf8;