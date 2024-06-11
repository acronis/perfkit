package main

// EventBusDDL is a DDL for Acronis EventBus tables
var EventBusDDL = `
create table acronis_db_bench_eventbus_consolidated
(
    sha1_key {$binary20}  not null
        primary key,
    topic_id bigint      not null,
    type_id  bigint      not null,
    seq      bigint      not null,
    seq_time {$datetime6} not null,
    data     {$longblob}  not null
)
    {$engine}
    {$ascii};

create index acronis_db_bench_eventbus_consolidated_seq_topic_idx
    on acronis_db_bench_eventbus_consolidated (seq, topic_id);

create table acronis_db_bench_eventbus_data
(
    int_id   bigint   not null
        primary key,
    topic_id bigint   not null,
    type_id  bigint   not null,
    data     {$longblob} not null
)
    {$engine}
    {$ascii};

create table acronis_db_bench_eventbus_archive
(
    int_id   bigint      not null
        primary key,
    topic_id bigint      not null,
    seq      bigint      not null,
    seq_time {$datetime6} not null,
    constraint acronis_db_bench_eventbus_archive_ibfk_1
        foreign key (int_id) references acronis_db_bench_eventbus_data (int_id)
            on delete cascade
)
    {$engine}
    {$ascii};

create index acronis_db_bench_eventbus_archive_seq_topic_idx
    on acronis_db_bench_eventbus_archive (seq, topic_id);

create table acronis_db_bench_eventbus_distrlocks
(
    lock_key  varchar(40) not null
        primary key,
    token     {$uuid} null,
    expire_at bigint      null
)
    {$engine};

create table acronis_db_bench_eventbus_migrations
(
    id         varchar(255) not null
        primary key,
    applied_at {$datetime}     null
)
    {$engine};

create table acronis_db_bench_eventbus_sequences
(
    int_id   bigint           not null
        primary key,
    sequence bigint default 0 not null
)
    {$engine}
    {$ascii};

create table acronis_db_bench_eventbus_stream
(
    int_id   bigint      not null
        primary key,
    topic_id bigint      not null,
    seq      bigint      not null,
    seq_time {$datetime} not null,
    constraint acronis_db_bench_eventbus_stream_ibfk_1
        foreign key (int_id) references acronis_db_bench_eventbus_data (int_id)
            on delete cascade
)
    {$engine}
    {$ascii};

create index acronis_db_bench_eventbus_stream_seq_topic_idx
    on acronis_db_bench_eventbus_stream (seq, topic_id);

create table acronis_db_bench_eventbus_topics
(
    internal_id         bigint       not null
        primary key,
    topic_id            varchar(256)                              not null,
    sent_cursor         bigint       default 0                    not null,
    acked_cursor        bigint       default 0                    not null,
    vacuum_cursor       bigint       default 0                    not null,
    cursor_shift_time   {$timestamp6} default {$current_timestamp6} not null,
    legacy_max_seq      bigint       default 0                    not null,
    max_seq             bigint       default 0                    not null,
    consolidated_cursor bigint       default 0                    not null,
    constraint topic_id
        unique (topic_id)
)
    {$engine}
    {$ascii};

create table acronis_db_bench_eventbus_event_types
(
    internal_id       bigint       not null
        primary key,
    topic_internal_id bigint       not null,
    event_type        varchar(256) not null,
    constraint acronis_db_bench_eventbus_event_types_tetui
        unique (topic_internal_id, event_type),
    constraint acronis_db_bench_eventbus_event_types_ibfk_1
        foreign key (topic_internal_id) references acronis_db_bench_eventbus_topics (internal_id)
)
    {$engine}
    {$ascii};

create table acronis_db_bench_eventbus_events
(
    internal_id            {$bigint_autoinc_pk},
    topic_internal_id      bigint       not null,
    event_type_internal_id bigint       not null,
    event_id               {$uuid}  not null,
    source                 varchar(256) not null,
    sequence               bigint       null,
    tenant_id              {$uuid}  not null,
    client_id              {$uuid}  null,
    origin_id              {$uuid}  null,
    trace_parent           {$uuid}  null,
    subject_id             varchar(64)  null,
    data_ref               varchar(256) null,
    data                   {$longblob}  null,
    data_base64            {$longblob}  null,
    created_at             {$datetime},
    consolidation_key      varchar(40)  not null,
    constraint FK_event_type
        foreign key (event_type_internal_id) references acronis_db_bench_eventbus_event_types (internal_id),
    constraint acronis_db_bench_eventbus_events_ibfk_1
        foreign key (topic_internal_id) references acronis_db_bench_eventbus_topics (internal_id)
)
    {$engine}
    {$ascii};

create index acronis_db_bench_eventbus_sequence_internal_id_index
    on acronis_db_bench_eventbus_events (sequence, internal_id);

create index acronis_db_bench_eventbus_topic_internal_id_sequence_index
    on acronis_db_bench_eventbus_events (topic_internal_id, sequence, created_at);

create table acronis_db_bench_eventbus_initial_seeding_cursors
(
    topic_internal_id bigint                 not null,
    seeded_id         bigint      default 0  not null,
    max_id            bigint      default 0  not null,
    name              varchar(64) default '' not null,
    primary key (topic_internal_id, name),
    constraint acronis_db_bench_eventbus_initial_seeding_cursors_ibfk_1
        foreign key (topic_internal_id) references acronis_db_bench_eventbus_topics (internal_id)
)
    {$engine}
    {$ascii};
`
