--drop all the transformation tables
drop table transformation_witness cascade constraints;
drop table transformation_station cascade constraints;
drop table transformation_officer cascade constraints;
drop table transformation_region cascade constraints;
drop table transformation_case cascade constraints;

--drop all the transformation sequences
drop sequence transformation_witness_seq;
drop sequence transformation_region_seq;
drop sequence transformation_officer_seq;
drop sequence transformation_case_seq;
drop sequence transformation_station_seq;

--create all the transformation tables
create table transformation_region(
    region_id integer not null,
    region_key integer not null,
    region_name varchar2(20),
    area_name varchar2(20),
    region_source varchar2(20) not null,
    constraint pk_transform_region primary key (region_id)
);
create table transformation_officer(
    officer_id integer not null,
    officer_key integer not null,
    officer_name varchar2(40),
    officer_source varchar2(20) not null,
    constraint pk_transform_officer primary key (officer_id)
);
create table transformation_station(
    station_id integer not null,
    station_key integer not null,
    station_name varchar2(20),
    region_id integer not null,
    station_source varchar(20) not null,
    constraint pk_transform_station primary key(station_id)
);
create table transformation_witness(
    witness_id integer not null,
    witness_key integer not null,
    case_id integer,
    witness_type varchar(40),
    witness_source varchar(20) not null,
    constraint pk_transform_witness primary key(witness_id)
);
create table transformation_case(
    case_id integer not null,
    case_key integer not null,
    officer_id integer,
    station_id integer not null,
    case_status varchar2(10),
    reported_date date,
    case_type varchar2(20),
    case_source varchar2(20) not null,
    constraint pk_transform_case primary key(case_id)
);

--create all the transformation sequences
create sequence transformation_witness_seq start with 1 increment by 1;
create sequence transformation_region_seq start with 1 increment by 1;
create sequence transformation_officer_seq start with 1 increment by 1;
create sequence transformation_case_seq start with 1 increment by 1;
create sequence transformation_station_seq start with 1 increment by 1;

--create all the transformation triggers
create or replace trigger transformation_region_pk
    before insert on transformation_region
    for each row
begin
    select transformation_region_seq.nextval
    into :new.region_id
    from dual;
end;
/
create or replace trigger transformation_officer_pk
    before insert on transformation_officer
    for each row
begin
    select transformation_officer_seq.nextval
    into:new.officer_id
    from dual;
end;
/
create or replace trigger transformation_witness_pk
    before insert on transformation_witness
    for each row
begin
    select transformation_witness_seq.nextval
    into :new.witness_id
    from dual;
end;
/
create or replace trigger transformation_station_pk
    before insert on transformation_station
    for each row
begin
    select transformation_station_seq.nextval
    into :new.station_id
    from dual;
end;
/
create or replace trigger transformation_case_pk
    before insert on transformation_case
    for each row
begin
    select transformation_case_seq.nextval
    into :new.case_id
    from dual;
end;
/