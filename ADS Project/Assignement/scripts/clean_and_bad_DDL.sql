--drop all the clean and bad tables--
drop table clean_witness cascade constraints;
drop table clean_station cascade constraints;
drop table clean_officer cascade constraints;
drop table clean_region cascade constraints;
drop table clean_case cascade constraints;

drop table bad_witness cascade constraints;
drop table bad_station cascade constraints;
drop table bad_officer cascade constraints;
drop table bad_region cascade constraints;
drop table bad_case cascade constraints;

--drop all of the sequences for the clean and bad tables
drop sequence clean_witness_seq;
drop sequence clean_region_seq;
drop sequence clean_officer_seq;
drop sequence clean_case_seq;
drop sequence clean_station_seq;

drop sequence bad_witness_seq;
drop sequence bad_region_seq;
drop sequence bad_officer_seq;
drop sequence bad_case_seq;
drop sequence bad_station_seq;

--creation of clean table structures
create table clean_region(
    region_id integer not null,
    region_key integer not null,
    region_name varchar2(20),
    area_name varchar2(20),
    region_source varchar2(20) not null,
    constraint pk_clean_region primary key (region_id)
);
create table clean_officer(
    officer_id integer not null,
    officer_key integer not null,
    officer_name varchar2(40),
    officer_source varchar2(20) not null,
    constraint pk_clean_officer primary key (officer_id)
);
create table clean_station(
    station_id integer not null,
    station_key integer not null,
    station_name varchar2(20),
    region_id integer not null,
    station_source varchar(20) not null,
    constraint pk_clean_station primary key(station_id)
);
create table clean_witness(
    witness_id integer not null,
    witness_key integer not null,
    case_id integer,
    witness_type varchar(40),
    witness_source varchar(20) not null,
    constraint pk_clean_witness primary key(witness_id)
);
create table clean_case(
    case_id integer not null,
    case_key integer not null,
    officer_id integer,
    station_id integer not null,
    case_status varchar2(10),
    reported_date date,
    case_type varchar2(20),
    case_source varchar2(20) not null,
    constraint pk_clean_case primary key(case_id)
);

--creation of bad table structures
create table bad_region(
    region_id integer not null,
    region_key integer not null,
    region_name varchar2(20),
    area_name varchar2(20),
    region_source varchar2(20) not null,
    data_status varchar(15) not null,
    constraint pk_bad_region primary key (region_id)
);
create table bad_officer(
    officer_id integer not null,
    officer_key integer not null,
    officer_name varchar2(40),
    officer_source varchar2(20) not null,
    data_status varchar(15) not null,
    constraint pk_bad_officer primary key (officer_id)
);
create table bad_station(
    station_id integer not null,
    station_key integer not null,
    station_name varchar2(20),
    region_id integer not null,
    station_source varchar(20) not null,
    data_status varchar(15) not null,
    constraint pk_bad_station primary key(station_id)
);
create table bad_witness(
    witness_id integer not null,
    witness_key integer not null,
    case_id integer,
    witness_type varchar(40),
    witness_source varchar(20) not null,
    data_status varchar(15) not null,
    constraint pk_bad_witness primary key(witness_id)
);
create table bad_case(
    case_id integer not null,
    case_key integer not null,
    officer_id integer,
    station_id integer not null,
    case_status varchar2(10),
    reported_date date,
    case_type varchar2(20),
    case_source varchar2(20) not null,
    data_status varchar(15) not null,
    constraint pk_bad_case primary key(case_id)
);

--creation of sequences for clean tables
create sequence clean_witness_seq start with 1 increment by 1;
create sequence clean_region_seq start with 1 increment by 1;
create sequence clean_officer_seq start with 1 increment by 1;
create sequence clean_case_seq start with 1 increment by 1;
create sequence clean_station_seq start with 1 increment by 1;

--creation of sequences for bad tables
create sequence bad_witness_seq start with 1 increment by 1;
create sequence bad_region_seq start with 1 increment by 1;
create sequence bad_officer_seq start with 1 increment by 1;
create sequence bad_case_seq start with 1 increment by 1;
create sequence bad_station_seq start with 1 increment by 1;

--creation of triggers for inserting sequences into clean and bad tables
create or replace trigger trigger_clean_region_pk
    before insert on clean_region
    for each row
begin
    select clean_region_seq.nextval
    into :new.region_id
    from dual;
end;
/
create or replace trigger trigger_clean_officer_pk
    before insert on clean_officer
    for each row
begin
    select clean_officer_seq.nextval
    into:new.officer_id
    from dual;
end;
/
create or replace trigger trigger_clean_witness_pk
    before insert on clean_witness
    for each row
begin
    select clean_witness_seq.nextval
    into :new.witness_id
    from dual;
end;
/
create or replace trigger trigger_clean_station_pk
    before insert on clean_station
    for each row
begin
    select clean_station_seq.nextval
    into :new.station_id
    from dual;
end;
/
create or replace trigger trigger_clean_case_pk
    before insert on clean_case
    for each row
begin
    select clean_case_seq.nextval
    into :new.case_id
    from dual;
end;
/
create or replace trigger trigger_bad_region_pk
    before insert on bad_region
    for each row
begin
    select bad_region_seq.nextval
    into :new.region_id
    from dual;
end;
/
create or replace trigger trigger_bad_officer_pk
    before insert on bad_officer
    for each row
begin
    select bad_officer_seq.nextval
    into:new.officer_id
    from dual;
end;
/
create or replace trigger trigger_bad_witness_pk
    before insert on bad_witness
    for each row
begin
    select bad_witness_seq.nextval
    into :new.witness_id
    from dual;
end;
/
create or replace trigger trigger_bad_station_pk
    before insert on bad_station
    for each row
begin
    select bad_station_seq.nextval
    into :new.station_id
    from dual;
end;
/
create or replace trigger trigger_bad_case_pk
    before insert on bad_case
    for each row
begin
    select bad_case_seq.nextval
    into :new.case_id
    from dual;
end;
/