--Purpose of this script: For creating staging area required for the data warehouse. 
drop table stage_region cascade constraints;
drop table stage_officer cascade constraints;
drop table stage_station cascade constraints;
drop table stage_case cascade constraints;
drop table stage_witness cascade constraints;

drop sequence stage_region_seq;
drop sequence stage_officer_seq;
drop sequence stage_case_seq;
drop sequence stage_witness_seq;
drop sequence stage_station_seq;

--stage_region table stores information from PL_Area and REGION tables
create table stage_region(
    region_id integer not null,
    region_key integer not null,
    region_name varchar2(20),
    area_name varchar2(20),
    region_source varchar2(20) not null,
    constraint pk_stage_region primary key (region_id)
);

--stage_officer table stores information from PL_POLICE_EMPLOYEE and OFFICER table
create table stage_officer(
    officer_id integer not null,
    officer_key integer not null,
    officer_name varchar2(40),
    officer_source varchar2(20) not null,
    constraint pk_stage_officer primary key (officer_id)
);

--stage_station table stores information from PL_STATION and LOCATION table
create table stage_station(
    station_id integer not null,
    station_key integer not null,
    station_name varchar2(20),
    region_id integer not null,
    station_source varchar(20) not null,
    constraint pk_stage_station primary key(station_id)
);

--stage_witness table stores information from PL_WITNESS and CRIME_REPORTER
create table stage_witness(
    witness_id integer not null,
    witness_key integer not null,
    case_id integer,
    witness_type varchar(40),
    witness_source varchar(20) not null,
    constraint pk_stage_witness primary key(witness_id)
);

--stage_case table stores information from PL_REPORTED_CRIME and CRIME_REGISTER table
create table stage_case(
    case_id integer not null,
    case_key integer not null,
    officer_id integer,
    station_id integer not null,
    case_status varchar2(10),
    reported_date date,
    case_type varchar2(20),
    case_source varchar2(20) not null,
    constraint pk_stage_case primary key(case_id)
);

--one sequence for every staging table above
create sequence stage_region_seq start with 1 increment by 1;
create sequence stage_officer_seq start with 1 increment by 1;
create sequence stage_case_seq start with 1 increment by 1;
create sequence stage_witness_seq start with 1 increment by 1;
create sequence stage_station_seq start with 1 increment by 1;


--Triggers for inserting sequence value in the id coloumns of the stage table.
create or replace trigger trigger_stage_region_pk
    before insert on stage_region
    for each row
begin
    select stage_region_seq.nextval
    into :new.region_id
    from dual;
end;
/
create or replace trigger trigger_stage_officer_pk
    before insert on stage_officer
    for each row
begin
    select stage_officer_seq.nextval
    into:new.officer_id
    from dual;
end;
/
create or replace trigger trigger_stage_witness_pk
    before insert on stage_witness
    for each row
begin
    select stage_witness_seq.nextval
    into :new.witness_id
    from dual;
end;
/
create or replace trigger trigger_stage_station_pk
    before insert on stage_station
    for each row
begin
    select stage_station_seq.nextval
    into :new.station_id
    from dual;
end;
/
create or replace trigger trigger_stage_case_pk
    before insert on stage_case
    for each row
begin
    select stage_case_seq.nextval
    into :new.case_id
    from dual;
end;

