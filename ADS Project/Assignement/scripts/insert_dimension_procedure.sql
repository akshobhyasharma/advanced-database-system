--this package contains the procedures to load dimension tables
create or replace package insert_dimension_procedure is
    procedure dimension_officer;
    procedure dimension_region;
    procedure dimension_station;
    procedure dimension_witness;
    procedure dimension_case;
    procedure dimension_time;
    procedure ins_fact_crime;
end insert_dimension_procedure;
/

create or replace package body insert_dimension_procedure is
    --this procedure loads the data into dimension_officer
    procedure dimension_officer
    is
    --selecting value from the relevant transformation table
        cursor c_officer is (select * from transformation_officer);
    begin
    --insertion of each rows in the transformation table
        for row_officer in c_officer loop
            insert into dim_officer(officer_name, officer_key, officer_source)
            values(row_officer.officer_name, row_officer.officer_key, row_officer.officer_source);
        end loop;
        insert into dim_officer(officer_name, officer_key, officer_source) values('UNASSIGNED', null, 'prcs');
        insert into dim_officer(officer_name, officer_key, officer_source) values ('UNASSIGNED',null, 'ps_wales');
    end dimension_officer;

    --this procedure loads the data into dimension_station
    procedure dimension_station
    is
    --selecting value from the relevant transformation table
        cursor c_station is (select * from transformation_station);
    begin
    --insertion of each rows in the transformation table
        for row_station in c_station loop
            insert into dim_station(station_name, station_key, station_source)
            values (row_station.station_name, row_station.station_key, row_station.station_source);
        end loop;
    end dimension_station;

    procedure dimension_region
    is
    --selecting value from the relevant transformation table
        cursor c_region is (select * from transformation_region);
    begin
        for row_region in c_region loop
            insert into dim_region(region_name, area_name, region_key, region_source)
            values (row_region.region_name, row_region.area_name, row_region.region_key, row_region.region_source);
        end loop;
    end dimension_region;

    --this procedure loads the data into dimension_witness
    procedure dimension_witness
    is
    --selecting value from the relevant transformation table
        cursor c_witness is (select distinct witness_key, witness_type, witness_source from transformation_witness);
    begin
    --insertion of each rows in the transformation table
        for row_witness in c_witness loop
            insert into dim_witness(witness_key, witness_type, witness_source)
            values (row_witness.witness_key, row_witness.witness_type, row_witness.witness_source);
        end loop;
        insert into dim_witness(witness_key, witness_type, witness_source) values (null , 'N/A', 'prcs');
        insert into dim_witness(witness_key, witness_type, witness_source) values (null, 'N/A','ps_wales');
    end dimension_witness;

    --this procedure loads the data into dimension_case
    procedure dimension_case
    is
    --selecting value from the relevant transformation table
        cursor c_case is (select distinct case_key,case_status,case_type,case_source from transformation_case);
    begin
        for row_case in c_case loop
            insert into dim_case(case_key, case_status, case_type, case_source)
            values (row_case.case_key, row_case.case_status, row_case.case_type, row_case.case_source);
        end loop;
    end dimension_case;

    --this procedure loads the data into dimension_time
    procedure dimension_time
    is
    --selecting value from the relevant transformation table
        cursor c_time is (select distinct to_char(reported_date,'yyyy') as YEAR , to_char(reported_date,'Q') as QUARTER, to_char(reported_date,'mm')as MONTH from transformation_case);
    begin
    --insertion of each rows in the transformation table
        for row_time in c_time loop
            insert into dim_time(year, quarter, month)
            values (row_time.year, row_time.quarter, row_time.month);
        end loop;
    end dimension_time;

    --this procedure loads the data into fact_crime
    procedure ins_fact_crime
    is
    --selecting all the required values for the fact table using nested queries
        cursor c_fact is(select ca.case_id, d.officer_id, r.region_id, s.station_id, w.witness_id, t.time_id from 
                        (select cs.case_key, cs.station_id, cs.officer_id, cs.reported_date,st.region_id,cs.case_source, wt.witness_key, cs.case_source as cs_source
                        from transformation_case cs 
                        inner join transformation_station st 
                        on(st.station_key = cs.station_id and st.station_source = cs.case_source)
                        left outer join transformation_witness wt
                        on(wt.case_id = cs.case_key and wt.witness_source = cs.case_source))tt , dim_case ca, dim_officer d, dim_region r, dim_station s, dim_witness w, dim_time t
                        where (tt.case_key = ca.case_key and tt.cs_source = ca.case_source) 
                        and (tt.officer_id = d.officer_key and tt.cs_source=d.officer_source) 
                        and (tt.region_id=r.region_key and tt.cs_source=r.region_source)
                        and (tt.station_id=s.station_key and tt.cs_source=s.station_source)
                        and (tt.witness_key = w.witness_key and tt.cs_source = w.witness_source)
                        and (to_char(tt.reported_date,'yyyy')=t.year and to_char(tt.reported_date,'mm')=t.month));

        --cursor for selecting the measure
        cursor no_crime(c_id number) is
        (select count(case_id) from transformation_case tc,
        (select case_type from dim_case dc where dc.case_id = c_id)ss 
        where ss.case_type =tc.case_type);
        var_case_num integer;
        begin
        --insertion of each rows in the transformation table
            for row_fact in c_fact loop
                open no_crime(row_fact.case_id);
                fetch no_crime into var_case_num;
                insert into fact_crime(region_id, officer_id, case_id, time_id, witness_id, station_id, number_of_crimes_by_crime_type)
                values (row_fact.region_id, row_fact.officer_id, row_fact.case_id, row_fact.time_id, row_fact.witness_id, row_fact.station_id, var_case_num);
                close no_crime;            
            end loop;
        end ins_fact_crime;
end insert_dimension_procedure;