--Collection of procedures to filter data into clean and bad table
create or replace package filter_procedure_package is
    procedure filter_stage_officer;
    procedure filter_stage_region;
    procedure filter_stage_station;
    procedure filter_stage_witness;
    procedure filter_stage_case;
end filter_procedure_package;
/

create or replace package body filter_procedure_package is

    --This procedure filters data from stage_officer
    procedure filter_stage_officer
    is
        --cursor to select data from stage_officer
        cursor c_officer is select * from stage_officer;
        --cursor to check if data is already in bad table
        cursor b_checker(off_key number, off_source varchar) is (select * from bad_officer b where b.officer_key=off_key  and b.officer_source=off_source);
        --cursor to check if data is already in clean table
        cursor c_checker(off_key number, off_source varchar) is (select * from clean_officer cl where cl.officer_key=off_key and cl.officer_source = off_source);
        --declaration of pl/sql table types
        type table_bad_office is table of b_checker%rowtype index by binary_integer;
        type table_good_office is table of c_checker%rowtype index by binary_integer;
        --pl/sql tables to store result returned by queires that the above cursor points to
        v_tab_office_bad table_bad_office;
        v_tab_office_clean table_good_office;
    begin
        for row_officer in c_officer loop
            open b_checker (row_officer.officer_key, row_officer.officer_source);
            open c_checker (row_officer.officer_key, row_officer.officer_source);
            Fetch b_checker bulk collect into v_tab_office_bad;
            Fetch c_checker bulk collect into v_tab_office_clean;
            --filtering conditions
            if ((row_officer.officer_name is NULL  or regexp_like(trim(row_officer.officer_name),'[^a-zA-Z[:space:]]')) and v_tab_office_bad.count<1 and v_tab_office_clean.count<1) then
                insert into bad_officer(officer_key, officer_name, officer_source, data_status) 
                values(row_officer.officer_key, row_officer.officer_name, row_officer.officer_source, 'unresolved');
            elsif(v_tab_office_bad.count<1 and v_tab_office_clean.count<1) then
                insert into clean_officer(officer_key, officer_name, officer_source)
                values(row_officer.officer_key, row_officer.officer_name, row_officer.officer_source);
            end if;
            close b_checker;
            close c_checker;
        end loop;
    end filter_stage_officer;

    --This procedure filters data from stage_region
    procedure filter_stage_region
    is
        --cursor to select data from stage_region
        cursor c_region is select * from stage_region;
        --cursor to check if the data is in the associated bad table
        cursor b_checker(reg_key number, reg_source varchar) is (select * from bad_region b where b.region_key=reg_key and b.region_source = reg_source);
        --cursor to check if the data is in the associated clean table
        cursor c_checker(reg_key number, reg_source varchar) is (select * from clean_region cl where cl.region_key=reg_key and cl.region_source = reg_source);
        --declaration of pl/sql table types
        type table_bad_region is table of b_checker%rowtype index by binary_integer;
        type table_clean_region is table of c_checker%rowtype index by binary_integer;
        --pl/sql tables to store result returned by queires that the above cursor points to
        v_tab_region_clean table_clean_region;
        v_tab_region_bad table_bad_region;
    begin
        for row_region in c_region loop
            open b_checker(row_region.region_key, row_region.region_source);
            open c_checker(row_region.region_key, row_region.region_source);
            fetch b_checker bulk collect into v_tab_region_bad;
            fetch c_checker bulk collect into v_tab_region_clean;
            --filtering conditions
            if((row_region.area_name IS null or regexp_like(row_region.region_name,'[^a-zA-Z[:space:]]')) and v_tab_region_bad.count<1 and v_tab_region_clean.count<1)then
                insert into bad_region(region_key, region_name,area_name,region_source,data_status)
                values (row_region.region_key, row_region.region_name, row_region.area_name, row_region.region_source,'unresolved');
            elsif(v_tab_region_bad.count<1 and v_tab_region_clean.count<1) then
                insert into clean_region(region_key, region_name,area_name,region_source)
                values (row_region.region_key, row_region.region_name, row_region.area_name, row_region.region_source);
            end if;
            close b_checker;
            close c_checker;
        end loop;
    end filter_stage_region;

    --This procedure filters data from stage_station
    procedure filter_stage_station
    is
        cursor c_station is select * from stage_station;
        --cursor to check if the data is in the associated bad table
        cursor b_checker (stat_key number, stat_source varchar) is (select * from bad_station b where b.station_key=stat_key and b.station_source = stat_source);
        --cursor to check if the data is in the associated clean table        
        cursor c_checker (stat_key number, stat_source varchar) is (select * from clean_station cl where cl.station_key = stat_key and cl.station_source = stat_source);
        --cursor to check the repitition of the station name in stage_station
        cursor rep_checker(stat_name varchar) is
        select station_name from(
        select station_name from
        (select upper(station_name) as station_name, count(upper(station_name))as station_count 
        from stage_station 
        group by upper(station_name))
        where station_count >1)s
        where station_name = upper(stat_name)
        ;
        --declaration of pl/sql table types
        type table_bad_station is table of b_checker%rowtype index by binary_integer;
        type table_clean_station is table of c_checker%rowtype index by binary_integer;
        type table_repeated_station is table of rep_checker%rowtype index by binary_integer;
        --pl/sql tables to store result returned by queires that the above cursor points to
        v_table_bad_station table_bad_station;
        v_table_clean_station table_clean_station;
        v_table_repeated_station table_repeated_station;
    begin
        for row_station in c_station loop
            open b_checker(row_station.station_key, row_station.station_source);
            open c_checker(row_station.station_key, row_station.station_source);
            open rep_checker (row_station.station_name);
            fetch b_checker bulk collect into v_table_bad_station;
            fetch c_checker bulk collect into v_table_clean_station;
            fetch rep_checker bulk collect into v_table_repeated_station;
            --filtering conditions
            if ((row_station.station_name is null or v_table_repeated_station.count >0) and v_table_bad_station.count<1 and v_table_clean_station.count<1)then
                insert into bad_station(station_key,station_name,region_id, station_source, data_status)
                values (row_station.station_key, row_station.station_name, row_station.region_id, row_station.station_source, 'unresloved');
            elsif(v_table_bad_station.count<1 and v_table_clean_station.count<1)then
                insert into clean_station(station_key, station_name, region_id, station_source)
                values (row_station.station_key, row_station.station_name, row_station.region_id, row_station.station_source);
            end if;
            close b_checker;
            close c_checker;
            close rep_checker;
        end loop;
    end filter_stage_station;

    --This procedure filters data from stage_witness
    procedure filter_stage_witness
    is
        cursor c_witness is select * from stage_witness;
        --cursor to check if the data is in the associated bad table
        cursor b_checker(wit_key number, c_id number, wit_source varchar) is 
        (select * from bad_witness b 
        where ((b.witness_key=wit_key and b.case_id=c_id)or(b.witness_key=wit_key and b.case_id is null))and b.witness_source=wit_source);
        --cursor to check if the data is in the associated clean table
        cursor c_checker(wit_key number, c_id number, wit_source varchar) is 
        (select * from clean_witness cl
        where ((cl.witness_key=wit_key and cl.case_id=c_id)or(cl.witness_key=wit_key and cl.case_id is null))and cl.witness_source=wit_source);
        --declaration of pl/sql table types
        type table_bad_witness is table of b_checker%rowtype index by binary_integer;
        type table_clean_witness is table of c_checker%rowtype index by binary_integer;
        --pl/sql tables to store result returned by queires that the above cursor points to
        v_table_bad_witness table_bad_witness;
        v_table_clean_witness table_clean_witness;
    begin
        for row_witness in c_witness loop
            open b_checker (row_witness.witness_key, row_witness.case_id, row_witness.witness_source);
            open c_checker (row_witness.witness_key, row_witness.case_id, row_witness.witness_source);
            fetch b_checker bulk collect into v_table_bad_witness;
            fetch c_checker bulk collect into v_table_clean_witness;
            --filtering conditions
                if((row_witness.witness_type is null or regexp_like(row_witness.witness_type,'[^a-zA-Z[:space:]]')) and v_table_bad_witness.count<1 and v_table_clean_witness.count<1) then
                    insert into bad_witness(witness_key, case_id, witness_type, witness_source, data_status)
                    values (row_witness.witness_key, row_witness.case_id, row_witness.witness_type, row_witness.witness_source, 'unresolved');
                elsif(v_table_bad_witness.count<1 and v_table_clean_witness.count<1) then
                    insert into clean_witness(witness_key, case_id, witness_type, witness_source)
                    values (row_witness.witness_key, row_witness.case_id, row_witness.witness_type, row_witness.witness_source);
                end if;
            close b_checker;
            close c_checker;
        end loop;
    end filter_stage_witness;

    --This procedure filters data from stage_case
    procedure filter_stage_case
    is
        cursor c_case is select * from stage_case;
        --cursor to check if the data is in the associated bad table
        cursor b_checker (c_key number, emp_id number, c_source varchar) is
        (select * from bad_case b
        where((b.case_key = c_key and b.officer_id = emp_id)or(b.case_key = c_key and b.officer_id is null)) and b.case_source = c_source);
        --cursor to check if the data is in the associated clean table
        cursor c_checker (c_key number, emp_id number, c_source varchar) is
        (select * from clean_case cl
        where((cl.case_key = c_key and cl.officer_id = emp_id)or(cl.case_key = c_key and cl.officer_id is null)) and cl.case_source = c_source);
        --declaration of pl/sql table types
        type table_bad_case is table of b_checker%rowtype index by binary_integer;
        type table_clean_case is table of c_checker%rowtype index by binary_integer;
        --pl/sql tables to store result returned by queires that the above cursor points to
        v_table_bad_case table_bad_case;
        v_table_clean_case table_clean_case;
    
    begin
        for row_case in c_case loop
            open b_checker(row_case.case_key, row_case.officer_id, row_case.case_source);
            open c_checker(row_case.case_key, row_case.officer_id, row_case.case_source);
            fetch b_checker bulk collect into v_table_bad_case;
            fetch c_checker bulk collect into v_table_clean_case;
                --filtering conditions
                if((row_case.case_type is null or regexp_like(row_case.case_type,'[^a-zA-Z[:space:]]') or row_case.reported_date>trunc(sysdate))and v_table_bad_case.count<1 and v_table_clean_case.count<1) then
                    insert into bad_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source, data_status)
                    values (row_case.case_key, row_case.officer_id, row_case.station_id, row_case.case_status, row_case.reported_date, row_case.case_type, row_case.case_source, 'unresolved');
                elsif(v_table_bad_case.count<1 and v_table_clean_case.count<1) then
                    insert into clean_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source)
                    values (row_case.case_key, row_case.officer_id, row_case.station_id, row_case.case_status, row_case.reported_date, row_case.case_type, row_case.case_source);
                end if;
            close b_checker;
            close c_checker;
        end loop;
    end filter_stage_case;

end filter_procedure_package;
/