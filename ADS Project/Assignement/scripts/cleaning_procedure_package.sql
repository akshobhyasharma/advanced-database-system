--this package contains the procedures that cleans bad data to good data
create or replace package cleaning_procedure_package is
    procedure clean_bad_station;
    procedure clean_bad_region;
    procedure clean_bad_officer;
    procedure clean_bad_witness;
    procedure clean_bad_case;
end cleaning_procedure_package;
/

create or replace package body cleaning_procedure_package is

    --procedure to clean bad officer
    procedure clean_bad_officer
    is
        --cursor to read bad table
        cursor b_officer is (select * from bad_officer);
        --cursor to check if the data is already inserted in clean table
        cursor c_checker(off_key number, off_source varchar) is (select * from clean_officer cl where cl.officer_key=off_key and cl.officer_source = off_source);
        --pl/sql table to store query value pointed by above cursor
        type table_clean_officer is table of c_checker%rowtype index by binary_integer;
        v_table_clean_officer table_clean_officer;
        --variable to store clean value
        org_officer_name varchar2(100);    
    begin
        for row_officer in b_officer loop
            open c_checker (row_officer.officer_key, row_officer.officer_source);
            fetch c_checker bulk collect into v_table_clean_officer;
            --using implicit cursor to retrieve original data based on table
            if(row_officer.officer_source = 'pl_police_employee') then
                select emp_name into org_officer_name from pl_police_employee where emp_id = row_officer.officer_key;
            elsif(row_officer.officer_source = 'officer') then
                select first_name||' '||middle_name||' '||last_name as officer_name into org_officer_name from officer where officer_id = row_officer.officer_key;
            end if;
            --various conditions are checked before inserting the value in clean table
            if((row_officer.officer_name is null or regexp_like(row_officer.officer_name,'[^a-zA-Z[:space:]]')) and (org_officer_name is not null) and 
            (not regexp_like(org_officer_name,'[^a-zA-Z[:space:]]')) and v_table_clean_officer.count<1) then
                insert into clean_officer(officer_key, officer_name, officer_source)
                values(row_officer.officer_key, org_officer_name, row_officer.officer_source);
                update bad_officer set data_status = 'resolved' where bad_officer.officer_key = row_officer.officer_key;
            elsif(v_table_clean_officer.count<1) then
                insert into clean_officer(officer_key, officer_name, officer_source)
                values(row_officer.officer_key, 'N/A', row_officer.officer_source);
                update bad_officer set data_status = 'resolved' where bad_officer.officer_key = row_officer.officer_key;
            end if;

            close c_checker;
        end loop;
    end clean_bad_officer;

    --procedure to clean bad region
    procedure clean_bad_region
    is
        --cursor to read bad table
        cursor b_region is (select * from bad_region);
        --cursor to check if the data is already inserted in clean table
        cursor c_checker(reg_key number, reg_source varchar) is (select * from clean_region cl where cl.region_key=reg_key and cl.region_source = reg_source);
        --pl/sql table to store query value pointed by above cursor
        type table_clean_region is table of c_checker%rowtype index by binary_integer;
        v_table_clean_region table_clean_region;
        new_region varchar2(50);
        new_area varchar2(50);
    begin
        for row_region in b_region loop
            open c_checker(row_region.region_key, row_region.region_source);
            fetch c_checker bulk collect into v_table_clean_region;
            new_region := row_region.region_name;
            new_area := row_region.area_name;
            --multiple ifs assigns to check each of the data inconsistencies
            if(row_region.area_name is null) then
                new_area:='N/A';
            end if;
            if(regexp_like(row_region.region_name,'[^a-zA-Z[:space:]]'))then
                new_region := replace(row_region.region_name,', ',' ');
            end if;
            if(v_table_clean_region.count<1) then
                insert into clean_region(region_key, region_name,area_name,region_source)
                values (row_region.region_key, new_region, new_area, row_region.region_source);
                update bad_region set data_status = 'resolved' where bad_region.region_key = row_region.region_key;
            end if;
            close c_checker;
        end loop;
    end clean_bad_region;

    --procedure to clean bad station
    procedure clean_bad_station
    is
        --cursor to read bad table
        cursor b_station is (select * from bad_station);
        --cursor to check if the data is already inserted in clean table
        cursor c_checker (stat_key number, stat_source varchar) is (select * from clean_station cl where cl.station_key = stat_key and cl.station_source = stat_source);
        --pl/sql table to store query value pointed by above cursor
        type table_clean_station is table of c_checker%rowtype;
        type station_id_type is table of integer(4);
        station_id station_id_type;
        v_table_clean_station table_clean_station;
    begin
        station_id := station_id_type(20,22,24);
        for row_station in b_station loop
            open c_checker(row_station.station_key, row_station.station_source);
            fetch c_checker bulk collect into v_table_clean_station;
                --purifying values from pl_station
                if(row_station.station_source = 'pl_station' and row_station.station_key member of station_id and v_table_clean_station.count<1) then
                    insert into clean_station(station_key, station_name, region_id, station_source)
                    values (row_station.station_key, row_station.station_name, row_station.region_id, row_station.station_source);
                    update bad_station set data_status='resolved' where station_key = row_station.station_key;
                elsif(row_station.station_source = 'pl_station') then
                    update bad_station set data_status='resolved' where station_key = row_station.station_key;
                end if;
                --purifying values from location
                if(row_station.station_source = 'location' and row_station.station_name is null and v_table_clean_station.count<1) then
                    insert into clean_station(station_key, station_name, region_id, station_source)
                    values (row_station.station_key, 'N/A', row_station.region_id, row_station.station_source);
                    update bad_station set data_status='resolved' where station_key = row_station.station_key;
                end if;
            close c_checker;
        end loop;
    end clean_bad_station;

    --procedure to clean bad witness
    procedure clean_bad_witness
    is
        --cursor to read bad table
        cursor b_witness is (select * from bad_witness);
        --cursor to check if the data is already inserted in clean table
        cursor c_checker(wit_key number, c_id number, wit_source varchar) is 
            (select * from clean_witness cl
            where ((cl.witness_key=wit_key and cl.case_id=c_id)or(cl.witness_key=wit_key and cl.case_id is null))and cl.witness_source=wit_source);
        --cursor to check original witness description
        cursor original_checker(wit_id number) is
            (select distinct witness_type_desc from pl_witness p, pl_witness_type pl where p.witness_id = wit_id and p.witness_type_id = pl.witness_type_id);
        --pl/sql table to store query value pointed by above cursor
        type table_clean_witness is table of c_checker%rowtype index by binary_integer;
        v_table_clean_witness table_clean_witness;
        org_witness_type varchar2(40);
    begin
        for row_witness in b_witness loop
            open c_checker(row_witness.witness_key, row_witness.case_id, row_witness.witness_source);
            fetch c_checker bulk collect into v_table_clean_witness;
                --only pl_witness table is checked because crime_register lacks the data
                if(row_witness.witness_source = 'pl_witness') then
                    open original_checker(row_witness.witness_key);
                    fetch original_checker into org_witness_type;
                    close original_checker;
                elsif(row_witness.witness_source = 'crime_register') then
                    org_witness_type := 'unknown';
                end if;
                --further condition checks before inserting the data
                if((row_witness.witness_type is null or regexp_like(row_witness.witness_type,'[^a-zA-Z[:space:]]')) and (org_witness_type is not null) and 
                (not regexp_like(org_witness_type,'[^a-zA-Z[:space:]]'))and v_table_clean_witness.count<1) then
                    insert into clean_witness(witness_key, case_id, witness_type, witness_source)
                    values (row_witness.witness_key, row_witness.case_id, org_witness_type, row_witness.witness_source);
                    update bad_witness set data_status ='resolved' where ((bad_witness.witness_key = row_witness.witness_key and bad_witness.case_id = row_witness.case_id)or
                    (bad_witness.witness_key = row_witness.witness_key)) and bad_witness.witness_source=row_witness.witness_source;
                elsif(v_table_clean_witness.count<1) then
                    insert into clean_witness(witness_key, case_id, witness_type, witness_source)
                    values (row_witness.witness_key, row_witness.case_id, 'unknown', row_witness.witness_source);
                    update bad_witness set data_status ='resolved' where ((bad_witness.witness_key = row_witness.witness_key and bad_witness.case_id = row_witness.case_id)or
                    (bad_witness.witness_key = row_witness.witness_key)) and bad_witness.witness_source=row_witness.witness_source;
                end if;
            close c_checker;
        end loop;
    end clean_bad_witness;

--procedure to clean bad case
procedure clean_bad_case
is
    --cursor to read bad table
    cursor b_case is (select * from bad_case);
    --cursor to check if the data is already inserted in clean table
    cursor c_checker (c_key number, emp_id number, c_source varchar) is
        (select * from clean_case cl
        where((cl.case_key = c_key and cl.officer_id = emp_id)or(cl.case_key = c_key and cl.officer_id is null)) and cl.case_source = c_source);
    --cursor to check original date and crime type
    cursor original_checker(c_id number) is
        (select date_reported,crime_type_desc from pl_reported_crime p, pl_crime_type c where p.reported_crime_id = c_id and p.fk1_crime_type_id=c.crime_type_id);
    --pl/sql table to store query value pointed by above cursor
    type clean_case_table is table of c_checker%rowtype index by binary_integer;
    v_clean_case_table clean_case_table;
    original_date date;
    original_case_type varchar2(30);
begin
    for row_case in b_case loop
        open c_checker(row_case.case_key, row_case.officer_id, row_case.case_source);
        fetch c_checker bulk collect into v_clean_case_table;
            --checking values based on original tables
            if(row_case.case_source='pl_reported_crime') then
                open original_checker (row_case.case_key);
                fetch original_checker into original_date, original_case_type;
                close original_checker;
            elsif(row_case.case_source='crime_register') then
                original_date :=row_case.reported_date;
                original_case_type :='unknown';
            end if;
            --checking for bad case_type
            if(row_case.case_type is null or regexp_like(row_case.case_type,'[^a-zA-Z[:space:]]')) then
                if(original_case_type is null or regexp_like(original_case_type,'[^a-zA-Z[:space:]]'))then
                    original_case_type := 'unknown';
                end if;
            end if;
            --checking for bad reported_date
            if(row_case.reported_date>trunc(sysdate))then
                if(original_date>trunc(sysdate) or original_date is null)then
                    original_date := to_char(sysdate, 'mm-dd-yyyy');
                end if;
            end if;
            --inserting into clean after checking
            if (v_clean_case_table.count <1) then
                insert into clean_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source)
                values (row_case.case_key, row_case.officer_id, row_case.station_id, row_case.case_status, original_date, original_case_type, row_case.case_source);
                update bad_case set data_status='resolved' 
                where (((bad_case.case_key=row_case.case_key and bad_case.officer_id=row_case.officer_id )or(bad_case.case_key=row_case.case_key and bad_case.officer_id is null))
                and case_source=row_case.case_source);
            end if;
        close c_checker;
    end loop;
end clean_bad_case;

end cleaning_procedure_package;
/