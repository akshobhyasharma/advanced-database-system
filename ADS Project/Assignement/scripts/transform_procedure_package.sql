--transforms data from clean tables
create or replace package transform_procedure_package is
    procedure transform_officer;
    procedure transform_region;
    procedure transform_station;
    procedure transform_witness;
    procedure transform_case;
end transform_procedure_package;
/

create or replace package body transform_procedure_package is
    procedure transform_officer
    is
        --selects data from clean table
        cursor c_officer is (select * from clean_officer);
        --checks if the transformation table already has the data
        cursor t_checker(off_key number, off_source varchar) is (select * from transformation_officer t where t.officer_key=off_key  and t.officer_source=off_source);
        type table_transform_officer is table of t_checker%rowtype index by binary_integer;
        v_table_transform_officer table_transform_officer;
    begin
        for row_officer in c_officer loop
            open t_checker(row_officer.officer_key, row_officer.officer_source);
            fetch t_checker bulk collect into v_table_transform_officer;
            --inserts and changes the source value based on source table
                if(v_table_transform_officer.count<1 and row_officer.officer_source='officer') then
                    insert into transformation_officer(officer_key, officer_name, officer_source)
                    values(row_officer.officer_key, upper(row_officer.officer_name), 'ps_wales');
                elsif(v_table_transform_officer.count<1 and row_officer.officer_source='pl_police_employee') then
                    insert into transformation_officer(officer_key, officer_name, officer_source)
                    values(row_officer.officer_key, upper(row_officer.officer_name), 'prcs');
                end if;
            close t_checker;
        end loop;
    end transform_officer;

    procedure transform_region
    is
        --selects data from clean table
        cursor c_region is (select * from clean_region);
        --checks if the transformation table already has the data
        cursor t_checker(reg_key number, reg_source varchar) is (select * from transformation_region t where t.region_key=reg_key and t.region_source = reg_source);
        type table_transform_region is table of t_checker%rowtype;
        v_table_transform_region table_transform_region;
    begin
        for row_region in c_region loop
            open t_checker(row_region.region_key, row_region.region_source);
            fetch t_checker bulk collect into v_table_transform_region;
            --inserts and changes the source value based on source table
                if(v_table_transform_region.count<1 and row_region.region_source ='region') then
                    insert into transformation_region(region_key, region_name,area_name,region_source)
                    values (row_region.region_key, upper(row_region.region_name), upper(row_region.area_name), 'ps_wales');
                elsif(v_table_transform_region.count<1 and row_region.region_source='pl_area') then
                    insert into transformation_region(region_key, region_name,area_name,region_source)
                    values (row_region.region_key, upper(row_region.region_name), upper(row_region.area_name), 'prcs');
                end if;
            close t_checker;
        end loop;
    end transform_region;

    procedure transform_station
    is
        --selects data from clean table
        cursor c_station is (select * from clean_station);
        --checks if the transformation table already has the data
        cursor t_checker (stat_key number, stat_source varchar) is (select * from transformation_station t where t.station_key = stat_key and t.station_source = stat_source);
        type transform_table_station is table of t_checker%rowtype;
        v_transform_table_station transform_table_station;
    begin
        for row_station in c_station loop
            open t_checker (row_station.station_key, row_station.station_source);
            fetch t_checker bulk collect into v_transform_table_station;
            --inserts and changes the source value based on source table
                if(v_transform_table_station.count<1 and row_station.station_source='location') then
                    insert into transformation_station(station_key, station_name, region_id, station_source)
                    values (row_station.station_key, upper(row_station.station_name), row_station.region_id, 'ps_wales');
                elsif(v_transform_table_station.count<1 and row_station.station_source='pl_station') then
                    insert into transformation_station(station_key, station_name, region_id, station_source)
                    values (row_station.station_key, upper(row_station.station_name), row_station.region_id, 'prcs');
                end if;
            close t_checker;
        end loop;
    end transform_station;

    procedure transform_witness
    is
        --selects data from clean table
        cursor c_witness is (select * from clean_witness);
        --checks if the transformation table already has the data
        cursor t_checker (wit_key number, c_id number, wit_source varchar) is 
        (select * from transformation_witness cl
        where ((cl.witness_key=wit_key and cl.case_id=c_id)or(cl.witness_key=wit_key and cl.case_id is null))and cl.witness_source=wit_source);
    type transform_witness_table is table of t_checker%rowtype;
    v_transform_witness_table transform_witness_table;
    begin
        for row_witness in c_witness loop
            open t_checker(row_witness.witness_key, row_witness.case_id, row_witness.witness_source);
            fetch t_checker bulk collect into v_transform_witness_table;
            --inserts and changes the source value based on source table
                if(v_transform_witness_table.count<1 and row_witness.witness_source='crime_register') then
                    insert into transformation_witness(witness_key, case_id, witness_type, witness_source)
                    values (row_witness.witness_key, row_witness.case_id, upper(row_witness.witness_type), 'ps_wales');
                elsif(v_transform_witness_table.count<1 and row_witness.witness_source='pl_witness') then
                    insert into transformation_witness(witness_key, case_id, witness_type, witness_source)
                    values (row_witness.witness_key, row_witness.case_id, upper(row_witness.witness_type), 'prcs');
                end if;
            close t_checker;
        end loop;
    end transform_witness;

    procedure transform_case
    is
        --selects data from clean table
        cursor c_case is (select * from clean_case);
        --checks if the transformation table already has the data
        cursor t_checker (c_key number, emp_id number, c_source varchar) is
        (select * from transformation_case cl
        where((cl.case_key = c_key and cl.officer_id = emp_id)or(cl.case_key = c_key and cl.officer_id is null)) and cl.case_source = c_source);
        type table_transform_case is table of t_checker%rowtype;
        v_table_transform_case table_transform_case;
    begin
        for row_case in c_case loop
            open t_checker(row_case.case_key, row_case.officer_id, row_case.case_source);
            fetch t_checker bulk collect into v_table_transform_case;
            --inserts and changes the source value based on source table
                if(v_table_transform_case.count<1 and row_case.case_source='crime_register') then
                    insert into transformation_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source)
                    values (row_case.case_key, row_case.officer_id, row_case.station_id, upper(row_case.case_status), row_case.reported_date, upper(row_case.case_type), 'ps_wales');
                elsif(v_table_transform_case.count<1 and row_case.case_source='pl_reported_crime') then
                    insert into transformation_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source)
                    values (row_case.case_key, row_case.officer_id, row_case.station_id, upper(row_case.case_status), row_case.reported_date, upper(row_case.case_type), 'prcs');
                end if;
            close t_checker;
        end loop;
    end transform_case;

end transform_procedure_package;
