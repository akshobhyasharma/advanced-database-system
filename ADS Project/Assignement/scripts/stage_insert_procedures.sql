--Package with the procedures to bring required data to the staging tables--
create or replace package staging_procedure_package is
    procedure insert_to_stage_region;
    procedure insert_to_stage_station;
    procedure insert_to_stage_officer;
    procedure insert_to_stage_witness;
    procedure insert_to_stage_case;
end staging_procedure_package;
/

create or replace package body staging_procedure_package is

    --this procedure brings data into stage_region from multiple data source--
    procedure insert_to_stage_region is
    begin
        --merge statement to bring data from region table--
        merge into stage_region s
        using (select region_id, region_name from region) r
        on (s.region_key = r.region_id and s.region_source = 'region')
        when matched then
            update set 
            s.region_name = r.region_name
        when not matched then
            insert (s.region_key,s.region_name,s.region_source)
            values (r.region_id,r.region_name,'region');
        --merge statement to bring data from stage table--
        merge into stage_region s
        using ( select area2.area_id as area_id, area1.area_name as region,area2.area_name as area 
                from pl_area area1, pl_area area2  
                where 
                area1.area_id = area2.parent_area 
                and area1.parent_area is not null
                union
                select area1.area_id as area_id, area1.area_name as region, null as area
                from pl_area area1
                where
                area1.parent_area<2
                and area1.parent_area is not null)r
        on(s.region_key = r.area_id and s.region_source='pl_area')
        when matched then
            update set
            s.region_name = r.region,
            s.area_name = r.area
        when not matched then
            insert (s.region_key,s.region_name,s.area_name,s.region_source)
            values (r.area_id, r.region, r.area, 'pl_area');
    end insert_to_stage_region;

    --procedure to bring data into stage_station--
    procedure insert_to_stage_station is
    begin
        --merges data from pl_station table--
        merge into stage_station s
        using (select station_id, station_name, fk1_area_id as area_id from pl_station) os
        on (s.station_key = os.station_id and station_source='pl_station')
        when matched then
            update set
            s.station_name = os.station_name,
            s.region_id = os.area_id
        when not matched then
            insert(s.station_key,s.station_name,s.region_id,s.station_source)
            values(os.station_id,os.station_name,os.area_id,'pl_station');

        --merges data from location table--
        merge into stage_station s
        using (select location_id, null as station_name, region_id from location) os
        on (s.station_key = os.location_id and station_source = 'location')
        when matched then
            update set
            s.station_name = os.station_name,
            s.region_id = os.region_id
        when not matched then
            insert(s.station_key, s.station_name, s.region_id, s.station_source)
            values(os.location_id, os.station_name, os.region_id, 'location');
    end insert_to_stage_station;       

    --procedure to insert data into stage_officer table--
    procedure insert_to_stage_officer is
    begin
        --merges data from pl_polic_employee--
        merge into stage_officer s
        using (select emp_id, emp_name from pl_police_employee) o
        on(s.officer_key = o.emp_id and s.officer_source='pl_police_employee')
        when matched then
            update set
            s.officer_name = o.emp_name
        when not matched then
            insert(s.officer_key, s.officer_name, s.officer_source)
            values(o.emp_id, o.emp_name, 'pl_police_employee');

        --merges data from officer table--
        merge into stage_officer s
        using (select officer_id, first_name||' '||middle_name||' '||last_name as officer_name from officer) o
        on(s.officer_key = o.officer_id and s.officer_source='officer')
        when matched then
            update set
            s.officer_name = o.officer_name
        when not matched then
            insert(s.officer_key, s.officer_name, s.officer_source)
            values(o.officer_id, o.officer_name, 'officer');        
    end insert_to_stage_officer;

    --procedure to insert data to stage_witness--
    procedure insert_to_stage_witness is
    begin
        --merges data from crime_register--
        merge into stage_witness w
        using (select crime_id as case_id, reporter_id as witness_id, null as witness_type from crime_register)cr
        on ((cr.witness_id = w.witness_key and cr.case_id = w.case_id) and w.witness_source = 'crime_register')
        when matched then
            update set
            w.witness_type = cr.witness_type
        when not matched then
            insert(w.witness_key, w.case_id, w.witness_type, w.witness_source)
            values(cr.witness_id, cr.case_id, cr.witness_type, 'crime_register');

        --merges data from pl_witness table--
        merge into stage_witness w
        using(select witness_id, witness_type_desc as witness_type, s.s_reported_crime_id as case_id
              from  
                (select w.witness_id, wt.witness_type_desc 
                from 
                pl_witness w 
                left outer join pl_witness_type wt 
                on w.witness_type_id = wt.witness_type_id)nw
                left outer join pl_statement s on nw.witness_id=s.d_witness_id)cr
        on (((w.witness_key = cr.witness_id and cr.case_id = w.case_id)or(w.witness_key = cr.witness_id and w.case_id IS null)) and w.witness_source='pl_witness')
        when matched then
            update set
            w.witness_type = cr.witness_type
        when not matched then
            insert(w.witness_key, w.case_id, w.witness_type, w.witness_source)
            values (cr.witness_id, cr.case_id, cr.witness_type, 'pl_witness');
    end insert_to_stage_witness;

    --procedure to insert data into stage_case--
    procedure insert_to_stage_case is
    begin
        --merges data from pl_reported_crime--
        merge into stage_case c
        using (select reported_crime_id, crime_status, date_reported, crime_type_desc, fk2_station_id, d_emp_id 
        from pl_reported_crime rc
        left outer join pl_work_allocation p on 
        rc.reported_crime_id = p.s_reported_crime_id
        inner join pl_crime_type cr on
        rc.fk1_crime_type_id = cr.crime_type_id
        )cc
        on (((cc.reported_crime_id=c.case_key and cc.d_emp_id=c.officer_id )or(cc.reported_crime_id=c.case_key and cc.d_emp_id is null)) and case_source='pl_reported_crime')
        when matched then
            update set
            c.case_status = cc.crime_status,
            c.station_id = cc.fk2_station_id,
            c.case_type = cc.crime_type_desc,
            c.reported_date = cc.date_reported
        when not matched then
            insert(c.case_key,c.officer_id,c.station_id,c.case_status,c.case_type,c.case_source,c.reported_date)
            values(cc.reported_crime_id, cc.d_emp_id, cc.fk2_station_id, cc.crime_status, cc.crime_type_desc, 'pl_reported_crime',cc.date_reported);

        --merges data from stage_case--
        merge into stage_case c
        using (select crime_id, location_id as station_id, crime_name as case_type, police_id, crime_status, reported_date from crime_register)cc
        on(c.case_id =cc.crime_id and case_source = 'crime_register')
        when matched then
            update set
            c.officer_id = cc.police_id,
            c.station_id = cc.station_id,
            c.case_status = cc.crime_status,
            c.case_type = cc.case_type,
            c.reported_date = cc.reported_date
        when not matched then
            insert(c.case_key,c.officer_id,c.station_id,c.case_status,c.case_type,c.case_source,c.reported_date)
            values(cc.crime_id,cc.police_id, cc.station_id, cc.crime_status, cc.case_type, 'crime_register', cc.reported_date);
    end insert_to_stage_case;
end staging_procedure_package;
/
