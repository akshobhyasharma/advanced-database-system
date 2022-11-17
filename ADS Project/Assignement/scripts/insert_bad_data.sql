--inserting invalid values into the stage_case table
insert all
    into stage_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source)
    values(1001, 1005,22,'closed', to_date('01/25/2025','mm/dd/yyyy'),'v1olent crime','pl_reported_crime')
    into stage_case(case_key, officer_id, station_id, case_status, reported_date, case_type, case_source)
    values(1002, 1005,22,'open', to_date('01/25/2025','mm/dd/yyyy'),'drugs offence','pl_reported_crime')
select 1 from dual;

begin
    --modifying values with change_data_case procedure
    create_bad_data_package.change_data_case(1,'01/25/2024','pl_reported_crime');
    create_bad_data_package.change_data_case(2,'01/25/2030','pl_reported_crime');

    --modifying values with change_data_officer procedure
    create_bad_data_package.change_data_officer(1000,'Ash1ley','pl_police_employee');
    create_bad_data_package.change_data_officer(1004,NULL,'pl_police_employee');

    --modifying values with change_data_witness procedure
    create_bad_data_package.change_data_witness(702,NULL,'pl_witness');
    create_bad_data_package.change_data_witness(703,'Exp3rt W1tness','pl_witness');
end;