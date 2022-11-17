--script to execute the staging procedures--
begin
    staging_procedure_package.insert_to_stage_case;
    staging_procedure_package.insert_to_stage_officer;
    staging_procedure_package.insert_to_stage_region;
    staging_procedure_package.insert_to_stage_witness;
    staging_procedure_package.insert_to_stage_station;
end;