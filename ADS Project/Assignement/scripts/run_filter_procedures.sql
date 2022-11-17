--execute all the filter procedures to seperate the data in good and bad table
begin
    filter_procedure_package.filter_stage_officer;
    filter_procedure_package.filter_stage_region;
    filter_procedure_package.filter_stage_case;
    filter_procedure_package.filter_stage_witness;
    filter_procedure_package.filter_stage_station;
end;