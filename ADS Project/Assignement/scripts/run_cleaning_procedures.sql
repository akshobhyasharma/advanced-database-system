--runs all the cleaning procedures
begin
    cleaning_procedure_package.clean_bad_station;
    cleaning_procedure_package.clean_bad_region;
    cleaning_procedure_package.clean_bad_officer;
    cleaning_procedure_package.clean_bad_witness;
    cleaning_procedure_package.clean_bad_case;
end;