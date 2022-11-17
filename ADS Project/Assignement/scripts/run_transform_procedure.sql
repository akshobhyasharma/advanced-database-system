--runs all the transformation procedures
begin
    transform_procedure_package.transform_officer;
    transform_procedure_package.transform_station;
    transform_procedure_package.transform_region;
    transform_procedure_package.transform_witness;
    transform_procedure_package.transform_case;
end;