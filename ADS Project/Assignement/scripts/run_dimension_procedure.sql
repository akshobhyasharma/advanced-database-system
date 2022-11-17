--runs all the dimension procedures
begin
    insert_dimension_procedure.dimension_officer;
    insert_dimension_procedure.dimension_station;
    insert_dimension_procedure.dimension_region;
    insert_dimension_procedure.dimension_witness;
    insert_dimension_procedure.dimension_case;
    insert_dimension_procedure.dimension_time;
    insert_dimension_procedure.ins_fact_crime;
end;