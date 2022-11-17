--Package for procedures to alter staging data
create or replace package create_bad_data_package is
    procedure change_data_case(c_id in number, rep_date in date, d_source varchar);
    procedure change_data_officer(o_id in number, o_name in varchar,d_source varchar);
    procedure change_data_witness(w_id in number, w_type in varchar,d_source varchar);
end create_bad_data_package;
/

create or replace package body create_bad_data_package is
    --Changes date of the given case from the given source
    procedure change_data_case(c_id in number, rep_date in date, d_source varchar) IS
        begin
            update stage_case
            set
                reported_date = to_date(rep_date,'mm-dd-yyyy')
            where case_key = c_id and case_source=d_source;
        end;
    --Changes officer name of the given officer_id from the given source
    procedure change_data_officer(o_id in number, o_name in varchar, d_source varchar)IS
        begin
            update stage_officer 
            set
                officer_name = o_name
            where officer_key = o_id and officer_source=d_source;
        end;

    --Changes witness type of the given witness_id from the given source
    procedure change_data_witness(w_id in number, w_type in varchar, d_source varchar)IS
        begin
            update stage_witness
            set
                witness_type = w_type
            where witness_key = w_id and witness_source=d_source;
        end; 
end create_bad_data_package;