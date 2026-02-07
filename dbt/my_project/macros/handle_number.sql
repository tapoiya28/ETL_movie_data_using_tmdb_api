{% macro handle_number(num_column) -%}

    case
        when {{num_column}} is null then 0
        when {{num_column}} < 0 then 0
    end
    
{%- endmacro %}