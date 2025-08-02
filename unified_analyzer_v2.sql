-- ============================================================================
-- Unified SQL Analyzer v2 for DuckDB Parser Tools Extension
-- ============================================================================
-- Combines function, table, and column parsing using a practical approach
-- that works around DuckDB's table function limitations

-- Load the extension
LOAD parser_tools;

-- Helper functions for existence checking
CREATE OR REPLACE MACRO function_exists(func_name) AS (
    func_name = ANY(
        SELECT DISTINCT function_name 
        FROM duckdb_functions() 
        WHERE function_name <> '%' AND length(function_name) > 2
    )
);

CREATE OR REPLACE MACRO function_exists_in_schema(func_name, target_schema) AS (
    EXISTS(
        SELECT 1 FROM duckdb_functions() 
        WHERE function_name = func_name 
          AND schema_name = target_schema
    )
);

CREATE OR REPLACE MACRO table_exists(obj_name) AS (
    EXISTS(SELECT 1 FROM duckdb_tables() WHERE table_name = obj_name AND NOT internal) OR
    EXISTS(SELECT 1 FROM duckdb_views() WHERE view_name = obj_name AND NOT internal)
);

CREATE OR REPLACE MACRO table_exists_in_schema(obj_name, target_schema) AS (
    EXISTS(SELECT 1 FROM duckdb_tables() WHERE table_name = obj_name AND schema_name = target_schema AND NOT internal) OR
    EXISTS(SELECT 1 FROM duckdb_views() WHERE view_name = obj_name AND schema_name = target_schema AND NOT internal)
);

CREATE OR REPLACE MACRO get_object_type(obj_name, target_schema) AS (
    CASE 
        WHEN EXISTS(SELECT 1 FROM duckdb_tables() WHERE table_name = obj_name AND schema_name = target_schema AND NOT internal)
        THEN 'table'
        WHEN EXISTS(SELECT 1 FROM duckdb_views() WHERE view_name = obj_name AND schema_name = target_schema AND NOT internal)
        THEN 'view'
        ELSE 'unknown'
    END
);

-- Column existence checking helpers
CREATE OR REPLACE MACRO column_exists_in_table(col_name, tbl_name, target_schema) AS (
    EXISTS(
        SELECT 1 FROM duckdb_columns() 
        WHERE column_name = col_name 
          AND table_name = tbl_name 
          AND schema_name = target_schema
    )
);

-- Suggestion functions
CREATE OR REPLACE MACRO suggest_functions(func_name) AS (
    list_slice(
        list_filter(
            (SELECT list(DISTINCT function_name ORDER BY function_name) 
             FROM duckdb_functions() 
             WHERE function_name <> '%' AND length(function_name) > 2),
            f -> levenshtein(f, func_name) <= 2 AND length(f) >= 3
        ),
        1, 3
    )
);

CREATE OR REPLACE MACRO suggest_tables(obj_name) AS (
    list_slice(
        list_filter(
            (SELECT list(DISTINCT table_name ORDER BY table_name) 
             FROM duckdb_tables() 
             WHERE NOT internal),
            t -> levenshtein(t, obj_name) <= 2 AND length(t) >= 3
        ) ||
        list_filter(
            (SELECT list(DISTINCT view_name ORDER BY view_name) 
             FROM duckdb_views() 
             WHERE NOT internal),
            v -> levenshtein(v, obj_name) <= 2 AND length(v) >= 3
        ),
        1, 3
    )
);

CREATE OR REPLACE MACRO suggest_columns(col_name) AS (
    list_slice(
        list_filter(
            (SELECT list(DISTINCT column_name ORDER BY column_name) 
             FROM duckdb_columns()),
            c -> levenshtein(c, col_name) <= 2 AND length(c) >= 3
        ),
        1, 3
    )
);

-- Schema-aware suggestions
CREATE OR REPLACE MACRO suggest_function_with_schema(func_name) AS (
    (SELECT list(DISTINCT schema_name || '.' || function_name) 
     FROM duckdb_functions() 
     WHERE function_name = func_name)
);

CREATE OR REPLACE MACRO suggest_table_with_schema(obj_name) AS (
    COALESCE(
        (SELECT list(DISTINCT schema_name || '.' || table_name) 
         FROM duckdb_tables() 
         WHERE table_name = obj_name AND NOT internal), 
        []
    ) ||
    COALESCE(
        (SELECT list(DISTINCT schema_name || '.' || view_name) 
         FROM duckdb_views() 
         WHERE view_name = obj_name AND NOT internal), 
        []
    )
);

-- ============================================================================
-- Analysis Functions
-- ============================================================================

-- Function to analyze a SQL query and return comprehensive results
-- Usage: SELECT * FROM analyze_sql_comprehensive('your_sql_query_here');

CREATE OR REPLACE FUNCTION analyze_sql_comprehensive(sql_query) AS TABLE 
SELECT * FROM (
    -- Analyze functions
    SELECT 
        'function' as type,
        function_name as name,
        schema,
        context,
        CASE 
            WHEN function_exists_in_schema(function_name, schema) THEN '✅ Found'
            WHEN function_exists(function_name) THEN '⚠️ Wrong schema'  
            ELSE '❌ Missing'
        END as status,
        CASE 
            WHEN function_exists(function_name) AND NOT function_exists_in_schema(function_name, schema) THEN 
                '💡 Available as: ' || array_to_string(suggest_function_with_schema(function_name), ', ')
            WHEN len(suggest_functions(function_name)) > 0 AND NOT function_exists_in_schema(function_name, schema) THEN 
                '🔍 Similar: ' || array_to_string(suggest_functions(function_name), ', ')
            ELSE NULL
        END as suggestions_text,
        'Function call in ' || context as details
    FROM parse_functions(sql_query)
    
    UNION ALL
    
    -- Analyze tables
    SELECT 
        get_object_type("table", schema) as type,
        "table" as name,
        schema,
        context,
        CASE 
            WHEN table_exists_in_schema("table", schema) THEN '✅ Found'
            WHEN table_exists("table") THEN '⚠️ Wrong schema'  
            ELSE '❌ Missing'
        END as status,
        CASE 
            WHEN table_exists("table") AND NOT table_exists_in_schema("table", schema) THEN 
                '💡 Available as: ' || array_to_string(suggest_table_with_schema("table"), ', ')
            WHEN len(suggest_tables("table")) > 0 AND NOT table_exists_in_schema("table", schema) THEN 
                '🔍 Similar: ' || array_to_string(suggest_tables("table"), ', ')
            ELSE NULL
        END as suggestions_text,
        'Table/view reference in ' || context as details
    FROM parse_tables(sql_query)
    
    UNION ALL
    
    -- Analyze columns (input columns only)
    SELECT 
        'column' as type,
        COALESCE(column_name, 'complex_expression') as name,
        COALESCE(table_schema, 'unknown') as schema,
        context,
        CASE 
            WHEN column_name IS NOT NULL AND table_name IS NOT NULL AND table_schema IS NOT NULL 
                 AND column_exists_in_table(column_name, table_name, table_schema) THEN '✅ Found'
            WHEN column_name IS NOT NULL 
                 AND EXISTS(SELECT 1 FROM duckdb_columns() WHERE column_name = c.column_name) THEN '⚠️ Different table'
            WHEN column_name IS NOT NULL THEN '❌ Missing'
            ELSE '📋 Expression'
        END as status,
        CASE 
            WHEN column_name IS NOT NULL 
                 AND EXISTS(SELECT 1 FROM duckdb_columns() WHERE column_name = c.column_name)
                 AND NOT (table_name IS NOT NULL AND table_schema IS NOT NULL 
                         AND column_exists_in_table(column_name, table_name, table_schema)) THEN 
                '💡 Available in other tables'
            WHEN column_name IS NOT NULL AND len(suggest_columns(column_name)) > 0 THEN 
                '🔍 Similar: ' || array_to_string(suggest_columns(column_name), ', ')
            ELSE NULL
        END as suggestions_text,
        CASE 
            WHEN selected_name IS NOT NULL THEN 'Output column: ' || selected_name
            WHEN column_name IS NOT NULL THEN 'Input column in ' || context
            ELSE 'Complex expression in ' || context
        END as details
    FROM parse_columns(sql_query) c
    WHERE selected_name IS NULL  -- Only input columns for main analysis
) 
ORDER BY 
    CASE type WHEN 'function' THEN 1 WHEN 'table' THEN 2 WHEN 'column' THEN 3 ELSE 4 END,
    status,
    name;

-- ============================================================================
-- Example Usage
-- ============================================================================

/*
-- Test with a complex query that has functions, tables, and columns
SELECT * FROM analyze_sql_comprehensive('
    SELECT 
        upper(u.name) as user_name,
        lenght(u.email) as email_len,
        fake_func(u.id) as processed_id,
        u.missing_column,
        o.total
    FROM users u 
    JOIN orders o ON u.id = o.user_id
    WHERE u.status = ''active'' AND u.age > 18
    ORDER BY u.created_at
');

-- Or analyze each component separately:
SELECT 'Functions:' as analysis_type;
SELECT * FROM parse_functions('SELECT upper(name), lenght(email) FROM users');

SELECT 'Tables:' as analysis_type;  
SELECT * FROM parse_tables('SELECT name FROM users JOIN orders ON users.id = orders.user_id');

SELECT 'Columns:' as analysis_type;
SELECT * FROM parse_columns('SELECT name, missing_col FROM users WHERE age > 18');
*/

-- Demo query
SELECT '=== Comprehensive SQL Analysis Demo ===' as demo_section;

-- Create demo tables
CREATE TABLE IF NOT EXISTS demo_users (id INT, name VARCHAR, email VARCHAR, age INT, status VARCHAR);
CREATE TABLE IF NOT EXISTS demo_orders (id INT, user_id INT, total DECIMAL, status VARCHAR);

-- Run comprehensive analysis on a complex query
SELECT * FROM analyze_sql_comprehensive('
    SELECT 
        upper(u.name) as user_name,
        lenght(u.email) as email_len,
        fake_func(u.id) as processed_id,
        u.missing_column,
        o.total
    FROM demo_users u 
    JOIN demo_orders o ON u.id = o.user_id
    WHERE u.status = ''active'' AND u.age > 18
    ORDER BY u.created_at
');

-- Cleanup demo tables
DROP TABLE IF EXISTS demo_users;
DROP TABLE IF EXISTS demo_orders;