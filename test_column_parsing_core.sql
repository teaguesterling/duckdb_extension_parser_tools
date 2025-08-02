-- Core column parsing tests
-- This file tests the essential functionality without dependencies on complex table structures

-- Test 1: Basic unqualified columns
SELECT 'Test 1: Basic columns' as test_name;
SELECT * FROM parse_columns('SELECT name, age FROM users');

-- Test 2: Schema-qualified columns  
SELECT 'Test 2: Schema-qualified' as test_name;
SELECT * FROM parse_columns('SELECT main.users.name FROM main.users');

-- Test 3: Alias chain (our key scenario)
SELECT 'Test 3: Alias chains' as test_name;
SELECT * FROM parse_columns('SELECT 1 AS a, users.age AS b, a+b AS c, b AS d FROM users');

-- Test 4: Complex expression with multiple identifiers
SELECT 'Test 4: Complex expressions' as test_name;
SELECT * FROM parse_columns('SELECT u.name, o.total, u.age + o.total AS summary FROM users u JOIN orders o ON u.id = o.user_id');

-- Test 5: WHERE clause columns
SELECT 'Test 5: WHERE clause' as test_name;
SELECT * FROM parse_columns('SELECT name FROM users WHERE age > 18 AND email LIKE ''test''');

-- Test 6: Function arguments
SELECT 'Test 6: Function arguments' as test_name;
SELECT * FROM parse_columns('SELECT UPPER(name), CONCAT(first_name, last_name) FROM users');

-- Test 7: Nested struct field (simulated)
SELECT 'Test 7: Nested struct' as test_name;
SELECT * FROM parse_columns('SELECT users.profile.address.city FROM users');

-- Test 8: Output validation - check NULL handling
SELECT 'Test 8: NULL handling verification' as test_name;
SELECT 
    CASE WHEN table_schema IS NULL THEN 'NULL' ELSE table_schema END as schema_check,
    CASE WHEN table_name IS NULL THEN 'NULL' ELSE table_name END as table_check,
    CASE WHEN selected_name IS NULL THEN 'NULL' ELSE selected_name END as selected_check
FROM parse_columns('SELECT 1 AS a, users.age AS b FROM users')
LIMIT 3;