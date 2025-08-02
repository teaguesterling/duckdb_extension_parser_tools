-- Test suite for column parsing functionality
-- Load the extension first
LOAD parser_tools;

-- Create test tables
CREATE TABLE users (id INT, age INT, name VARCHAR, email VARCHAR);
CREATE TABLE orders (id INT, user_id INT, total DECIMAL, status VARCHAR);
CREATE TABLE profiles (user_id INT, first_name VARCHAR, last_name VARCHAR, address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR));

-- Test 1: Basic column parsing
SELECT '=== Test 1: Basic column parsing ===' as test_section;
SELECT * FROM parse_columns('SELECT name, age FROM users');

-- Test 2: Schema-qualified columns
SELECT '=== Test 2: Schema-qualified columns ===' as test_section;
SELECT * FROM parse_columns('SELECT main.users.name, main.users.age FROM main.users');

-- Test 3: Complex expressions with multiple columns
SELECT '=== Test 3: Complex expressions ===' as test_section;
SELECT * FROM parse_columns('SELECT u.name, o.total, u.age + o.total AS summary FROM users u JOIN orders o ON u.id = o.user_id');

-- Test 4: Alias chain scenario (from our discussion)
SELECT '=== Test 4: Alias chains ===' as test_section;
SELECT * FROM parse_columns('SELECT 1 AS a, users.age AS b, a+b AS c, b AS d FROM users');

-- Test 5: Nested struct field access
SELECT '=== Test 5: Nested struct fields ===' as test_section;
SELECT * FROM parse_columns('SELECT profiles.address.street, profiles.address.city FROM profiles');

-- Test 6: Deeply nested struct with schema qualification
SELECT '=== Test 6: Deep nested struct with schema ===' as test_section;
SELECT * FROM parse_columns('SELECT main.profiles.address.city FROM main.profiles');

-- Test 7: WHERE clause columns
SELECT '=== Test 7: WHERE clause columns ===' as test_section;
SELECT * FROM parse_columns('SELECT name FROM users WHERE age > 18 AND email LIKE ''%@gmail.com''');

-- Test 8: GROUP BY and HAVING columns
SELECT '=== Test 8: GROUP BY and HAVING columns ===' as test_section;
SELECT * FROM parse_columns('SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5');

-- Test 9: ORDER BY columns
SELECT '=== Test 9: ORDER BY columns ===' as test_section;
SELECT * FROM parse_columns('SELECT name, age FROM users ORDER BY age DESC, name ASC');

-- Test 10: Function arguments with columns
SELECT '=== Test 10: Function arguments ===' as test_section;
SELECT * FROM parse_columns('SELECT UPPER(name), LENGTH(email), CONCAT(first_name, '' '', last_name) FROM users');

-- Test 11: Window functions
SELECT '=== Test 11: Window functions ===' as test_section;
SELECT * FROM parse_columns('SELECT name, ROW_NUMBER() OVER (PARTITION BY age ORDER BY name) FROM users');

-- Test 12: Complex query with joins, subqueries, and functions
SELECT '=== Test 12: Complex query ===' as test_section;
SELECT * FROM parse_columns('
    WITH user_stats AS (
        SELECT u.id, u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.name
    )
    SELECT 
        us.name,
        us.order_count,
        CASE 
            WHEN us.order_count > 5 THEN ''high''
            WHEN us.order_count > 1 THEN ''medium''
            ELSE ''low''
        END as activity_level
    FROM user_stats us
    WHERE us.order_count > 0
    ORDER BY us.order_count DESC
');

-- Test 13: Unqualified columns (aliases, literals)
SELECT '=== Test 13: Unqualified columns and literals ===' as test_section;
SELECT * FROM parse_columns('SELECT 42 AS answer, ''hello'' AS greeting, name FROM users');

-- Test 14: Mixed qualified and unqualified references
SELECT '=== Test 14: Mixed qualifications ===' as test_section;
SELECT * FROM parse_columns('SELECT users.name, age, profiles.first_name FROM users JOIN profiles ON users.id = profiles.user_id');

-- Test 15: CASE expressions with columns
SELECT '=== Test 15: CASE expressions ===' as test_section;
SELECT * FROM parse_columns('
    SELECT 
        name,
        CASE 
            WHEN age < 18 THEN ''minor''
            WHEN age < 65 THEN ''adult''
            ELSE ''senior''
        END as age_group
    FROM users
');

-- Test 16: Subquery column references
SELECT '=== Test 16: Subquery columns ===' as test_section;
SELECT * FROM parse_columns('
    SELECT name, age 
    FROM users 
    WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
');

-- Test 17: JOIN conditions
SELECT '=== Test 17: JOIN conditions ===' as test_section;
SELECT * FROM parse_columns('
    SELECT u.name, o.total
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id AND u.age > 18
');

-- Test 18: Multiple table aliases with same column names
SELECT '=== Test 18: Multiple aliases, same column names ===' as test_section;
SELECT * FROM parse_columns('
    SELECT u.id as user_id, o.id as order_id, u.name, o.status
    FROM users u
    JOIN orders o ON u.id = o.user_id
');

-- Test 19: Column references in aggregates
SELECT '=== Test 19: Aggregates with columns ===' as test_section;
SELECT * FROM parse_columns('
    SELECT 
        COUNT(DISTINCT u.id) as unique_users,
        AVG(o.total) as avg_order,
        SUM(o.total) as total_revenue
    FROM users u
    JOIN orders o ON u.id = o.user_id
');

-- Test 20: Column with arithmetic operations
SELECT '=== Test 20: Arithmetic operations ===' as test_section;
SELECT * FROM parse_columns('SELECT age * 2 + 10 AS calculated_age, total / quantity AS unit_price FROM users JOIN orders ON users.id = orders.user_id');

-- Summary report: Show unique contexts found
SELECT '=== Summary: Column contexts found ===' as summary_section;
SELECT DISTINCT context, COUNT(*) as count 
FROM (
    SELECT * FROM parse_columns('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 18 ORDER BY o.total DESC')
) 
GROUP BY context 
ORDER BY count DESC;

-- Summary report: Show expression identifier patterns
SELECT '=== Summary: Expression identifier patterns ===' as summary_section;
SELECT 
    CASE 
        WHEN expression_identifiers LIKE '%[%[%,%' THEN 'Multiple identifiers'
        WHEN expression_identifiers LIKE '%"%,"%,"%' THEN 'Three-part qualified'
        WHEN expression_identifiers LIKE '%"%,"%' THEN 'Two-part qualified'
        ELSE 'Single identifier'
    END as pattern_type,
    COUNT(*) as count
FROM (
    SELECT * FROM parse_columns('SELECT main.users.name, users.age, name, 1 AS const FROM main.users')
)
GROUP BY pattern_type
ORDER BY count DESC;

-- Cleanup
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS profiles;