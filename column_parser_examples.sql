-- Column Parser Examples - Demonstrating Key Features
LOAD parser_tools;

SELECT '=== Example 1: Basic Column References ===' as example;
SELECT * FROM parse_columns('SELECT name, age, email FROM customers') LIMIT 3;

SELECT '=== Example 2: Alias Chain (Key Innovation) ===' as example;
SELECT * FROM parse_columns('SELECT 1 AS a, users.age AS b, a+b AS c, b AS d FROM users');

SELECT '=== Example 3: Schema-Qualified Columns ===' as example;
SELECT * FROM parse_columns('SELECT main.customers.name, main.customers.email FROM main.customers') LIMIT 2;

SELECT '=== Example 4: Nested Struct Field Access ===' as example;
SELECT expression_identifiers, expression, table_schema, table_name, column_name 
FROM parse_columns('SELECT customers.profile.address.city, customers.profile.address.street FROM customers');

SELECT '=== Example 5: Multi-table JOIN with Complex Expressions ===' as example;
SELECT column_name, context, expression, selected_name 
FROM parse_columns('
    SELECT 
        c.name AS customer_name,
        o.total AS order_amount, 
        c.age + o.total AS customer_score
    FROM customers c 
    JOIN orders o ON c.id = o.customer_id
') 
WHERE column_name IS NOT NULL OR selected_name IS NOT NULL;

SELECT '=== Example 6: Input vs Output Column Distinction ===' as example;
SELECT 
    CASE WHEN selected_name IS NULL THEN 'INPUT' ELSE 'OUTPUT' END as column_type,
    COALESCE(selected_name, column_name) as identifier,
    expression,
    context
FROM parse_columns('
    SELECT 
        customers.name AS customer_name,
        orders.total * 1.1 AS total_with_tax,
        customers.age
    FROM customers 
    JOIN orders ON customers.id = orders.customer_id
')
ORDER BY column_type, identifier;

SELECT '=== Example 7: Different SQL Contexts ===' as example;
SELECT DISTINCT context, COUNT(*) as count
FROM parse_columns('
    SELECT 
        c.name,
        COUNT(*) as order_count
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    WHERE c.age > 25 AND c.status = ''active''
    GROUP BY c.id, c.name
    HAVING COUNT(*) > 2
    ORDER BY c.name
')
GROUP BY context
ORDER BY context;

SELECT '=== Example 8: Function Arguments vs Select Items ===' as example;
SELECT 
    context,
    column_name,
    expression,
    CASE WHEN selected_name IS NOT NULL THEN selected_name ELSE 'N/A' END as output_name
FROM parse_columns('
    SELECT 
        UPPER(c.name) AS customer_name,
        CONCAT(c.first_name, '' '', c.last_name) AS full_name,
        LENGTH(c.email) AS email_length
    FROM customers c
')
ORDER BY context, column_name;