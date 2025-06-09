#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Forward declarations
class DatabaseInstance;

struct WhereConditionResult {
    std::string condition;
    std::string table_name;  // The table this condition applies to (if determinable)
    std::string context;     // The context where this condition appears (WHERE, HAVING, etc.)
};

struct DetailedWhereConditionResult {
    std::string column_name;     // The column being compared
    std::string operator_type;   // The comparison operator (>, <, =, etc.)
    std::string value;          // The value being compared against
    std::string table_name;     // The table this condition applies to (if determinable)
    std::string context;        // The context where this condition appears (WHERE, HAVING, etc.)
};

void RegisterParseWhereFunction(DatabaseInstance &db);
void RegisterParseWhereScalarFunction(DatabaseInstance &db);
void RegisterParseWhereDetailedFunction(DatabaseInstance &db);

} // namespace duckdb 