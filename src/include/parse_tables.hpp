#pragma once

#include "duckdb.hpp"

namespace duckdb {

/**
 * Represents where a table is used in a query.
 */
enum class TableContext {
    From,       // table in from clause
    JoinLeft,   // table in left side of a join
    JoinRight,  // table in right side of a join
    FromCTE,    // table in from clause that references a CTE
    CTE,        // table is defined as a CTE
    Subquery    // table in a subquery
};

const char *ToString(TableContext context);
const TableContext FromString(const char *context);

struct TableRefResult {
    std::string schema;
    std::string table;
    TableContext context;
};

static void ExtractTablesFromSQL(const std::string &sql, std::vector<TableRefResult> &results);
static void ExtractTablesFromQueryNode(
    const duckdb::QueryNode &node,
    std::vector<TableRefResult> &results,
    const TableContext context = TableContext::From,
    const duckdb::CommonTableExpressionMap *cte_map = nullptr
);

void RegisterParseTablesFunction(duckdb::ExtensionLoader &loader);
void RegisterParseTableScalarFunction(ExtensionLoader &loader);

} // namespace duckdb
