#define DUCKDB_EXTENSION_MAIN

#include "parse_tables_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

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

inline const char *ToString(TableContext context) {
    switch (context) {
        case TableContext::From: return "from";
        case TableContext::JoinLeft: return "join_left";
        case TableContext::JoinRight: return "join_right";
        case TableContext::FromCTE: return "from_cte";
        case TableContext::CTE: return "cte";
        case TableContext::Subquery: return "subquery";
        default: return "unknown";
    }
}

struct TableRefResult {
    string schema;
    string table;
    TableContext context;
};

struct ParseTablesState : public GlobalTableFunctionState {
    idx_t row = 0;
    vector<TableRefResult> results;
};

struct ParseTablesBindData : public TableFunctionData {
    string sql;
};

// BIND function: runs during query planning to decide output schema
static unique_ptr<FunctionData> ParseTablesBind(ClientContext &context, 
                                    TableFunctionBindInput &input, 
                                    vector<LogicalType> &return_types, 
                                    vector<string> &names) {
                                
    string sql_input = StringValue::Get(input.inputs[0]);
                                                    
    // always return the same columns:

    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
    // schema name, table name, usage context (from, join, cte, etc)
    names = {"schema", "table", "context"};
    
    // create a bind data object to hold the SQL input
    
    auto result = make_uniq<ParseTablesBindData>();
    result->sql = sql_input;

    return std::move(result);
}

// INIT function: runs before table function execution
static unique_ptr<GlobalTableFunctionState> ParseTablesInit(ClientContext &context,
    TableFunctionInitInput &input) {
    return make_uniq<ParseTablesState>();
}

static void ExtractTablesFromQueryNode(
    const duckdb::QueryNode &node,
    std::vector<TableRefResult> &results,
    const TableContext context = TableContext::From,
    const duckdb::CommonTableExpressionMap *cte_map = nullptr
);

static void ExtractTablesFromRef(
    const duckdb::TableRef &ref,
    std::vector<TableRefResult> &results,
    const TableContext context = TableContext::From,
    bool is_top_level = false,
    const duckdb::CommonTableExpressionMap *cte_map = nullptr
) {
    using namespace duckdb;

    switch (ref.type) {
        case TableReferenceType::BASE_TABLE: {
            auto &base = (BaseTableRef &)ref;
            TableContext context_label = context;

            if (cte_map && cte_map->map.find(base.table_name) != cte_map->map.end()) {
                context_label = TableContext::FromCTE;
            } else if (is_top_level) {
                context_label = TableContext::From;
            }

            results.push_back(TableRefResult{
                base.schema_name.empty() ? "main" : base.schema_name,
                base.table_name,
                context_label
            });
            break;
        }
        case TableReferenceType::JOIN: {
            auto &join = (JoinRef &)ref;
            ExtractTablesFromRef(*join.left, results, TableContext::JoinLeft, is_top_level, cte_map);
            ExtractTablesFromRef(*join.right, results, TableContext::JoinRight, false, cte_map);
            break;
        }
        case TableReferenceType::SUBQUERY: {
            auto &subquery = (SubqueryRef &)ref;
            if (subquery.subquery && subquery.subquery->node) {
                ExtractTablesFromQueryNode(*subquery.subquery->node, results, TableContext::Subquery, cte_map);
            }
            break;
        }
        default:
            break;
    }
}


static void ExtractTablesFromQueryNode(
    const duckdb::QueryNode &node,
    std::vector<TableRefResult> &results,
    const TableContext context,
    const duckdb::CommonTableExpressionMap *cte_map
) {
    using namespace duckdb;

    if (node.type == QueryNodeType::SELECT_NODE) {
        auto &select_node = (SelectNode &)node;

        // Emit CTE definitions
        for (const auto &entry : select_node.cte_map.map) {
            results.push_back(TableRefResult{
                "", entry.first, TableContext::CTE
            });

            if (entry.second && entry.second->query && entry.second->query->node) {
                ExtractTablesFromQueryNode(*entry.second->query->node, results, TableContext::From, &select_node.cte_map);
            }
        }

        if (select_node.from_table) {
            ExtractTablesFromRef(*select_node.from_table, results, context, true, &select_node.cte_map);
        }
    }
}

static void ParseTablesFunction(ClientContext &context,
                   TableFunctionInput &data,
                   DataChunk &output) {
    auto &state = (ParseTablesState &)*data.global_state;
    auto &bind_data = (ParseTablesBindData &)*data.bind_data;

    if (state.results.empty() && state.row == 0) {
        try {
            Parser parser;
            parser.ParseQuery(bind_data.sql);

            for (auto &stmt : parser.statements) {
                if (stmt->type != StatementType::SELECT_STATEMENT) {
                    throw InvalidInputException("parse_tables only supports SELECT statements");
                }
                
                if (stmt->type == StatementType::SELECT_STATEMENT) {
                    auto &select_stmt = (SelectStatement &)*stmt;
                    if (select_stmt.node) {
                        ExtractTablesFromQueryNode(*select_stmt.node, state.results);
                    }
                }
            }
        } catch (const std::exception &ex) {
            throw InvalidInputException("Failed to parse SQL: %s", ex.what());
        }
    }

    if (state.row >= state.results.size()) {
        return;
    }

    auto &ref = state.results[state.row];
    output.SetCardinality(1);
    output.SetValue(0, 0, Value(ref.schema));
    output.SetValue(1, 0, Value(ref.table));
    output.SetValue(2, 0, Value(ToString(ref.context)));

    state.row++;
}


// ---------------------------------------------------
// EXTENSION SCAFFOLDING

static void LoadInternal(DatabaseInstance &instance) {

    // Register parse_tables
    TableFunction tf("parse_tables", {LogicalType::VARCHAR}, ParseTablesFunction, ParseTablesBind, ParseTablesInit);
    ExtensionUtil::RegisterFunction(instance, tf);
}

void ParseTablesExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ParseTablesExtension::Name() {
	return "parse_tables";
}

std::string ParseTablesExtension::Version() const {
#ifdef EXT_VERSION_PARSE_TABLES
	return EXT_VERSION_PARSE_TABLES;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void parse_tables_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ParseTablesExtension>();
}

DUCKDB_EXTENSION_API const char *parse_tables_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
