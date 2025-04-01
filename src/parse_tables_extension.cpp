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

namespace duckdb {

struct TableRefResult {
    string schema;
    string table;
    string context;
};

struct ParseTablesState : public GlobalTableFunctionState {
    idx_t row = 0;
    vector<TableRefResult> results;
};

struct ParseTablesBindData : public TableFunctionData {
    string sql;
};

// BIND function: runs during query planning to decide output schema
static unique_ptr<FunctionData> Bind(ClientContext &context, 
                                    TableFunctionBindInput &input, 
                                    vector<LogicalType> &return_types, 
                                    vector<string> &names) {
                                
    string sql_input = StringValue::Get(input.inputs[0]);
    std::cout << "sql_input: " << sql_input << std::endl;
                                                    
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
static unique_ptr<GlobalTableFunctionState> MyInit(ClientContext &context,
    TableFunctionInitInput &input) {
    return make_uniq<ParseTablesState>();
}

static void ExtractTablesFromQueryNode(const QueryNode &node, vector<TableRefResult> &results);

static void ExtractTablesFromRef(const TableRef &ref, vector<TableRefResult> &results, const string &context = "from") {
    std::cout << "Ref type: " << (int)ref.type << std::endl;
    if (ref.type == TableReferenceType::BASE_TABLE) {
        auto &base = (BaseTableRef &)ref;
        std::cout << "Found base table: " << base.schema_name << "." << base.table_name << std::endl;
    }


    switch (ref.type) {
        case TableReferenceType::BASE_TABLE: {
            auto &base = (BaseTableRef &)ref;
            results.push_back(TableRefResult{
                base.schema_name.empty() ? "main" : base.schema_name,
                base.table_name,
                context
            });
            break;
        }
        case TableReferenceType::JOIN: {
            auto &join = (JoinRef &)ref;
            ExtractTablesFromRef(*join.left, results, "join_left");
            ExtractTablesFromRef(*join.right, results, "join_right");
            break;
        }
        case TableReferenceType::SUBQUERY: {
            auto &subquery = (SubqueryRef &)ref;
            if (subquery.subquery && subquery.subquery->node) {
                ExtractTablesFromQueryNode(*subquery.subquery->node, results);
            }
            break;
        }
        default:
            break;
    }
}

static void ExtractTablesFromQueryNode(const QueryNode &node, vector<TableRefResult> &results) {
    if (node.type == QueryNodeType::SELECT_NODE) {
        auto &select_node = (SelectNode &)node;

        std::cout << "Extracting from query node" << std::endl;


        // Handle CTEs
        
        for (const auto &entry : select_node.cte_map.map) {
            if (entry.second && entry.second->query) {
                ExtractTablesFromQueryNode(*entry.second->query->node, results);
            }
        }
        

        if (select_node.from_table) {
            ExtractTablesFromRef(*select_node.from_table, results, "from");
        }
    }
}

static void MyFunc(ClientContext &context,
                   TableFunctionInput &data,
                   DataChunk &output) {
    auto &state = (ParseTablesState &)*data.global_state;
    auto &bind_data = (ParseTablesBindData &)*data.bind_data;

    if (state.results.empty() && state.row == 0) {
        try {
            Parser parser;
            parser.ParseQuery(bind_data.sql);

            for (auto &stmt : parser.statements) {
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
    output.SetValue(2, 0, Value(ref.context));

    state.row++;
}


// ---------------------------------------------------
// EXTENSION SCAFFOLDING

static void LoadInternal(DatabaseInstance &instance) {

    // Register parse_tables
    TableFunction tf("parse_tables", {LogicalType::VARCHAR}, MyFunc, Bind, MyInit);
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
