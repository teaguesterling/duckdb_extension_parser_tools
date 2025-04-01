#define DUCKDB_EXTENSION_MAIN

#include "parse_tables_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>


namespace duckdb {

struct ParseTablesState : public GlobalTableFunctionState {
    idx_t row = 0;
};

struct TableRefResult {
    string schema;
    string table;
    string context;
};

// BIND function: runs during query planning to decide output schema
static unique_ptr<FunctionData> Bind(ClientContext &context, 
                                                TableFunctionBindInput &input, 
                                                vector<LogicalType> &return_types, 
                                                vector<string> &names) {
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
    // schema name, table name, usage context (from, join, cte, etc)
    names = {"schema", "table", "context"};
    return nullptr;
}

// INIT function: runs before table function execution
static unique_ptr<GlobalTableFunctionState> MyInit(ClientContext &context,
    TableFunctionInitInput &input) {
    return make_uniq<ParseTablesState>();
}

// EXECUTE function: produces rows
static void MyFunc(ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output) {

    auto &state = (ParseTablesState &)*data.global_state;

    std::cout << "row: " << state.row << std::endl;

    if (state.row >= 1) {
        return; // no more rows to produce
    }

    // Example: single string column with 1 row
    // auto row_count = 1;
    output.SetCardinality(1);

    output.SetValue(0, 0, Value("my_schema"));
    output.SetValue(1, 0, Value("my_table"));
    output.SetValue(2, 0, Value("from"));
    
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
