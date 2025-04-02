#define DUCKDB_EXTENSION_MAIN

#include "parse_tables_extension.hpp"
#include "parse_tables.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

// ---------------------------------------------------
// EXTENSION SCAFFOLDING

static void LoadInternal(DatabaseInstance &instance) {
    RegisterParseTablesFunction(instance);
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
