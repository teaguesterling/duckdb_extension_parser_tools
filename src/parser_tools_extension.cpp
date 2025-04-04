#define DUCKDB_EXTENSION_MAIN

#include "parser_tools_extension.hpp"
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
	RegisterParseTableScalarFunction(instance);
}

void ParserToolsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string ParserToolsExtension::Name() {
	return "parser";
}

std::string ParserToolsExtension::Version() const {
#ifdef EXT_VERSION_PARSER_TOOLS
	return EXT_VERSION_PARSER_TOOLS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void parser_tools_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ParserToolsExtension>();
}

DUCKDB_EXTENSION_API const char *parser_tools_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
