#define DUCKDB_EXTENSION_MAIN

#include "parse_tables_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void ParseTablesScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "ParseTables "+name.GetString()+" 🐥");
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto parse_tables_scalar_function = ScalarFunction("parse_tables", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ParseTablesScalarFun);
    ExtensionUtil::RegisterFunction(instance, parse_tables_scalar_function);
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
