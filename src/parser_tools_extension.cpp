#define DUCKDB_EXTENSION_MAIN

#include "parser_tools_extension.hpp"
#include "parse_tables.hpp"
#include "parse_where.hpp"
#include "parse_functions.hpp"
#include "parse_columns.hpp"
#include "parse_statements.hpp"
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

static void LoadInternal(ExtensionLoader &loader) {
  RegisterParseTablesFunction(loader);
	RegisterParseTableScalarFunction(loader);
	RegisterParseWhereFunction(loader);
	RegisterParseWhereScalarFunction(loader);
	RegisterParseWhereDetailedFunction(loader);
	RegisterParseFunctionsFunction(loader);
	RegisterParseFunctionScalarFunction(loader);
  RegisterParseColumnsFunction(instance);
	RegisterParseColumnScalarFunction(instance);
	RegisterParseStatementsFunction(loader);
	RegisterParseStatementsScalarFunction(loader);
}

void ParserToolsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(parser_tools, loader) {
    duckdb::LoadInternal(loader);
}

}
