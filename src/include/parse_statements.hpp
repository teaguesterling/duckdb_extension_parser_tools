#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Forward declarations
class ExtensionLoader;

struct StatementResult {
	std::string statement;
};

void RegisterParseStatementsFunction(ExtensionLoader &loader);
void RegisterParseStatementsScalarFunction(ExtensionLoader &loader);

} // namespace duckdb