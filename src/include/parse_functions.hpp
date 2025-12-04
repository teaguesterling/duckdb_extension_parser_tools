#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Forward declarations
class ExtensionLoader;

struct FunctionResult {
	std::string function_name;
	std::string schema;
	std::string context;     // The context where this function appears (SELECT, WHERE, etc.)
};

void RegisterParseFunctionsFunction(ExtensionLoader &loader);
void RegisterParseFunctionScalarFunction(ExtensionLoader &loader);

} // namespace duckdb