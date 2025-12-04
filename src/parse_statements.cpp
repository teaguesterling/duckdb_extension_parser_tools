#include "parse_statements.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

struct ParseStatementsState : public GlobalTableFunctionState {
	idx_t row = 0;
	vector<StatementResult> results;
};

struct ParseStatementsBindData : public TableFunctionData {
	string sql;
};

// BIND function: runs during query planning to decide output schema
static unique_ptr<FunctionData> ParseStatementsBind(ClientContext &context,
													TableFunctionBindInput &input,
													vector<LogicalType> &return_types,
													vector<string> &names) {

	string sql_input = StringValue::Get(input.inputs[0]);

	// Return single column with statement text
	return_types = {LogicalType::VARCHAR};
	names = {"statement"};

	// Create a bind data object to hold the SQL input
	auto result = make_uniq<ParseStatementsBindData>();
	result->sql = sql_input;

	return std::move(result);
}

// INIT function: runs before table function execution
static unique_ptr<GlobalTableFunctionState> ParseStatementsInit(ClientContext &context,
																TableFunctionInitInput &input) {
	return make_uniq<ParseStatementsState>();
}

static void ExtractStatementsFromSQL(const std::string &sql, std::vector<StatementResult> &results) {
	Parser parser;

	try {
		parser.ParseQuery(sql);
	} catch (const ParserException &ex) {
		// Swallow parser exceptions to make this function more robust
		return;
	}

	for (auto &stmt : parser.statements) {
		if (stmt) {
			// Convert statement back to string
			auto statement_str = stmt->ToString();
			results.push_back(StatementResult{statement_str});
		}
	}
}

static void ParseStatementsFunction(ClientContext &context,
								   TableFunctionInput &data,
								   DataChunk &output) {
	auto &state = (ParseStatementsState &)*data.global_state;
	auto &bind_data = (ParseStatementsBindData &)*data.bind_data;

	if (state.results.empty() && state.row == 0) {
		ExtractStatementsFromSQL(bind_data.sql, state.results);
	}

	if (state.row >= state.results.size()) {
		return;
	}

	auto &stmt = state.results[state.row];
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(stmt.statement));

	state.row++;
}

static void ParseStatementsScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, list_entry_t>(args.data[0], result, args.size(),
	[&result](string_t query) -> list_entry_t {
		// Parse the SQL query and extract statements
		auto query_string = query.GetString();
		std::vector<StatementResult> parsed_statements;
		ExtractStatementsFromSQL(query_string, parsed_statements);

		auto current_size = ListVector::GetListSize(result);
		auto number_of_statements = parsed_statements.size();
		auto new_size = current_size + number_of_statements;

		// Grow list if needed
		if (ListVector::GetListCapacity(result) < new_size) {
			ListVector::Reserve(result, new_size);
		}

		// Write the statements into the child vector
		auto statements = FlatVector::GetData<string_t>(ListVector::GetEntry(result));
		for (size_t i = 0; i < parsed_statements.size(); i++) {
			auto &stmt = parsed_statements[i];
			statements[current_size + i] = StringVector::AddStringOrBlob(ListVector::GetEntry(result), stmt.statement);
		}

		// Update size
		ListVector::SetListSize(result, new_size);

		return list_entry_t(current_size, number_of_statements);
	});
}

static void NumStatementsScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
	[](string_t query) -> int64_t {
		// Parse the SQL query and count statements
		auto query_string = query.GetString();
		std::vector<StatementResult> parsed_statements;
		ExtractStatementsFromSQL(query_string, parsed_statements);

		return static_cast<int64_t>(parsed_statements.size());
	});
}

// Extension scaffolding
// ---------------------------------------------------

void RegisterParseStatementsFunction(ExtensionLoader &loader) {
	// Table function that returns one row per statement
	TableFunction tf("parse_statements", {LogicalType::VARCHAR}, ParseStatementsFunction, ParseStatementsBind, ParseStatementsInit);
	loader.RegisterFunction(tf);
}

void RegisterParseStatementsScalarFunction(ExtensionLoader &loader) {
	// parse_statements is a scalar function that returns a list of statement strings
	ScalarFunction sf("parse_statements", {LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR), ParseStatementsScalarFunction);
	loader.RegisterFunction(sf);

	// num_statements is a scalar function that returns the count of statements
	ScalarFunction num_sf("num_statements", {LogicalType::VARCHAR}, LogicalType::BIGINT, NumStatementsScalarFunction);
	loader.RegisterFunction(num_sf);
}

} // namespace duckdb