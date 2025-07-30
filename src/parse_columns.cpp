#include "parse_columns.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

enum class ColumnContext {
	Select,
	Where,
	Having,
	OrderBy,
	GroupBy,
	Join,
	FunctionArg,
	Window,
	Nested
};

inline const char *ToString(ColumnContext context) {
	switch (context) {
		case ColumnContext::Select: return "select";
		case ColumnContext::Where: return "where";
		case ColumnContext::Having: return "having";
		case ColumnContext::OrderBy: return "order_by";
		case ColumnContext::GroupBy: return "group_by";
		case ColumnContext::Join: return "join";
		case ColumnContext::FunctionArg: return "function_arg";
		case ColumnContext::Window: return "window";
		case ColumnContext::Nested: return "nested";
		default: return "unknown";
	}
}

struct ParseColumnsState : public GlobalTableFunctionState {
	idx_t row = 0;
	vector<ColumnResult> results;
};

struct ParseColumnsBindData : public TableFunctionData {
	string sql;
};

// Helper function to extract schema, table, and column from column_names vector
static void ExtractTableInfo(const vector<string> &column_names, 
                            string &table_schema, string &table_name, string &column_name) {
	if (column_names.empty()) {
		return;
	}
	
	// For now, assume simple heuristic:
	// - If 3+ elements: first could be schema, second table, third+ column path
	// - If 2 elements: first table, second+ column path  
	// - If 1 element: unqualified column
	
	if (column_names.size() >= 3) {
		// Assume schema.table.column format
		table_schema = column_names[0];
		table_name = column_names[1]; 
		column_name = column_names[2];
	} else if (column_names.size() == 2) {
		// Assume table.column format
		table_schema = "main"; // Default schema
		table_name = column_names[0];
		column_name = column_names[1];
	} else {
		// Unqualified column - could be table column or alias
		table_schema = ""; // Will be set to NULL
		table_name = "";   // Will be set to NULL  
		column_name = column_names[0];
	}
}

// Helper function to convert vector<string> to a readable expression string
static string VectorToString(const vector<string> &vec) {
	if (vec.empty()) {
		return "";
	}
	string result = vec[0];
	for (size_t i = 1; i < vec.size(); i++) {
		result += "." + vec[i];
	}
	return result;
}

// Helper function to serialize expression_identifiers as JSON-like string
static string SerializeExpressionIdentifiers(const vector<vector<string>> &identifiers) {
	if (identifiers.empty()) {
		return "[]";
	}
	
	string result = "[";
	for (size_t i = 0; i < identifiers.size(); i++) {
		if (i > 0) result += ",";
		result += "[";
		for (size_t j = 0; j < identifiers[i].size(); j++) {
			if (j > 0) result += ",";
			result += "\"" + identifiers[i][j] + "\"";
		}
		result += "]";
	}
	result += "]";
	return result;
}

// Recursive function to extract column references from expressions
static void ExtractFromExpression(const ParsedExpression &expr, 
                                 vector<ColumnResult> &results, 
                                 ColumnContext context,
                                 const string &selected_name = "") {
	
	if (expr.expression_class == ExpressionClass::COLUMN_REF) {
		auto &col_ref = (ColumnRefExpression &)expr;
		
		string table_schema, table_name, column_name;
		ExtractTableInfo(col_ref.column_names, table_schema, table_name, column_name);
		
		// Convert empty strings to NULLs for consistency
		if (table_schema.empty()) table_schema = "";
		if (table_name.empty()) table_name = "";
		
		vector<vector<string>> expr_ids = {col_ref.column_names};
		results.push_back(ColumnResult{
			expr_ids, // expression_identifiers
			table_schema.empty() ? "" : table_schema,
			table_name.empty() ? "" : table_name,
			column_name,
			ToString(context),
			VectorToString(col_ref.column_names),
			selected_name.empty() ? "" : selected_name
		});
	} else {
		// For non-column expressions, continue traversing to find nested column references
		ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
			ExtractFromExpression(child, results, ColumnContext::FunctionArg);
		});
	}
}

// Helper function to collect all identifiers from an expression recursively
static void CollectExpressionIdentifiers(const ParsedExpression &expr, 
                                        vector<vector<string>> &all_identifiers) {
	if (expr.expression_class == ExpressionClass::COLUMN_REF) {
		auto &col_ref = (ColumnRefExpression &)expr;
		all_identifiers.push_back(col_ref.column_names);
	} else {
		ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
			CollectExpressionIdentifiers(child, all_identifiers);
		});
	}
}

// Extract columns from SELECT node
static void ExtractFromSelectNode(const SelectNode &select_node, vector<ColumnResult> &results) {
	
	// Extract from SELECT list (output columns)
	for (const auto &select_item : select_node.select_list) {
		string selected_name = select_item->alias.empty() ? "" : select_item->alias;
		
		// If no explicit alias, derive from expression  
		if (selected_name.empty() && select_item->expression_class == ExpressionClass::COLUMN_REF) {
			auto &col_ref = (ColumnRefExpression &)*select_item;
			selected_name = col_ref.GetColumnName();
		}
		
		// First extract individual column references
		ExtractFromExpression(*select_item, results, ColumnContext::Select);
		
		// Then add the output column entry if it's a complex expression
		vector<vector<string>> all_identifiers;
		CollectExpressionIdentifiers(*select_item, all_identifiers);
		
		if (all_identifiers.size() > 1 || (all_identifiers.size() == 1 && !select_item->alias.empty())) {
			// Complex expression or aliased column - add output entry
			results.push_back(ColumnResult{
				all_identifiers,
				"", // table_schema 
				"", // table_name
				"", // column_name
				ToString(ColumnContext::Select),
				select_item->ToString(),
				selected_name.empty() ? "" : selected_name
			});
		}
	}
	
	// Extract from WHERE clause
	if (select_node.where_clause) {
		ExtractFromExpression(*select_node.where_clause, results, ColumnContext::Where);
	}
	
	// Extract from GROUP BY clause
	for (const auto &group_expr : select_node.groups.group_expressions) {
		ExtractFromExpression(*group_expr, results, ColumnContext::GroupBy);
	}
	
	// Extract from HAVING clause  
	if (select_node.having) {
		ExtractFromExpression(*select_node.having, results, ColumnContext::Having);
	}
	
	// Extract from ORDER BY clause
	for (const auto &modifier : select_node.modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			auto &order_modifier = (OrderModifier &)*modifier;
			for (const auto &order_term : order_modifier.orders) {
				ExtractFromExpression(*order_term.expression, results, ColumnContext::OrderBy);
			}
		}
	}
}

// BIND function: runs during query planning to decide output schema
static unique_ptr<FunctionData> ParseColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	
	string sql_input = StringValue::Get(input.inputs[0]);
	
	// Define output schema - simplified for initial implementation
	return_types = {
		LogicalType::VARCHAR,  // expression_identifiers (as JSON-like string for now)
		LogicalType::VARCHAR,  // table_schema
		LogicalType::VARCHAR,  // table_name  
		LogicalType::VARCHAR,  // column_name
		LogicalType::VARCHAR,  // context
		LogicalType::VARCHAR,  // expression
		LogicalType::VARCHAR   // selected_name
	};
	
	names = {"expression_identifiers", "table_schema", "table_name", "column_name", 
	         "context", "expression", "selected_name"};
	
	auto result = make_uniq<ParseColumnsBindData>();
	result->sql = sql_input;
	return std::move(result);
}

// INIT function: runs before table function execution
static unique_ptr<GlobalTableFunctionState> ParseColumnsInit(ClientContext &context,
                                                           TableFunctionInitInput &input) {
	return make_uniq<ParseColumnsState>();
}

// Main parsing function
static void ParseColumnsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (ParseColumnsBindData &)*data_p.bind_data;
	auto &state = (ParseColumnsState &)*data_p.global_state;
	
	if (state.row == 0) {
		// Parse the SQL statement  
		Parser parser;
		parser.ParseQuery(bind_data.sql);
		
		if (parser.statements.empty()) {
			return;
		}
		
		// Process each statement
		for (const auto &statement : parser.statements) {
			if (statement->type == StatementType::SELECT_STATEMENT) {
				auto &select_stmt = (SelectStatement &)*statement;
				auto &select_node = (SelectNode &)*select_stmt.node;
				ExtractFromSelectNode(select_node, state.results);
			}
		}
	}
	
	// Output results  
	idx_t count = 0;
	while (state.row < state.results.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &result = state.results[state.row];
		
		output.data[0].SetValue(count, Value(SerializeExpressionIdentifiers(result.expression_identifiers)));
		output.data[1].SetValue(count, result.table_schema.empty() ? Value() : Value(result.table_schema));
		output.data[2].SetValue(count, result.table_name.empty() ? Value() : Value(result.table_name));
		output.data[3].SetValue(count, result.column_name.empty() ? Value() : Value(result.column_name));
		output.data[4].SetValue(count, Value(result.context));
		output.data[5].SetValue(count, Value(result.expression));
		output.data[6].SetValue(count, result.selected_name.empty() ? Value() : Value(result.selected_name));
		
		state.row++;
		count++;
	}
	
	output.SetCardinality(count);
}

void RegisterParseColumnsFunction(DatabaseInstance &db) {
	TableFunction parse_columns("parse_columns", {LogicalType::VARCHAR}, ParseColumnsFunction, ParseColumnsBind, ParseColumnsInit);
	ExtensionUtil::RegisterFunction(db, parse_columns);
}

void RegisterParseColumnScalarFunction(DatabaseInstance &db) {
	// TODO: Implement scalar version similar to parse_function_names
}

} // namespace duckdb