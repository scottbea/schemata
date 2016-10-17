var fs = require('fs');
var _ = require('underscore');
var pd = require('pretty-data').pd;
var CSV = require('csv-string');
var util = require('util');
var scheme = require('./discovery.js');
var polymorph = require('./polymorph.js');

// Command Line Processing and Handlers
function usage() {
	console.log("\n\nUsage: node schematize.js <command> <file1> <file2> <jsonarray>");
	console.log("\n\tfile1 is the primary data file.\n\tfile2 is a schema file when issuing json.");
	console.log("\n");
	console.log("\tnode schematize.js --parse <file.schema>");
	console.log("\tnode schematize.js --compile <file.schema>");
	console.log("\tnode schematize.js --js2json <file.js>");
	console.log("\tnode schematize.js --csv2json <file.csv> <file.schema> <file2file.map> [--skipheader] [--tsv]");
	console.log("\tnode schematize.js --types <file.json> [--sort] [--jsonarray]");
	console.log("\tnode schematize.js --json2schema <file.json> [--sort] [--jsonarray]");
	console.log("\tnode schematize.js --keys <file.json>");
	console.log("\tnode schematize.js --flatten <file.json> <file.schema>");
	console.log("\tnode schematize.js --extrude <file.json> <file.schema>");
	console.log("\tnode schematize.js --values <file.json> [<max_values_per_field> [[<max_records> [<field_list_csv>]]]");
	console.log("\tnode schematize.js --dimensions <file.json>");
	console.log("\tnode schematize.js --json2csv <file.json> <file.schema>");
	console.log("\tnode schematize.js --merge <source.json> <updates.json>");
	console.log("\tnode schematize.js --help");
	console.log("\n");
	console.log("\n");
	console.log("--parse: Load a schema file, parse it, and display the JSON version of it.");
	console.log("--compile: Load a schema file and renders it with the resulting field names, expanding arrays and dealing with types.");
	console.log("--js2json: Loads a normal javascript file containing some data structure and converts it to JSON.");
	console.log("--csv2json: Load a CSV file and convert it to JSON using the supplied schema file and the optional mapping file.");
	console.log("--types: Analyze a JSON data file and find all of the unique key values after trying to flatten the structure for arrays.");
	console.log("--json2schema: Analyze a JSON data file, does type inspection, and generates output in the .schema format.");
	console.log("--keys: Analyze a JSON data file and find all of the distinct keys");
	console.log("--flatten: Flatten the hierarchy of the objects in a JSON file by using dot notation for compound field names");
	console.log("--extrude: Extrude the hierarchy of the flattened object fields and values into a JSON object");
	console.log("--values: Analyze a JSON data file and find all of the distinct values and their frequencies. Optionally choose fields and max values.");
	console.log("--dimensions: Analyze a JSON data file and find the observed array sizes for all array elements in the current data.");
	console.log("--json2csv: Load a JSON file and convert it to a CSV file using the supplied schema.");
	console.log("--merge: Load two JSON files and merge the object tree of the second into the first.");
	console.log("\n");
}
function processSchemaCommand(schemaFileName) {
	var schema = polymorph.parseSchema(schemaFileName);
	console.log(pd.json(schema));
}
function processCompileCommand(schemaFileName) {
	var schema = polymorph.parseSchema(schemaFileName);
	_.each(schema, function(field) { console.log(field.name); })
}
function processNormalizeCommand(dataFileName, asJsonArray) {
	var valid = false;
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	var jsonData = jsonDataSet.data;
	//.trim().replace(/[\n]|[\r]/g, "");
	if (jsonData) {
		jsonData = eval(jsonData);
		if (jsonData) {
			jsonData = JSON.stringify(jsonData);
			valid = true;
		}
	}
	if (valid) { console.log(jsonData); }
	else { console.log("Invalid javascript data"); }
}
function processJsonCommand(dataFileName, schemaFileName, mappingFileName, asJsonArray, fieldSeparator, skipHeader) {
	var table = fs.readFileSync(dataFileName, 'utf8');
	var schema = polymorph.parseSchema(schemaFileName);
	var mapping = mappingFileName ? polymorph.loadDataMapping(mappingFileName) : null;
	polymorph.convertCsvToJson(table, schema, mapping, asJsonArray, fieldSeparator, skipHeader);
}
function processExtrudeJsonCommand(dataFileName, schemaFileName, mappingFileName, asJsonArray) {
	debugger;
	var table = fs.readFileSync(dataFileName, 'utf8');
	var schema = polymorph.parseSchema(schemaFileName);
	var mapping = mappingFileName ? polymorph.loadFieldMapping(mappingFileName) : null;
	polymorph.convertTsvToJson(table, schema, mapping, asJsonArray);
}
function processDiscoverCommand(dataFileName, useAlphabeticalSort, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	scheme.discoverJsonSchema(jsonDataSet, true, false, useAlphabeticalSort);
}
function processHeadersCommand(dataFileName, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	scheme.discoverJsonKeys(jsonDataSet, false);
}
function processJson2SchemaCommand(dataFileName, useAlphabeticalSort, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	scheme.discoverJsonSchema(jsonDataSet, false, true, useAlphabeticalSort);
}
function processJson2Schema2Command(dataFileName, useAlphabeticalSort, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	scheme.discoverJsonSchema2(jsonDataSet, false, true, true, useAlphabeticalSort);
}
function processValuesCommand(dataFileName, maxValuesPerField, maxRecords, fieldListSpecifier, asJsonArray) {
	//console.log("dataFileName=%s, maxValuesPerField=%s, maxRecords=%s, fieldListSpecifier=%s, asJsonArray=%s", dataFileName, maxValuesPerField, maxRecords, fieldListSpecifier, asJsonArray);
	var max = parseInt(_.isFinite(maxRecords) ? maxRecords : "10000");
	polymorph.loadJsonDataSetAsync(dataFileName, asJsonArray, max, 0, function(err, jsonDataSet) {
		if (!err && jsonDataSet) {
			var fieldList = fieldListSpecifier ? _.map(fieldListSpecifier.split(','), function(s) { return s.trim(); }) : null;
			scheme.discoverJsonValueHistograms(jsonDataSet, true, maxValuesPerField, fieldList);
		}
		else {
			console.log("Error: %s", err);
		}
	});
}
function processDimensionsCommand(dataFileName, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	scheme.discoverJsonArrayDimensions(jsonDataSet, true);
}
function processCsvCommand(dataFileName, schemaFileName, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	var schema = polymorph.parseSchema(schemaFileName);
	polymorph.convertJsonToCsv(jsonDataSet, schema);
}
function processFlattenCommand(dataFileName, schemaFileName, asJsonArray) {
	var jsonDataSet = polymorph.loadJsonDataSet(dataFileName, asJsonArray);
	var schema = polymorph.parseSchema(schemaFileName);
	polymorph.flattenJson(jsonDataSet, schema);
}
function processMergeCommand(dataFileName, mergeFileName, mergeFileName2) {
	var original = JSON.parse(fs.readFileSync(dataFileName, 'utf-8'));
	var updates = JSON.parse(fs.readFileSync(mergeFileName, 'utf-8'));
	var updates2 = mergeFileName2 ? JSON.parse(fs.readFileSync(mergeFileName2, 'utf-8')) : null;

	var merged = polymorph.mergeObjects(original, updates);
	if (updates2) {
		merged = polymorph.mergeObjects(merged, updates2);
	}

	console.log(pd.json(merged));
}


function processCommandLine() {
	var validCommands = ["--parse", "schema", "--compile", "compile", "--js2json", "normalize", "--csv2json", "json",
								"--types", "discover", "--json2schema", "--json2schema2", "--keys", "keys", "headers", "--values", "values",
								"--dimensions", "dimensions", "--json2csv", "csv", "--flatten", "flatten", "extrude",
								"--extrude", "--merge", "merge", "--help", "help", "?"];

	var args = process.argv || [];
	var argStart = (args[1] == "debug") ? 3 : 2;
	//var command = (args.length > (argStart)) ? args[argStart] : "unknown";
	var command = "unknown";
	var fileParameters = [];
	var asJsonArray = false;
	var useAlphaSort = false;
	var skipHeader = false;

	var fieldSeparator = ',';
	_.each(args.slice(argStart), function(a) {
		var as = a.toLocaleLowerCase();
		if ("--jsonarray" === as) { asJsonArray = true; }
		else if ("--sort" === as) { useAlphaSort = true; }
		else if ("--tsv" === as) { fieldSeparator = '\t'; }
		else if ("--skipheader" === as) { skipHeader = true; }
		else if (validCommands.indexOf(as) < 0) { fileParameters.push(a); }
		else { command = as; }
	});
	var p1 = (fileParameters.length > 0) ? fileParameters[0] : null;
	var p2 = (fileParameters.length > 1) ? fileParameters[1] : null;
	var p3 = (fileParameters.length > 2) ? fileParameters[2] : null;
	var p4 = (fileParameters.length > 3) ? fileParameters[3] : null;

	switch(command) {
		case "--parse": case "schema": processSchemaCommand(p1); break;
		case "--compile": case "compile": processCompileCommand(p1); break;
		case "--js2json": case "normalize": processNormalizeCommand(p1, asJsonArray); break;
		case "--csv2json": case "json": processJsonCommand(p1, p2, p3, asJsonArray, fieldSeparator, skipHeader); break;
		case "--extrude": case "extrude": processExtrudeJsonCommand(p1, p2, p3, asJsonArray); break;
		case "--types": case "discover": processDiscoverCommand(p1, useAlphaSort, asJsonArray); break;
		case "--json2schema": processJson2SchemaCommand(p1, useAlphaSort, asJsonArray); break;
		case "--json2schema2": processJson2Schema2Command(p1, useAlphaSort, asJsonArray); break;
		case "--keys": case "keys": case "headers": processHeadersCommand(p1, asJsonArray); break;
		case "--values": case "values": processValuesCommand(p1, p2, p3, p4, asJsonArray); break;
		case "--dimensions": case "dimensions": processDimensionsCommand(p1, asJsonArray); break;
		case "--json2csv": case "csv": processCsvCommand(p1, p2, asJsonArray); break;
		case "--flatten": case "flatten": processFlattenCommand(p1, p2, asJsonArray); break;
		case "--merge": case "merge": processMergeCommand(p1, p2, p3); break;
		case "--help": case "help": case "?": default: usage(); break;
	}
}

// Now execute the command handler
processCommandLine();
