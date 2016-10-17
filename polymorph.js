var fs = require('fs');
var _ = require('underscore');
var pd = require('pretty-data').pd;
var CSV = require('csv-string');
var util = require('util');
var moment = require('moment');
var phoneFormatter = require('phone-formatter');

var Slate = {};
Slate.StreamingFileProcessor = require("./StreamingFileProcessor.js").StreamingFileProcessor;

exports.NormalizePhoneNumber = NormalizePhoneNumber;
exports.NormalizeDate = NormalizeDate;
exports.loadDataMapping = loadDataMapping;
exports.loadFieldMapping = loadFieldMapping;
exports.mapRecord = mapRecord;
exports.makeFlat = makeFlat;
exports.makeStructured = makeStructured;
exports.isObject = isObject;
exports.mergeObjects = mergeObjects;
exports.jsonToCsv = jsonToCsv;
exports.convertJsonToCsv = convertJsonToCsv;
exports.flattenJson = flattenJson;
exports.convertStringToInt = convertStringToInt;
exports.convertCsvToJson = convertCsvToJson;
exports.convertTsvToJson = convertTsvToJson;
exports.parseJsonRecords = parseJsonRecords;
exports.parseSchema = parseSchema;
exports.loadJsonDataSet = loadJsonDataSet;
exports.loadJsonDataSetAsync = loadJsonDataSetAsync;

var DefaultPhoneFormatString = "NNN-NNN-NNNN";
var DefaultDateFormatString = 'YYYY-MM-DDTHH:MM:SS.000Z';

var DefaultLineFeedSplit = /\n\r|\r\n|\r|\n/;
var DefaultLineFeedSplitGlobal = /\n\r|\r\n|\r|\n/g;

// Utility Methods
function ISODate(dt) {
	return new Date(dt);
}

function unwrapLine(line) {
	var n = 0;
	var c = null;
	for (var x = 1; (x <= 2) && (line.length >= x); x++) {
		c = line[line.length - x];
		if ((c == '\n') || (c == '\r')) { n++; }
	}
	return (line && line.length > n) ? line.substr(0, line.length-n) : line;
}

var NormalizePhoneNumber = function (phone, formatString) {
	if (!phone) return null;

	var containsExtensionCharacter = phone.toLowerCase().indexOf('x') >= 0;

	// Resolve the issue of a forward slash separator
	phone = phone.replace(/\//g, '-');

	// If this is a super short number, report it
	if (phone.length < 7) {
		phone = null;
	}
	// If this is 7 or 8 digits, it might be: NNN-NNNN or NNNNNNN, and we could, in theory, deduce the prefix code.
	// This is not supported for now, so we return null.
	else if ((phone.length === 7) || (phone.length === 8)) {
		if (!containsExtensionCharacter) {
			// This is a number without an area code;
		}
		phone = null;
	}
	// If this is a longer phone number and it contains an X character, then we try to parse the extension in a special way
	else if ((phone.length > 10) && containsExtensionCharacter) {
		var extensionTerms = /[ ]?[EXTENSION|EXT|EX|X][\.]?[ ]?/;
		var parts = phone.toUpperCase().split(extensionTerms, 10); // No more than 10 splits to be safe
		var goodparts = _.filter(parts, function (part) {
			part = part.trim();
			return part.length > 0;
		});
		var baseNumber = parts[0];
		var extension = (goodparts.length > 1) ? goodparts[goodparts.length - 1] : null;

		/* Examples of the formatting scenarios
		 N EX PPPP
		 N EX. PPPP
		 N EXPPPP
		 N EXT PPPP
		 N EXT. PPPP
		 N X PPPP
		 N X.PPPP
		 N XPPPP
		 NXPPPP
		 */

		if (extension) {
			phone = phoneFormatter.format(baseNumber, (formatString || DefaultPhoneFormatString)) + " x" + extension;
		}
		else {
			//console.log("%s based on %s (unsupported scenario due to unrecognized extension)", phoneFormatter.normalize(phone), phone);
			phone = null;
		}
	}
	// Otherwise try a normal phone number formatting
	else {
		phone = phoneFormatter.format(phone, (formatString || DefaultPhoneFormatString));
	}

	return phone;
};

var NormalizeDate = function(s, format) {
	var d = null;
	try { d = moment(s, (format || DefaultDateFormatString)).toDate(); } catch(e) { console.log("Error parsing %s to Date", s); }
	return d;
};

// File Loading for Datasets, Schemas, and Mappings
function loadJsonDataSetAsync(dataFileName, asJsonArray, max, start, cb) {
	var lines = [];
	var targetLineCount = 0;
	var streamProcessor = new Slate.StreamingFileProcessor();
	var parseContext = {
		inputFile: dataFileName,
		outputFormat: null,
		mode: null,
		lineTerminator: '\n',
		fieldSeparator: null,
		start: start,
		max: max,
		maxLineDisplay: null,
		lineNumber: 0,
		columns: null,
		encoding: 'utf-8',
		fileType: 'json',
		asObject: true,
		keyFieldName: null
	};

	var LineRead = function (err, record, stats) {
		lines.push(record);
	};

	var LineError = function (err, stats) {
		console.log("%d:\t%s", stats.processed, "Error");
	};

	var FileStarted = function (err, filename, stats) {
		console.log("Reading file %s\n", filename);
	};

	var FileCompleted = function (err, filename, stats) {
		targetLineCount = Math.min(targetLineCount, stats.lines);
		if (cb) { cb(err, { data: lines, isArray: true }); }
	};

	var FileError = function (err, filename) {
		console.log("Error reading file %s: %s", filename, err||"<Unknown Error>");
	};

	streamProcessor.ParseFile(parseContext, LineRead, LineError, FileError, FileStarted, FileCompleted);

	//return { data: jsonData, isArray: isArray };
}
function loadJsonDataSet(dataFileName, asJsonArray) {
	var jsonData = fs.readFileSync(dataFileName, 'utf8').trim();
	var isArray = (jsonData[0] == '[') && (jsonData[jsonData.length-1] == ']');
	if (!isArray) {
		var numberNewlines = jsonData.split(DefaultLineFeedSplit).length;
		if (asJsonArray || (numberNewlines > 1)) {
			jsonData = '[' + jsonData.replace(DefaultLineFeedSplitGlobal, ',') + ']';
			isArray = true;
		}
	}
	return { data: jsonData, isArray: isArray };
}
function loadDataMapping(file) {
	var mapping = null;
	if (_.isString(file) && file.length > 0) {
		var mapData = fs.readFileSync(file, 'utf8');
		mapping = {};
		var lines = mapData.split(DefaultLineFeedSplit);

		//var headers = lines[0].split('\t');
		var rows = lines.slice(1);
		_.each(rows, function(row) {
			var fields = row.split('\t');
			if (fields.length < 6) fields.push('');
			mapping[fields[0]] = _.object(['target','source','default','transforms', 'iterator', 'arg1'], fields);
		})
	}
	return mapping;
}
function loadFieldMapping(file) {
	var mapping = null;
	if (_.isString(file) && file.length > 0) {
		var mapData = fs.readFileSync(file, 'utf8');
		mapping = {};
		//var lines = mapData.split('\n');
       var lines = mapData.split(DefaultLineFeedSplit);
		_.each(lines, function(row) {
			var fields = row.split('\t');
			if (fields.length === 2) { mapping[fields[0]] = fields[1]; }
		})
	}
	return mapping;
}

// Converting JSON to Flat Formats
function makeFlat(record, v, k, p) {
	var s = (p || []);
	if (k != undefined) s.push(k);

	_.each(v, function(v1, k1) {
		try {
			if (_.isObject(v1)) {
				makeFlat(record, v1, k1, s);
			}
			else {
				s.push(k1);
				record[s.join(".")] = v1;
				s.pop();
			}
		}
		catch(e) {
			debugger;
		}
	});
	if (s.length > 0) { s.pop() }
}
function makeStructuredUsingDataMapping(schema, row, obj, fieldNameMappings, dataMapping) {
	var schemaMap = _.indexBy(schema, "name");

	//var targetFieldNames = _.filter(_.keys(dataMapping), function(v) { return v; });
	var resolvedMapping = dataMapping || schemaMap;
        var targetFieldNames = _.filter(_.keys(resolvedMapping), function(v) { return v; });

	_.each(targetFieldNames, function(targetFieldName, x) {
		//var fieldMapping = dataMapping[targetFieldName];
		var fieldMapping = resolvedMapping[targetFieldName];
                fieldMapping.source = fieldMapping.source || fieldMapping.name;
		var field = schemaMap[fieldMapping.source];

		var colValue = row[x];

		var type = field ? field.type : null;
		var format = field ? field.format : null;
		var header = null;
		var mappedHeader = null;
		var val = null;

		if (_.isObject(colValue) && !_.isArray(colValue)) {
			header = colValue.key;
			mappedHeader = fieldNameMappings ? (fieldNameMappings[header] || header) : header;
			val = colValue.value;
		}
		else {
			header = targetFieldName;
			mappedHeader = fieldNameMappings ? (fieldNameMappings[header] || header) : header;
			val = colValue;
		}

		if (val) {
			try {
				if ((type == "int") || (type == "integer")) {
					val = parseInt(val);
				}
				else if ((type == "float") || (type == "numeric")) {
					val = parseFloat(val);
				}
				else if ((type == "bool") || (type == "boolean")) {
					val = (val.toLowerCase() == "true");
				}
				else if ((type == "date") || (type == "datetime")) {
					var val2 = NormalizeDate(val, format);
					val = new Date(val2);
				}
				else if (type == "phone") {
					var val2 = NormalizePhoneNumber(val, format);
					val = val2;
				}
			} catch (e) {
			}

			//console.log(header);
			var subFields = mappedHeader.split(".");
			if (subFields.length <= 1) {
				obj[mappedHeader] = val;
			}
			else {
				var ptr = obj;
				var lastPtr = null;
				for (var j = 0; j < subFields.length; j++) {
					var subField = subFields[j];
					if (j == subFields.length - 1) {
						ptr[subField] = val;
						//if (_.isObject(ptr)) { ptr[field] = val; }
					}
					if ((subField.length == 1) && (subField <= "9") && (subField >= "0")) {
						var arrayField = subFields[j - 1];
						var arrayIndex = convertStringToInt(subField);
						//console.log("%s\t%s\t%s", arrayIndex, arrayField, fieldName);
						if (!_.isNull(lastPtr)) {
							if (!_.isArray(lastPtr[arrayField])) {
								lastPtr[arrayField] = [];
								ptr = lastPtr[arrayField];
							}
						}
						if (!_.isNull(ptr)) {
							if (j == subFields.length - 1) {
								ptr[arrayIndex] = val;
							}
							else {
								ptr[arrayIndex] = (ptr[arrayIndex] || {});
							}
							lastPtr = ptr;
							ptr = ptr[arrayIndex];
						}
					}
					else if (j == subFields.length - 1) {
						if (_.isObject(ptr)) {
							ptr[subField] = val;

						}
					}
					else {
						//console.log("%s\t%s", index, field);
						if (_.isObject(ptr)) {
							ptr[subField] = (ptr[subField] || {});
							lastPtr = ptr;
							ptr = ptr[subField];
						}
					}
				}
			}
		}
	});
}
function makeStructured(schema, row, obj, fieldNameMappings) {
	_.each(schema, function(field, index) {
		var colValue = row[index];

		var type = field.type;
		var format = field ? field.format : DefaultPhoneFormatString;
		var header = null;
		var mappedHeader = null;
		var val = null;

		if (_.isObject(colValue) && !_.isArray(colValue)) {
			header = colValue.key;
			mappedHeader = fieldNameMappings ? (fieldNameMappings[header] || header) : header;
			type = field.type;
			val = colValue.value;
		}
		else {
			header = field.name;
			mappedHeader = fieldNameMappings ? (fieldNameMappings[header] || header) : header;
			type = field.type;
			val = row[index];
		}
		if (val) {
			try {
				if ((type == "int") || (type == "integer")) { val = parseInt(val); }
				else if ((type == "float") || (type == "numeric")) { val = parseFloat(val); }
				else if ((type == "bool") || (type == "boolean")) { val = (val.toLowerCase() == "true"); }
				else if ((type == "date") || (type == "datetime")) { var val2 = NormalizeDate(val, format); val = new Date(val2); }
				else if (type == "phone") { var val2 = NormalizePhoneNumber(val, format); val = val2; }
			} catch(e) {}

			//console.log(header);
			var subFields = mappedHeader.split(".");
			if (subFields.length <= 1) {
				obj[mappedHeader] = val;
			}
			else
			{
				var ptr = obj;
				var lastPtr = null;
				for (var j = 0; j < subFields.length; j++) {
					var subField = subFields[j];
					if (j == subFields.length-1) {
						ptr[subField] = val;
						//if (_.isObject(ptr)) { ptr[field] = val; }
					}
					if ((subField.length == 1) && (subField <= "9") && (subField >= "0")) {
						var arrayField = subFields[j-1];
						var arrayIndex = convertStringToInt(subField);
						//console.log("%s\t%s\t%s", arrayIndex, arrayField, fieldName);
						if (!_.isNull(lastPtr)) {
							if (!_.isArray(lastPtr[arrayField])) {
								lastPtr[arrayField] = [];
								ptr = lastPtr[arrayField];
							}
						}
						if (!_.isNull(ptr)) {
							if (j == subFields.length-1) { ptr[arrayIndex] = val; }
							else { ptr[arrayIndex] = (ptr[arrayIndex]||{}); }
							lastPtr = ptr;
							ptr = ptr[arrayIndex];
						}
					}
					else if (j == subFields.length-1) {
						if (_.isObject(ptr)) {
							ptr[subField] = val;

						}
					}
					else {
						//console.log("%s\t%s", index, field);
						if (_.isObject(ptr)) {
							ptr[subField] = (ptr[subField] || {});
							lastPtr = ptr;
							ptr = ptr[subField];
						}
					}
				}
			}
		}
	});
}
function isObject(v) {
	return (v != null ? v.constructor : void 0) === {}.constructor;
}
function mergeObjects(target, source) {
	return _.reduce(source, function(a, v, k) {
		a[k] = (k in target) && isObject(target[k]) && isObject(v) ? mergeObjects(target[k], v) : v;
		return a;
	}, target);
}
function jsonToCsv(json) {
	var record = {};
	if (_.isArray(json)) {
		_.each(json, function(subprofile) {
			makeFlat(record, subprofile, null, []);
		});
	}
	else if (_.isObject(json))
	{
		makeFlat(record, json, null, []);
	}
	else {
		record = json;
	}
	return record;
}

function convertJsonToCsv(jsonDataSet, schema) {
	var jsonRecords = parseJsonRecords(jsonDataSet);

	// Do the headers first
	var schemaHeaders = _.pluck(schema, "name");
	var row = CSV.stringify(schemaHeaders, ',');
	row = unwrapLine(row);
	console.log(row);

	// Now do the CSV file	
	_.each(jsonRecords, function(obj) {
		var record = jsonToCsv(obj);
		debugger;
		var headers = _.keys(record);
		var exportRecord = _.object(schemaHeaders, []);
		_.each(headers, function(header) {
			if (schemaHeaders.indexOf(header) >= 0) {
				var val = record[header];
				if (_.isString(val)) { val = val.replace(/[\n]|[\r]/g, ""); }
				exportRecord[header] = val;
			}
		});
		var row = CSV.stringify(exportRecord, ',');
		row = unwrapLine(row);
		console.log(row);
	})
}
function flattenJson(jsonDataSet, schema) {
	var jsonRecords = parseJsonRecords(jsonDataSet);

	// Do the headers first
	var schemaHeaders = []; _.each(schema, function(field) { schemaHeaders.push(field.name); });

	// Now do the CSV file
	_.each(jsonRecords, function(obj) {
		var record = jsonToCsv(obj);
		debugger;
		var headers = _.keys(record);
		var exportRecord = _.object(schemaHeaders, []);
		_.each(headers, function(header) {
			if (schemaHeaders.indexOf(header) >= 0) {
				var val = record[header];
				if (_.isString(val)) { val = val.replace(/[\n]|[\r]/g, ""); }
				console.log("%s\t%s", header, val);
				exportRecord[header] = val;
			}
		});
		//console.log(util.inspect(exportRecord, false, null));
		//console.log(util.inspect(exportRecord, false, null));
	})
}

// Converting Flat Formats to JSON
function convertStringToInt(candidateString) {
	var index = -1;
	var s = _.isString(candidateString) ? candidateString.trim() : null;
	if (_.isFinite(s) && (s.length <= 6)) {
		try { index = parseInt(s); } catch(e) {}
	}
	return index;
}
function convertCsvToJson(csvData, schema, mapping, asJsonArray, fieldSeparator, skipHeader) {
	//_.each(headers, function(header, i) { console.log("%s\t%s\t%s\t%s", i, header, row[i], map[header] ? map[header] : ""); });

	var max = 10000000;
	if (asJsonArray) { console.log("["); }

	var rows = csvData.split(DefaultLineFeedSplit);

	debugger;

	var headers = schema ? _.pluck(schema, "name") : null;

	if (skipHeader) {
		var fileHeaders = rows[0].split(fieldSeparator || ',');
		headers = headers || fileHeaders;
	}
	else {
		var cols = rows[0].split(fieldSeparator || ',');
		headers = headers || _.map(_.range(cols.length), function(i) { return "Col" + i; });
	}

	var numberRows = rows.length - 1 - (skipHeader ? 1 : 0);
   var resolvedMapping = mapping || schema;

	_.each(rows.slice(skipHeader ? 1 : 0), function(row, index) {
		if (index >= max) process.exit(0);
		var cols;
		if (fieldSeparator === ',') {
			cols = CSV.parse(row, fieldSeparator)[0] || [];
		}
		else {
			cols = row.split(fieldSeparator);
		}
		var isNormal = (cols.length > 1) && (cols.length <= headers.length);

		var colsMapped = mapping ? mapRecord(cols, headers, mapping) : cols;

		//for (var k = 0; k < cols.length; k++) { console.log("%d\t%d\t%s -> %s\t%s -> %s", index, k, headers[k], colsMapped[k].key, cols[k], colsMapped[k].value); }

		if (isNormal) {
			var obj = {};
			//makeStructuredUsingDataMapping(schema, colsMapped, obj, null, mapping);
			//makeStructuredUsingDataMapping(schema, row, obj, fieldNameMappings, dataMapping) {
         makeStructuredUsingDataMapping(schema, colsMapped, obj, null, mapping);

			if (asJsonArray) {
				console.log("%s%s", pd.json(obj), (index < (numberRows-1)) ? "," : "" );
			}
			else {
			   console.log(JSON.stringify(obj));
				//console.log(pd.json(obj));
			}
		}
	});
	if (asJsonArray) { console.log("]"); }
}
function convertTsvToJson(flattenedData, schema, mapping, asJsonArray) {
	//_.each(headers, function(header, i) { console.log("%s\t%s\t%s\t%s", i, header, row[i], map[header] ? map[header] : ""); });

	var max = 10000000;
	if (asJsonArray) { console.log("["); }
	var rows = flattenedData.split(DefaultLineFeedSplit);
	var numberRows = rows.length-1;
	var values = [];
	var keys = [];
	_.each(rows, function(row, index) {
		if (index >= max) process.exit(0);
		var cols = row.split('\t');
		if (cols.length === 2) {
			keys.push(cols[0]);
			values.push(cols[1]);
		}
	});

	if (values.length > 0) {
		var obj = {};

		makeStructured(schema, values, obj, mapping);

		if (asJsonArray) {
			console.log("%s%s", pd.json(obj), (index < (numberRows-1)) ? "," : "" );
		}
		else {
			console.log(JSON.stringify(obj));
			//console.log(pd.json(obj));
		}
	}
	if (asJsonArray) { console.log("]"); }
}

// Data Parsing
function parseJsonRecords(jsonDataSet) {
	var jsonRecords = [];
	var jsonData = jsonDataSet.data;
	var asJsonArray = jsonDataSet.isArray;
	if (_.isArray(jsonData)) { return jsonData; }

	if (!asJsonArray) {
		//var jsonLines = jsonData.split('\n');
		var jsonLines = jsonData.split(DefaultLineFeedSplit);
		_.each(jsonLines, function (json) {
			var trim = json.trim();
			if (trim) {
				try {
					var j = JSON.parse(trim);
					if (j) {
						jsonRecords.push(j);
					}
				}
				catch (e) {
					try {
						trim = trim.replace(/NaN/g, null);
						var j = JSON.parse(trim);
						if (j) {
							jsonRecords.push(j);
						}
					}
					catch (e) {
					}
				}
			}
		});
	}
	else {
		try {
			jsonRecords = JSON.parse(jsonData);
		}
		catch (e) {
			try {
				jsonData = jsonData.replace(/NaN/g, null);
				jsonRecords = JSON.parse(jsonData);
			}
			catch (e) {
			}
		}
	}
	return jsonRecords;
}

// Schema Management
function parseSchema(schemaFile) {
	// BugBug: This is broken on array processing because it should deal with the leaf nodes first.
	// Example: matrix.0.0.0, matrix.0.0.1, matrix.0.0.2, matrix.0.1.0, matrix.0.1.1, matrix.0.1.2, etc.
	// Current: matrix.0.0.0, matrix.1.0.0, matrix.0.1.0, matrix.1.1.0, matrix.0.2.0, matrix.1.2.0, and we need to start from the end, not the start.
	var schema = fs.readFileSync(schemaFile, 'utf8') || [];
	var fields = [];
	var field = null;
	var total = 0;
	var k = 0;

	// Format for schema:
	// Every line represents a flattened field name in the format of:
	// <field>.<subField1>.<subField2>.<finalSubField>[:<data_type>[:<data_formatter>]]
	// This would look like: {field: {subField1: {subField2: {finalSubField: <VALUE_GOES_HERE> } } } }
	// Arrays are supported using: <field>.<subField1>.[].<childObjectSubField1>...
	// If the optional data type is provided, only the following are supported: int, float, bool, date

	/* Example Schema:
	 id:int
	 name
	 codes.[12]
	 nocodes.[0]
	 object.field1:int
	 object.field2:string
	 object.field3:bool
	 object.field4:datime
	 object.field5.[5]:int
	 object.field5.[5]:phone:NNN-NNN-NNNN
	 object.field6.subfield6a.subfieldsa1.subfield6a1a.subfield6a1a1
	 */

	var rows = schema.split(DefaultLineFeedSplit);
	//var rows = schema.split('\n');
	var arrayParser = /\[[0-9]*\]/g;
	_.each(rows, function (row) {
		var type = "string";
		var format = null;
		var parts = row.split(':');
		if (parts.length >= 2) {
			type = parts[1].trim().toLowerCase();
		}
		if (parts.length >= 3) {
			format = parts.slice(2).join(':').trim();
		}
		field = parts[0];
		var fieldRows = [];

		var subFields = field.split('.');
		for (var i = 0; i < subFields.length; i++) {
			var subField = subFields[i];

			var match = subField.match(arrayParser);
			if (match) {
				var arrayLengthStr = match[0].slice(1, match[0].length - 1);
				var arrayLength = parseInt(arrayLengthStr);
				var numberRows = fieldRows.length;

				if (arrayLength > 0) {
					var orig = [];
					for (k = 0; k < numberRows; k++) {
						orig[k] = fieldRows[k];
					}

					for (var x = 0; x < arrayLength; x++) {
						for (var y = 0; y < numberRows; y++) {
							fieldRows[numberRows * x + y] = orig[y] + "." + x;
						}
					}
				}
				else {
					fieldRows = [];
				}
			}
			else {
				if (fieldRows.length > 0) {
					for (k = 0; k < fieldRows.length; k++) {
						fieldRows[k] = (fieldRows[k] || "") + "." + subField;
					}
				}
				else {
					fieldRows.push(subField);
				}
			}
		}
		for (var z = 0; z < fieldRows.length; z++) {
			if (fieldRows[z] !== '') {
				var fieldSpec = {pos: total++, name: fieldRows[z], type: type};
				if (format) { fieldSpec.format = format; }
				fields.push(fieldSpec);
			}
		}
	});
	return fields;
}
function mapRecord(source, headers, mapping) {
	var target = [];
	//var targetHeaders = _.keys(mapping);
	var compiledMapping = {};

	// Pre-compile the mapping and expand it out
	_.each(mapping, function(map, key) {
		if (map.iterator) {
			var items = {};
			_.each(headers, function (header, z) {
				if (header.indexOf(map.iterator + ".") == 0) {
					var remainder = header.slice(map.iterator.length + 1);
					var parts = remainder.split('.');
					if (_.isFinite(parts[0])) {
						var itemKey = map.iterator + "." + parts[0];
						var subItemKey = parts.length > 1 ? parts.slice(1).join(".") : "";
						if (!items[itemKey]) {
							items[itemKey] = {};
							items[itemKey]["#"] = parts[0];
						}
						if (subItemKey) {
							items[itemKey][subItemKey] = z;
						}
					}
				}
			});

			_.each(items, function(item) {
				var newMap = _.clone(map);
				_.each(item, function(sub, subkey) {
					var templateId = "<" + subkey + ">";
					var templateData = (subkey == "#") ? sub : source[sub];
					if (templateData) {
						newMap.target = newMap.target.replace(templateId, templateData);
						newMap.source = newMap.source.replace(templateId, templateData);
					}
				});
				compiledMapping[newMap.target] = newMap;
			});
		}
		else {
			compiledMapping[key] = map;
		}
	});

	/*
	 _.each(compiledMapping, function(map, key) {
	 if (map.source) {
	 console.log("%s mapped to %s with value %s", map.target, map.source, source[headers.indexOf(map.source)]);
	 }
	 });
	 */

	// Now run the mapping
	_.each(compiledMapping, function (map) {
		var val = '';

		if ((map.target == "_id") && (source.length > 1) && (source[28])) {
			//console.log(map);
			//debugger;
		}

		if (map.source) {
			var sources = map.source.split('|');
			if (sources.length > 1) {
				val = [];
				_.each(sources, function(s) {
					var i = headers.indexOf(s);
					if (i >= 0) val.push(source[i] || '');
				});
			}
			else {
				var i = headers.indexOf(map.source);
				if (i >= 0) val = source[i] || '';
			}
		}
		if (map.default) val = val || map.default;
		if (map.transforms) {
			var transforms = map.transforms.split(',');
			_.each(transforms, function(transform) {

				// If we have an arg1 value (column 6), then we convert it to JSON and try using it
				var arg1 = map.arg1;
				if (arg1 && arg1.length >= 2) {
					var j = arg1;
					j = j.replace(/""/g, '"').slice((j[0]=='"' ? 1 : 0), (j[j.length-1] == '"') ? -1 : j.length);
					try { j = JSON.parse(arg1); arg1 = j; } catch(e) { };
				}
				var transformParts = transform.split('|');
				switch(transformParts[0]) {
					case "lower": if (_.isString(val) && val.length > 0) { val = val.toLowerCase(); } break;
					case "newid": val = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);}); break;
					case "splitcolon": if (_.isString(val) && val.length > 0) { val = val.split(':');} break;
					case "join": if (_.isArray(val)) { val = val.join(','); } break;
					case "int": if (_.isString(val)) { try { val = parseInt(val); } catch(e) {}; } break;
					case "map": if (_.isString(val)) { val = (arg1 || {})[val]; } break;
					case "format": debugger; if (_.isString(val) && arg1) { val = util.format(arg1, val); } break;
					case "concat": if (_.isArray(val)) { val = val.join(''); } break;
					case "last": if (_.isArray(val) && val.length > 0) { val = val[val.length-1];} break;
					case "now": val = new Date(); break;
					case "owner": val = val || "dev@circlmedia.com"; break;
					case "connectorid": val = val || "twitter_0.20.0"; break;
					case "jobid": val = val || ""; break;
					case "batchid": val = val || ""; break;
					case "recordid": val = val || "1"; break;
					case "requestid": val = val || ""; break;
				}
			});
		}
		target.push({key:map.target, value:val});
	});
	return target;
}
