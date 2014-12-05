var fs = require('fs');
var _ = require('underscore');
var pd = require('pretty-data').pd;
var CSV = require('csv-string');
var util = require('util');
var polymorph = require('./polymorph');


exports.isNumberStringWithDecimalPoint = isNumberStringWithDecimalPoint;
exports.processNumericValue = processNumericValue;
exports.analyzeDataType = analyzeDataType;
exports.buildDataDictionary = buildDataDictionary;
exports.analyzeJsonData = analyzeJsonData;
exports.discoverJsonKeys = discoverJsonKeys;
exports.discoverJsonSchema = discoverJsonSchema;
exports.discoverJsonSchema2 = discoverJsonSchema2;
exports.discoverJsonValueHistograms = discoverJsonValueHistograms;
exports.discoverJsonArrayDimensions = discoverJsonArrayDimensions;


// Data Type Discovery and Inference
function isNumberStringWithDecimalPoint(s) {
	var p1 = s.indexOf('.');
	var p2 = s.lastIndexOf('.');
	return (p1 >= 0) && (p1 === p2);
}
function processNumericValue(dt, value) {
	var hasDecimalPoint = false;
	var numericValue = null;
	try {
		if (typeof value == "number") {
			numericValue = value;
			hasDecimalPoint = isNumberStringWithDecimalPoint(value + "");
		}
		else if (_.isString(value) && _.isFinite(value)) {
			hasDecimalPoint = isNumberStringWithDecimalPoint(value);
			numericValue = hasDecimalPoint ? parseFloat(value) : parseInt(value);
			dt.minNumericCastValue = Math.min(dt.minNumericCastValue, numericValue);
			dt.maxNumericCastValue = Math.max(dt.maxNumericCastValue, numericValue);
			dt.countCastNumbers = (dt.countCastNumbers || 0) + 1;
			dt.castTotal = (dt.castTotal || 0) + numericValue;
			dt.castAverage = dt.castTotal / dt.countCastNumbers;
			if (hasDecimalPoint) { dt.countCastFloats = (dt.countCastFloats || 0) + 1; }
			else { dt.countCastIntegers = (dt.countCastIntegers || 0) + 1; }
		}
		else {
			numericValue = null;
		}

		if (numericValue !== null) {
			dt.hasNumber = true;
			dt.countNumbers = dt.countNumbers + 1;
			if (_.isFinite(numericValue)) {
				dt.countFiniteNumbers = (dt.countFiniteNumbers || 0) + 1;
				dt.numericTotal = (dt.numericTotal || 0) + numericValue;
				dt.numericAverage = dt.numericTotal / dt.countFiniteNumbers;
				dt.minNumericValue = Math.min(dt.minNumericValue, numericValue);
				dt.maxNumericValue = Math.max(dt.maxNumericValue, numericValue);
				if (hasDecimalPoint) {
					dt.countFloats = (dt.countFloats || 0) + 1;
					dt.hasFloat = true;
				}
				else {
					dt.countIntegers = (dt.countIntegers || 0) + 1;
					dt.hasInteger = true;
				}
			}
		}
	}
	catch (e) {
	}
}
function analyzeDataType(dataTypesTable, el, key) {
	var keyPartsInUse = [];
	var lastPathIsArray = false;
	var keyParts = key.split('.');
	var path = null;

	_.each(keyParts, function(fieldName, i, keyPartsCollection) {
		var processElement = !lastPathIsArray || (i >= (keyPartsCollection.length-1));
		//console.log("%s is %san array and %s is an item. Process=%s", path, lastPathIsArray ? "" : "NOT ", fieldName, processElement ? "Yes": "No");
		if (processElement) {
			keyPartsInUse.push(fieldName);
			path = keyPartsInUse.join(".");
			dataTypesTable[path] = dataTypesTable[path] || {};
			var dt = dataTypesTable[path];
			//console.log("%s\t%s\t%s", path, fieldName, el);
			if (!_.isUndefined(el)) {
				el = el[fieldName];
				if (_.isArray(el)) {
					dt.hasArray = true;
					dt.countArrays = (dt.countArrays || 0) + 1;
					lastPathIsArray = true;
				}
				else if (_.isObject(el)) {
					dt.hasObject = true;
					dt.countObjects = (dt.countObjects || 0) + 1;
				}
				else {
					if (el == "2") { debugger; }
					dt.values = dt.values || [];
					dt.values.push(el);
					if (_.isNull(el)) {
						dt.hasNull = true;
						dt.countNulls = (dt.countNulls || 0) + 1;
					}
					//typeof number
					if (_.isNumber(el)) {
						processNumericValue(dt, el);
					}
					if (_.isBoolean(el)) {
						dt.hasBoolean = true;
						dt.countBooleans = (dt.countBooleans || 0) + 1;
					}
					if (_.isDate(el)) {
						dt.hasDate = true;
						dt.countDates = (dt.countDates || 0) + 1;
					}
					if (_.isNaN(el)) {
						dt.hasNaN = true;
						dt.countNaNs = (dt.countNaNs || 0) + 1;
					}
					if (_.isString(el)) {
						dt.hasString = true;
						dt.countStrings = (dt.countStrings || 0) + 1;
						dt.minStringValue = dt.minStringValue || "";
						dt.maxStringValue = dt.maxStringValue || "";
						if (el < dt.minStringValue) {
							dt.minStringValue = el;
						}
						if (el > dt.maxStringValue) {
							dt.maxStringValue = el;
						}
						if (path == "friends_count") { debugger; }
						processNumericValue(dt, el);
					}
				}
			}
			else {
				dataTypesTable[path].hasUndefined = true;
				dataTypesTable[path].countUndefined = (dataTypesTable[path].countUndefined || 0) + 1;
			}
		} else {
			lastPathIsArray = false;
			//keyPartsInUse.push("[" + fieldName + "]");
			path = keyPartsInUse.join(".");
			if (!_.isUndefined(el)) {
				el = el[fieldName];
			}
			//console.log("%s is an array and %s is an item", path, fieldName);
		}
	});
}
function buildDataDictionary(obj, dataTypesTable, arrayMaxByField, includeCardinality) {
	var headers = _.keys(obj);
	var cardinalityIndex = _.sortBy(_.keys(arrayMaxByField), function(key) { return -key.length; });
	var dataDictionary = {};
	_.each(headers, function(header) {
		var fieldInfo = dataTypesTable[header];
		if (fieldInfo) {
			var expandedHeader = "#" + header;
			_.each(cardinalityIndex, function (key) {
				var searchToken = "#" + key;
				var foundAt = expandedHeader.indexOf(searchToken);
				if (foundAt >= 0 ) {
					var cardinality = arrayMaxByField[key];
					if (cardinality > 1) {
						if (searchToken.indexOf(".nocodes") >= 0) { debugger; }
						expandedHeader = expandedHeader.replace(searchToken, searchToken + ".[" + (includeCardinality ? cardinality : "") + "]");
						fieldInfo.arraySize = cardinality;
					}
				}
			});
			//dataDictionary.push(expandedHeader);
			//console.log("%d\t%s", i, expandedHeader);
			dataDictionary[expandedHeader.slice(1)] = fieldInfo;
		}
	});
	return dataDictionary;
}
function analyzeJsonData(jsonDataSet, flattenHeaders) {
	var arrayMaxByField = {};
	var dataTypesTable = {};
	var jsonRecords = polymorph.parseJsonRecords(jsonDataSet);
	var numSkipFields = 0;
	var all = {};

	_.each(jsonRecords, function(obj) {
		var record = polymorph.jsonToCsv(obj);
		debugger;
		var keys = _.keys(record);
		var newKeys = [];
		_.each(keys, function (key) {
			var newKeyParts = [];
			var keyParts = key.split('.');

			analyzeDataType(dataTypesTable, obj, key);

			_.each(keyParts, function (keyPart, index, origKeyParts) {
				var fieldKeyForArrayMax = ((newKeyParts.length > numSkipFields) ? newKeyParts.slice(numSkipFields) : newKeyParts).join(".");
				var fieldKeyForTypeInspection = origKeyParts.slice(0, index).join(".");

				var fieldTypeInfo = dataTypesTable[fieldKeyForTypeInspection];
				var isFieldAnArray = fieldTypeInfo && fieldTypeInfo.isArray;

				var cardinality = 1;

				if (_.isFinite(keyPart) || isFieldAnArray) {
					cardinality = parseInt(keyPart);
					//newKeyParts.push("*");
				}
				else {
					newKeyParts.push(keyPart);
				}
				arrayMaxByField[fieldKeyForArrayMax] = Math.max(arrayMaxByField[fieldKeyForArrayMax] || 1, cardinality);
			});
			if (newKeyParts.length > numSkipFields) {
				var nk = newKeyParts.slice(numSkipFields).join('.');
				if (newKeys.indexOf(nk) < 0) newKeys.push(nk);
			}
		});
		var normalizedRecord = _.object(newKeys, _.values(record));

		// Apply the defaults to get the merged set
		_.defaults(all, normalizedRecord);
	});

	debugger;

	var dataDictionary = buildDataDictionary(all, dataTypesTable, arrayMaxByField, !flattenHeaders);
	return {superSet: all, fieldTypes: dataDictionary, arraySizes: arrayMaxByField};
}
function discoverJsonKeys(jsonDataSet, flattenHeaders) {
	var results = analyzeJsonData(jsonDataSet, flattenHeaders);
	var headers = _.keys(results.superSet);
	_.each(headers, function(header) { console.log("%s", header); });
}
function discoverJsonSchema(jsonDataSet, flattenHeaders, asSchemaFormat, useAlphabeticalOrder) {
	debugger;
	var results = analyzeJsonData(jsonDataSet, flattenHeaders);
	var dataDictionary = results.fieldTypes;

	if (!asSchemaFormat) {
		var fieldNames = ["index", "object", "array", "string", "boolean",
			"date", "number", "float", "integer", "finite",
			"castedFloat", "nan", "null", "undefined", "arraySize",
			"countObjects", "countArrays", "countStrings",
			"countBooleans", "countDates",
			"countNumbers", "countFloats", "countIntegers",
			"countFiniteNumbers", "countCastNumbers",
			"countCastFloats", "countCastIntegers", "countNaNs",
			"countNulls", "countUndefined", "total", "average",
			"minNumericValue", "maxNumericValue", "castTotal",
			"castAverage", "minFloatCastValue", "maxFloatCastValue",
			"minStringValue", "maxStringValue", "field" ];
		console.log(fieldNames.join('\t'));
	}

	var typeIndex = null;
	var sortedIndex = null;
	var sortCounter = 0;

	if (useAlphabeticalOrder) {
		typeIndex = _.indexBy(_.keys(dataDictionary), function(v) { return v; });
		sortedIndex = _.keys(typeIndex).sort();
	}
	else {
		typeIndex = _.indexBy(_.keys(dataDictionary), function() { return sortCounter++; });
		sortedIndex = _.keys(typeIndex);
	}

	debugger;
	_.each(sortedIndex||[], function(skey, i) {
		var key = typeIndex[skey];
		var dt = dataDictionary[key];
		if (asSchemaFormat) {
			var bestType = "string";
			// If there are any real floats, we pick this type to be safe
			if (dt.hasDate) { bestType = "date"; }
			else if (dt.hasNumber) {
				if ((dt.countCastFloats > 0) || (dt.countFloats > 0)) { bestType = "float"; }
				else { bestType = "int"; }
			}
			else if (dt.hasBoolean) { bestType = "bool"; }
			console.log("%s:%s", key, bestType);
		}
		else {
			var typesDiscoveryValues = [
				i,
				dt.hasObject?1:0,
				dt.hasArray?1:0,
				dt.hasString?1:0,
				dt.hasBoolean?1:0,
				dt.hasDate?1:0,
				dt.hasNumber?1:0,
				dt.hasFloat?1:0,
				dt.hasInteger?1:0,
				(dt.countFiniteNumbers>0)?1:0,
				(dt.countCastNumbers>0)?1:0,
				dt.hasNaN?1:0,
				dt.hasNull?1:0,
				dt.hasUndefined?1:0,
						dt.arraySize||"",
						dt.countObjects||0,
						dt.countArrays||0,
						dt.countStrings||0,
						dt.countBooleans||0,
						dt.countDates||0,
						dt.countNumbers||0,
						dt.countFloats||0,
						dt.countIntegers||0,
						dt.countFiniteNumbers||0,
						dt.countCastNumbers||0,
						dt.countCastFloats||0,
						dt.countCastIntegers||0,
						dt.countNaNs||0,
						dt.countNulls||0,
						dt.countUndefined||0,
						dt.numericTotal||"",
						dt.numericAverage||"",
						dt.minNumericValue||"",
						dt.maxNumericValue||"",
						dt.castTotal||"",
						dt.castAverage||"",
						dt.minNumericCastValue||"",
						dt.maxNumericCastValue||"",
						dt.minStringValue||"",
						dt.maxStringValue||"",
				key
			];
			console.log(typesDiscoveryValues.join('\t'));
		}
	});
}
function discoverJsonSchema2(jsonDataSet, flattenHeaders, asSchemaFormat, asSchemaTree, useAlphabeticalOrder) {
	debugger;
	asSchemaTree = true;
	var results = analyzeJsonData(jsonDataSet, flattenHeaders);
	var dataDictionary = results.fieldTypes;

	if (!asSchemaFormat) {
		var fieldNames = ["index", "object", "array", "string", "boolean",
			"date", "number", "float", "integer", "finite",
			"castedFloat", "nan", "null", "undefined", "arraySize",
			"countObjects", "countArrays", "countStrings",
			"countBooleans", "countDates",
			"countNumbers", "countFloats", "countIntegers",
			"countFiniteNumbers", "countCastNumbers",
			"countCastFloats", "countCastIntegers", "countNaNs",
			"countNulls", "countUndefined", "total", "average",
			"minNumericValue", "maxNumericValue", "castTotal",
			"castAverage", "minFloatCastValue", "maxFloatCastValue",
			"minStringValue", "maxStringValue", "field" ];
		console.log(fieldNames.join('\t'));
	}

	var typeIndex = null;
	var sortedIndex = null;
	var sortCounter = 0;

	if (useAlphabeticalOrder) {
		typeIndex = _.indexBy(_.keys(dataDictionary), function(v) { return v; });
		sortedIndex = _.keys(typeIndex).sort();
	}
	else {
		typeIndex = _.indexBy(_.keys(dataDictionary), function() { return sortCounter++; });
		sortedIndex = _.keys(typeIndex);
	}

	debugger;
	var lastIndex = -1;
	_.each(sortedIndex||[], function(skey, i) {
		var key = typeIndex[skey];
		var dt = dataDictionary[key];

		var fieldParts = key.split('.');
		var newParts = [];
		var indent = 0;
		for (var z = 0; z < fieldParts.length; z++) {
			var fp = fieldParts[z];
			if (fp && _.isString(fp) && fp.length) {
				if (fp[0] == '$') { }
				else if (fp[0] == '[') {
//					newParts.push(fp.splice(1, fp.length-3));
					newParts.push("3");
					//fieldParts[z] = fp.splice(1, fp.length-3);
				}
				else {
					newParts.push(fp);
				}
			}
			key = newParts.join('.');
		};
		indent = newParts.length;

		var indentSpaces = '                                                                                                    ';
		if (asSchemaFormat) {
			var bestType = "string";
			// If there are any real floats, we pick this type to be safe
			if (dt.hasDate) { bestType = "date"; }
			else if (dt.hasNumber) {
				if ((dt.countCastFloats > 0) || (dt.countFloats > 0)) { bestType = "float"; }
				else { bestType = "int"; }
			}
			else if (dt.hasBoolean) { bestType = "bool"; }
			var spacesForIndent = indentSpaces.slice(0, indent*3);
			if (lastIndent !== indent) {
				//console.log("%s%s:%s", spacesForIndent, key, "object");
				console.log("%s%s:%s", spacesForIndent, key, bestType);
			}
			console.log("%s%s:%s", spacesForIndent, key, bestType);
		}
		else {
			var typesDiscoveryValues = [
				i,
				dt.hasObject?1:0,
				dt.hasArray?1:0,
				dt.hasString?1:0,
				dt.hasBoolean?1:0,
				dt.hasDate?1:0,
				dt.hasNumber?1:0,
				dt.hasFloat?1:0,
				dt.hasInteger?1:0,
				(dt.countFiniteNumbers>0)?1:0,
				(dt.countCastNumbers>0)?1:0,
				dt.hasNaN?1:0,
				dt.hasNull?1:0,
				dt.hasUndefined?1:0,
				dt.arraySize||"",
				dt.countObjects||0,
				dt.countArrays||0,
				dt.countStrings||0,
				dt.countBooleans||0,
				dt.countDates||0,
				dt.countNumbers||0,
				dt.countFloats||0,
				dt.countIntegers||0,
				dt.countFiniteNumbers||0,
				dt.countCastNumbers||0,
				dt.countCastFloats||0,
				dt.countCastIntegers||0,
				dt.countNaNs||0,
				dt.countNulls||0,
				dt.countUndefined||0,
				dt.numericTotal||"",
				dt.numericAverage||"",
				dt.minNumericValue||"",
				dt.maxNumericValue||"",
				dt.castTotal||"",
				dt.castAverage||"",
				dt.minNumericCastValue||"",
				dt.maxNumericCastValue||"",
				dt.minStringValue||"",
				dt.maxStringValue||"",
				key
			];
			console.log(typesDiscoveryValues.join('\t'));
		}
	});
}
function discoverJsonValueHistograms(jsonDataSet, flattenHeaders, maxValuesPerField, fieldList) {
	var maxValues = _.isFinite(maxValuesPerField) ? maxValuesPerField : 0;
	var results = analyzeJsonData(jsonDataSet, flattenHeaders);
	var dataDictionary = results.fieldTypes;

	var smartIndex = _.indexBy(_.keys(dataDictionary), function(v) { return v; });
	//var smartIndex = _.indexBy(_.keys(dataDictionary), function(v) { var subFields = v.split('.'); var numSubFields = subFields.length; return rightJustify(numSubFields) + ":" + v; });
	var alphaIndex = (fieldList ? _.intersection(fieldList, _.keys(smartIndex)) : _.keys(smartIndex)).sort();
	console.log(pd.json(alphaIndex));

	// Output data lines
	var output = [];

	_.each(alphaIndex||[], function(skey) {
		var key = smartIndex[skey];
		var info = dataDictionary[key];
		var values = (info ? info.values : null) || [];
		var valueCounts = _.countBy(values, function(v) { return v; });
		var sortedPairs = _.sortBy(_.pairs(valueCounts), function(v) { return -v[1]; });
		var selectedPairs = maxValues ? sortedPairs.slice(0, maxValues) : sortedPairs;

		// If we are showing sample values from each column, then we do that here.  Otherwise, show field and the freq.
		if (_.isArray(selectedPairs) && selectedPairs.length) {
			_.each(selectedPairs, function(p) { output.push(util.format("%s\t%d\t%s", skey, p[1], p[0])); });
		}
		else {
			output.push(util.format("%s\t%d", skey, sortedPairs.length));
		}
	});
	console.log(output.join("\n"));
}
function discoverJsonArrayDimensions(jsonDataSet, flattenHeaders) {
	var results = analyzeJsonData(jsonDataSet, flattenHeaders);
	_.each(results.arraySizes, function(max, key) {
		if (key && (max > 1)) { console.log("%d\t%s", max+1, key); }
	});
}
