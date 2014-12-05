// Sample by Scott Beaudreau, 06-16-2014
// Demonstrating stream-based processing of a large CSV file and handling the individual records as callbacks

var fs = require('fs');
var CSV = require('csv-string');
var _ = require('underscore');
var microtime = require('microtime');
var util = require('util');
var parseString = require('xml2js').parseString;
var pd = require('pretty-data').pd;
var path = require('path');
var moment = require('moment');

// Module Exports
exports.StreamingFileProcessor = StreamingFileProcessor;


function StreamingFileProcessor(application) {
	// Call base class constructor if any
	if (arguments.callee._baseClass) arguments.callee._baseClass.apply(this, arguments);

	// Static constructor
	(function constructorStatic(self, application) {
		self.lineCounter = 0;

	})(this, application);
}
StreamingFileProcessor.prototype.ParseFile = function(parseContext, onLineRead, onLineError, onFileError, onFileStarted, onFileCompleted) {
	var self = this;
	var stream = null;
	var fileData = '';

	var filename = parseContext.inputFile;
	var encoding = parseContext.encoding || 'utf-8';
	var fileType = parseContext.fileType || 'text';
	var lineTerminator = parseContext.lineTerminator || '\r\n';
	var fieldSeparator = parseContext.fieldSeparator || ((fileType === 'csv') ? ',' : null);
	var fields = parseContext.columns;
	var asObject = parseContext.asObject;
	var max = parseContext.max || 10000000;
	var start = parseContext.start || 0;
	var useSimpleCsv = parseContext.useSimpleCsv || false;
	var keyField = parseContext.keyFieldName;

	var parser = null;
	switch(fileType) {
		case 'json': parser = 'json';
			break;
		case 'csv': parser = (useSimpleCsv ? 'simplecsv' : 'csv');
			break;
		case 'text':
		default: parser = 'text';
			break;
	}

	var stats = {
		read: 0,
		processed: 0,
		errors: 0,
		skipped: 0,
		start: start,
		max: max,
		startTime: microtime.nowDouble(),
		bytes: 0,
		lines: 0
	};

	var fieldNames = (fields && (typeof fields === 'object') && (fields.length > 0)) ? fields : null;

	try {
		stream = fs.createReadStream(filename, {
			flags: 'r',
			encoding: encoding,
			fd: null,
			mode: 0666,
			bufferSize: 64 * 1024 * 16
		});
		self.OnStreamStarted(null, filename, stats, onFileStarted);
	}
	catch(e) {
		self.OnStreamError(e, filename, stats, onFileError);
		return;
	}

	var lineSplitHandlerJson = function(buffer, cb) {
		stats.bytes += buffer.length;

		var lines = buffer.split(lineTerminator);
		var k1 = lines.length;

		// If we don't have the headers, then grab them
		if (['csv', 'simplecsv', 'tsv'].indexOf(parser) >= 0) {
			if (!fieldNames && lines.length) {
				var headerLine = lines.length ? lines[0] : "";
				lines = lines.slice(1);
				k1--;
				if (useSimpleCsv) {
					fieldNames = headerLine.split(fieldSeparator);
				}
				else {
					fieldNames = CSV.parse(headerLine)[0];
				}
			}
		}

		// If we don't have the headers, then grab them
		var x1 = Math.max(0, Math.min(stats.start - stats.lines - 1, k1));
		var x2 = Math.max(0, Math.min(stats.max + stats.start - stats.lines - 1, k1));

		if ((x1 <= x2) && (x1 >= 0) && (x1 < k1)) {
			var skipBefore = Math.max(0, x1);
			var skipAfter = Math.max(0, k1 - x2);

			// For all of the lines we skip initially
			stats.lines += skipBefore;
			stats.skipped += skipBefore;

			for (var i = x1; i < x2; i++) {
				stats.lines++;
				stats.read++;

				var line = lines[i];

				if (parser === 'text') {
					var j = asObject ? {text: line} : line;
					cb(null, j);
				}
				else if (parser === 'json') {
					if (asObject) {
						try {
							if (line.trim().length > 1) {
								var j = JSON.parse(line);
								cb(null, j);
							}
							else { cb(null, null); }
						}
						catch (e) {
							try {
								var j = JSON.parse(line.replace(/NaN/g, null));
								cb(null, j);
							}
							catch (e) { cb({message: e, line: line}); }
						}
					}
					else { cb(null, line); }
				}
				else if ((parser === 'simplecsv') || (parser === 'csv')) {
					var values = (parser === 'simplecsv') ? line.split(fieldSeparator) : CSV.parse(line, fieldSeparator)[0];
					if (values && !((values.length === 1) && (values[0] === ""))) {
						if (!useSimpleCsv && (values.length !== fieldNames.length)) {
							cb({message: "Fields do not match expected headers", headers: fieldNames, values: values, line: line});
						}
						else {
							var record = asObject ? _.object(fieldNames, values) : values;
							cb(null, record);
						}
					}
				}
			}

			// For all of the lines we skip after
			stats.lines += skipAfter;
			stats.skipped += skipAfter;
		}
		else {
			stats.lines += k1;
			stats.skipped += k1;
		}
	};
	var lineSplitHandler = function(buffer, cb) {
		stats.bytes += buffer.length;

		var lines = buffer.split(lineTerminator);
		var k1 = lines.length;

		// If we don't have the headers, then grab them
		if (!fieldNames && lines.length) {
			var headerLine = lines.length ? lines[0] : "";
			lines = lines.slice(1); k1--;
			if (useSimpleCsv) { fieldNames = headerLine.split(fieldSeparator); }
			else { fieldNames = CSV.parse(headerLine)[0]; }
		}

		var x1 = Math.max(0, Math.min(stats.start - stats.lines - 1, k1));
		var x2 = Math.max(0, Math.min(stats.max + stats.start - stats.lines - 1, k1));

		if ((x1 <= x2) && (x1 >= 0) && (x1 < k1)) {
			var skipBefore = Math.max(0, x1);
			var skipAfter = Math.max(0, k1 - x2);

			// For all of the lines we skip initially
			stats.lines += skipBefore;
			stats.skipped += skipBefore;

			for (var i = x1; i < x2; i++) {
				stats.lines++;
				stats.read++;

				var line = lines[i];

				var values = null;
				if (useSimpleCsv) {
					values = line.split(fieldSeparator);
				}
				else {
					values = CSV.parse(line, fieldSeparator)[0];
					//values = CSV.parse(line, fieldSeparator).shift();
				}

				if ((values.length === 1) && (values[0] === "")) {}
				else {
					if (!useSimpleCsv && (values.length !== fieldNames.length)) {
						cb({message: "Fields do not match expected headers", headers: fieldNames, values: values, line: line});
					}
					else {
						var record = asObject ? _.object(fieldNames, values) : values;
						cb(null, record);
					}
				}
			}

			// DEBUG ONLY: for (var i = x2; i < k1; i++) { console.log("%d:%s\t%s", stats.lines + (i - x2) + 1, "", "SKIPPED"); }

			// For all of the lines we skip after
			stats.lines += skipAfter;
			stats.skipped += skipAfter;
		}
		else {
			stats.lines += k1;
			stats.skipped += k1;
		}
	};

	var processChunk = function(buffer) {
		lineSplitHandlerJson(buffer, function(err, record) {
			if (err) {
				stats.errors++;
				self.OnRecordError(err, record, stats, onLineError);
			}
			else {
				try { self.OnRecord(err, record, stats, onLineRead); stats.processed++; } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
			}
		});
	};

	stream.on('data', function (data) {
		fileData += data;
		var i = fileData.lastIndexOf(lineTerminator);
		var buffer = null;

		if (i >= 0) {
			buffer = fileData.slice(0, i);
			fileData = fileData.slice(i + 1);
			processChunk(buffer);
		}
	});

	stream.on('error', function (err) {
		self.OnStreamError(err, filename, stats, onFileError);
	});

	stream.on('end', function () {
		processChunk(fileData);
		self.OnStreamFinished(null, filename, stats, onFileCompleted);
	});
};
StreamingFileProcessor.prototype.ParseFileSync = function(parseContext, onLineRead, onLineError, onFileError, onFileStarted, onFileCompleted) {
	var self = this;
	var data = null;

	var filename = parseContext.inputFile;
	var encoding = parseContext.encoding || 'utf-8';
	var fileType = parseContext.fileType || 'text';
	var lineTerminator = parseContext.lineTerminator || '\r\n';
	var fieldSeparator = parseContext.fieldSeparator || ((fileType === 'csv') ? ',' : null);
	var fields = parseContext.columns;
	var asObject = parseContext.asObject;
	var max = parseContext.max || 10000000;
	var start = parseContext.start || 0;
	var useSimpleCsv = parseContext.useSimpleCsv || false;
	var keyField = parseContext.keyFieldName;

	var parser = null;
	switch(fileType) {
		case 'json': parser = 'json';
			break;
		case 'csv': parser = (useSimpleCsv ? 'simplecsv' : 'csv');
			break;
		case 'text':
		default: parser = 'text';
			break;
	}

	var stats = {
		read: 0,
		processed: 0,
		errors: 0,
		skipped: 0,
		start: start,
		max: max,
		startTime: microtime.nowDouble(),
		bytes: 0,
		lines: 0
	};

	var fieldNames = (fields && (typeof fields === 'object') && (fields.length > 0)) ? fields : null;

	try {
		self.OnStreamStarted(null, filename, onFileStarted);
		data = fs.readFileSync(filename, encoding);
	}
	catch(e) {
		self.OnStreamError(e, filename, stats, onFileError);
		return;
	}

	stats.bytes += data.length;
	var lines = data.split(lineTerminator);

	// If we don't have the headers, then grab them
	if (['csv', 'simplecsv', 'tsv'].indexOf(parser) >= 0) {
		if (!fieldNames && lines.length) {
			var headerLine = lines.length ? lines[0] : "";
			lines = lines.slice(1);
			if (useSimpleCsv) {
				fieldNames = headerLine.split(fieldSeparator);
			}
			else {
				fieldNames = CSV.parse(headerLine)[0];
			}
		}
	}

	var numberLines = lines.length;

	var x1 = Math.max(0, Math.min(stats.start, numberLines-1));
	var x2 = Math.max(x1, Math.min(x1 + stats.max, numberLines-1));

	stats.lines += x1;
	stats.skipped += x1;

	for (var i = x1; i < x2; i++) {
		stats.lines++;
		stats.read++;

		var line = lines[i];
		var values = null;

		if (parser === 'text') {
			var j = asObject ? {text: line} : line;
			stats.processed++;
			try { self.OnRecord(null, j, stats, onLineRead); } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
		}
		else if (parser === 'json') {
			if (asObject) {
				try {
					if (line.trim().length > 1) {
						var j = JSON.parse(line);
						stats.processed++;
						try { self.OnRecord(null, j, stats, onLineRead); } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
					}
					else {
						stats.processed++;
						try { self.OnRecord(null, null, stats, onLineRead); } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
					}
				}
				catch (e) {
					try {
						var j = JSON.parse(line.replace(/NaN/g, null));
						stats.processed++;
						try { self.OnRecord(null, j, stats, onLineRead); } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
					}
					catch (e) {
						stats.errors++;
						self.OnRecordError({message: "Fields do not match expected headers", headers: fieldNames, values: values, line: line}, null, stats, onLineError);
					}
				}
			}
			else {
				stats.processed++;
				try { self.OnRecord(null, line, stats, onLineRead); } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
			}
		}
		else if ((parser === 'simplecsv') || (parser === 'csv')) {
			var values = null;
			if (parser === 'simplecsv') {
				values = line.split(fieldSeparator);
			}
			else {
				var valueRows = CSV.parse(line, fieldSeparator);
				values = valueRows && valueRows.length ? valueRows[0] : null;
			}
			//var values = (parser === 'simplecsv') ? line.split(fieldSeparator) : CSV.parse(line, fieldSeparator)[0];
			if (values && !((values.length === 1) && (values[0] === ""))) {
				if (!useSimpleCsv && (values.length !== fieldNames.length)) {
					stats.errors++;
					self.OnRecordError({message: "Fields do not match expected headers", headers: fieldNames, values: values, line: line}, null, stats, onLineError);
				}
				else {
					var record = asObject ? _.object(fieldNames, values) : values;
					stats.processed++;
					try { self.OnRecord(null, record, stats, onLineRead); } catch(e) { console.log("Error in OnLineRead() callback. %s", e); }
				}
			}
		}
	}

	stats.lines += (numberLines - x2 - 1);
	stats.skipped += (numberLines - x2 - 1);

	// All done - shut it down
	self.OnStreamFinished(null, filename, stats, onFileCompleted);
};
StreamingFileProcessor.prototype.OnRecord = function (err, record, stats, cb) {
	if (record) {
		//stats.totalTime = (microtime.nowDouble() - stats.startTime);
		//self.SendEvent(record);
		if (cb) { cb(err, record, stats); }
	}
};
StreamingFileProcessor.prototype.OnRecordError = function (err, record, stats, cb) {
	console.log("StreamingFileProcessor.prototype.OnRecordError: %d", stats.processed);
	if (err) { console.log(_.isObject(err) ? err.line : err); }
	if (cb) { cb(err, stats); }
};
StreamingFileProcessor.prototype.OnStreamStarted = function (err, filename, stats, cb) {
	//console.log("StreamingFileProcessor.prototype.OnStreamStarted for %s", filename);
	if (cb) { cb(err, filename, stats); }
};
StreamingFileProcessor.prototype.OnStreamFinished = function (err, filename, stats, cb) {
	stats.totalTime = (microtime.nowDouble() - stats.startTime);
	//console.log("Read %d out of %d total lines", stats.read, stats.lines);
	if (cb) { cb(err, filename, stats); }
};
StreamingFileProcessor.prototype.OnStreamError = function (err, filename, stats, cb) {
	console.log("StreamingFileProcessor.prototype.OnStreamError with file %s: %s", filename, err||"<Unknown Error>");
	if (cb) { cb(err, filename, stats); }
};
