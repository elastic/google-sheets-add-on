/**
 * @OnlyCurrentDoc  Limits the script to only accessing the current spreadsheet.
 */

var SIDEBAR_TITLE = 'Spreadsheet To Elasticsearch';

/**
 * Adds a custom menu with items to show the sidebar and dialog.
 *
 * @param {Object} e The event parameter for a simple onOpen trigger.
 */
function onOpen(e) {
  SpreadsheetApp.getUi()
    .createAddonMenu()
    .addItem('Configure', 'showSidebar')
    .addToUi();
}

/**
 * Runs when the add-on is installed; calls onOpen() to ensure menu creation and
 * any other initializion work is done immediately.
 *
 * @param {Object} e The event parameter for a simple onInstall trigger.
 */
function onInstall(e) {
  onOpen(e);
}

/**
 * Opens a sidebar. The sidebar structure is described in the Sidebar.html
 * project file.
 */
function showSidebar() {
  var ui = HtmlService.createTemplateFromFile('Sidebar')
      .evaluate()
      .setTitle(SIDEBAR_TITLE);
  SpreadsheetApp.getUi().showSidebar(ui);
}

/**
 * Checks to see if the cluster is accessible by calling /_status
 * Throws an error if the cluster does not return a 200
 *
 * @param {Object} host The set of parameters needed to connect to a cluster.
 */
function checkClusterConnection(host) {
  isValidHost(host);
  var url = [(host.use_ssl) ? 'https://' : 'http://',
             host.host,':',host.port,'/'].join('');
  var options = getDefaultOptions(host.username,host.password);
  options['muteHttpExceptions'] = true;
  try {
    var resp = UrlFetchApp.fetch(url, options);
    if(resp.getResponseCode() != 200) {
      var jsonData = JSON.parse(resp.getContentText());
      if(jsonData.message == 'forbidden') {
        throw "The username and/or password is incorrect."
      }
      throw jsonData.message;
    }
  } catch(e) {
    throw 'There was a problem connecting to your cluster. Please check the host or port and try again.'
  }
}

/**
 * Returns a clean name to use as an index based on the sheet name
 *
 */
function getSheetName() {
  try {
    var name = SpreadsheetApp.getActiveSheet().getName();
    return name.replace(/[^0-9a-zA-Z]/g,'_').toLowerCase();
  } catch(e) {
    return "";
  }
}

/**
 * Highlights the cells in the A1 range
 * @param {String} a1_range A1 notation for the cells to highlight - required.
 */
function highlightData(a1_range) {
  var sheet = SpreadsheetApp.getActiveSheet();
  try {
    var range = sheet.getRange(a1_range);
    if(a1_range.length == 3) {
      sheet.setActiveSelection(sheet.getRange(range.getRow(),range.getColumn(),range.getHeight()));
    } else {
      sheet.setActiveSelection(range);
    }
  } catch(e) {
    throw "The range entered was invalid. Please verify the range entered.";
  }
}

/**
 * Gets the default locations for headers and data, namely the first row
 * and all other rows.
 */
function getDefaultRanges() {
  try {
    var sheet = SpreadsheetApp.getActiveSheet();
    var header_range = sheet.getRange(1, 1, 1, sheet.getLastColumn());
    var data_range = sheet.getRange(2, 1, sheet.getLastRow()-1, sheet.getLastColumn());
    return [header_range.getA1Notation(),data_range.getA1Notation()];
  } catch(e) {
    throw "There is no data in the sheet.";
  }
}

/**
 * Attempts to validate that the data in each column is the same format.
 * If something isn't the same, it adds a note to the sheet and throws an
 * exception.
 */
function validateData(new_value) {
  var sheet = SpreadsheetApp.getActiveSheet();
  var range = null;
  try {
    range = sheet.getRange(new_value);
  } catch(e) {
    throw 'There is no data in the sheet.';
  }
  clearNotes();
  var start_row = parseInt(range.getRow());
  var start_col = parseInt(range.getColumn());
  var formats = range.getNumberFormats();
  var header_formats = formats.shift();
  for(var r in formats) {
    for(var c in formats[r]) {
      if(formats[r][c] != header_formats[c]) {
        var note_row = start_row+1+parseInt(r);
        var note_col = start_col+parseInt(c);
        var cell = sheet.getRange(note_row,note_col)
        cell.setNote('Not the same format as first row. This may cause data to not be inserted into your cluster. ~SpreadsheetToES');
        throw "Not all data formats are the same. See the note in the sheet.";
      }
    }
  }
}

/**
 * Attempts to clear only the notes that we've made
 */
function clearNotes() {
  var sheet = SpreadsheetApp.getActiveSheet();
  var notes_range = sheet.getRange(1, 1, sheet.getMaxRows(), sheet.getMaxColumns()).getNotes();
  for(var r in notes_range) {
    for(var c in notes_range[r]) {
      if(notes_range[r][c] && notes_range[r][c].indexOf('~SpreadsheetToES') >= 0) {
        sheet.getRange(1+parseInt(r),1+parseInt(c)).clearNote()
      }
    }
  }
}

/**
 * Pushes data from the spreadsheet to the cluster.
 *
 * @param {Object} host The set of parameters needed to connect to a cluster - required.
 * @param {String} index The index name - required.
 * @param {String} index_type The index type - required.
 * @param {String} template The name of the index template to use.
 * @param {String} header_range The A1 notion of the header row.
 * @param {String} data_range The A1 notion of the data rows.
 */
function pushDataToCluster(host,index,index_type,template,header_range_a1,data_range_a1,doc_id_range_a1) {
  isValidHost(host);

  if(!index) { throw "Index name cannot be empty." }
  if(index.indexOf(' ')>=0) { throw "Index should not have spaces." }

  if(!index_type) { throw "Index type cannot be empty." }
  if(index_type.indexOf(' ')>=0) { throw "Index type should not have spaces." }

  if(template && template.indexOf(' ')>=0) { throw "Template name should not have spaces." }

  if(!header_range_a1) { throw "Header range cannot be empty. " }
  if(!data_range_a1) { throw "Data range cannot be empty." }


  var data_range = null;
  try {
    data_range = SpreadsheetApp.getActiveSheet().getRange(data_range_a1);
  } catch(e) {
    throw "The data range entered was invalid. Please verify the range entered.";
  }
  var data = data_range.getValues();
  if(data.length <= 0) {
    throw "No data to push."
  }
  var doc_id_data = null;
  if(doc_id_range_a1) {
    var doc_id_range = null;
    try {
      doc_id_range = SpreadsheetApp.getActiveSheet().getRange([doc_id_range_a1,':',doc_id_range_a1].join(''));
    } catch(e) {
      throw "The doc id columne entered was invalid. Please verify the id column entered."
    }
    doc_id_range = doc_id_range.offset(data_range.getRow()-1, 0,data_range.getHeight());
    doc_id_data = doc_id_range.getValues();
    if(doc_id_data.length != data.length) {

    }
  }
  var headers = null;
  try {
    headers = SpreadsheetApp.getActiveSheet().getRange(header_range_a1).getValues()[0];
  } catch(e) {
    throw "The header range entered was invalid. Please verify the range entered.";
  }
  if(headers.length != data[0].length) {
    throw "Header and data ranges must have the same number of columns.";
  }
  for(var i in headers) {
    if(!headers[i]) {
      throw 'Header cell cannot be empty.';
    }
    headers[i] = headers[i].replace(/[^0-9a-zA-Z]/g,'_'); // clean up the column names for index keys
    headers[i] = headers[i].toLowerCase();
    if(!headers[i]) {
      throw 'Header cell cannot be empty.';
    }
  }
  var bulkList = [];
  if(template) { createTemplate(host,index,template); }
  var did_send_some_data = false;
  for(var r=0;r<data.length;r++) {
    var row = data[r];
    var toInsert = {};
    for(var c=0;c<row.length;c++) {
      if(row[c]) {
        toInsert[headers[c]] = row[c];
      }
    }
    if(Object.keys(toInsert).length > 0) {
      if(doc_id_data) {
        if(!doc_id_data[r][0]) {
          throw "Missing document id for data row: "+r;
        }
        bulkList.push(JSON.stringify({ "index" : { "_index" : index, "_type" : index_type, "_id" : doc_id_data[r][0] } }));
      } else {
        bulkList.push(JSON.stringify({ "index" : { "_index" : index, "_type" : index_type } }));
      }
      bulkList.push(JSON.stringify(toInsert));
      did_send_some_data = true;
      // Don't hit the UrlFetchApp limits of 10MB for POST calls.
      if(bulkList.length >= 2000) {
        postDataToES(host,bulkList.join("\n")+"\n");
        bulkList = [];
      }
    }
  }
  if(bulkList.length > 0) {
    postDataToES(host,bulkList.join("\n")+"\n");
    did_send_some_data = true;
  }
  if(!did_send_some_data) {
    throw "No data was sent to the cluster. Make sure your ranges are valid.";
  }
  return [(host.use_ssl) ? 'https://' : 'http://', host.host,':',host.port,'/',index,'/',index_type,'/_search'].join('');
}

/**
 * Creates a index template if required. If template already exists, it
 * does not update. If not, it uses default_template and the template name
 * to create a new one.
 *
 * @param {Object} host The set of parameters needed to connect to a cluster - required.
 * @param {String} index The index name - required.
 * @param {String} template_name The name of the index template to use - required.
 */
function createTemplate(host,index,template_name) {
  var url = [(host.use_ssl) ? 'https://' : 'http://',
             host.host,':',host.port,
            '/_template/',template_name].join('')
  var options = getDefaultOptions(host.username,host.password);
  options['muteHttpExceptions'] = true;
  var resp = null
  try {
    var resp = UrlFetchApp.fetch(url, options);
  } catch(e) {
    throw "There was an issue creating the template. Please check the names of the template or index and try again."
  }
  if(resp.getResponseCode() == 404) {
    options = getDefaultOptions(host.username,host.password);
    options.method = 'POST';
    default_template.template = index;
    options['payload'] = JSON.stringify(default_template);
    options.headers["Content-Type"] = "application/json";
    options['muteHttpExceptions'] = true;
    resp = null;
    try {
      resp = UrlFetchApp.fetch(url, options);
    } catch(e) {
      throw "There was an issue creating the template. Please check the names of the template or index and try again."
    }
    if(resp.getResponseCode() != 200) {
      var jsonData = JSON.parse(resp.getContentText());
      throw jsonData.message;
    }
  }
}

/**
 * Posts data to the ES cluster using the /_bulk endpoint
 *
 * @param {Object} host The set of parameters needed to connect to a cluster - required.
 * @param {Array} data The data to push in an array of JSON strings - required.
 */
function postDataToES(host,data) {
  var url = [(host.use_ssl) ? 'https://' : 'http://',
             host.host,':',host.port,'/_bulk'].join('');
  var options = getDefaultOptions(host.username,host.password);
  options.method = 'POST';
  options['payload'] = data;
  options.headers["Content-Type"] = "application/json";
  options['muteHttpExceptions'] = true;
  var resp = null;
  try {
    resp = UrlFetchApp.fetch(url, options);
  } catch(e) {
    throw "There was an error sending data to the cluster. Please check your connection details and data."
  }
  if(resp.getResponseCode() != 200) {
    var jsonData = JSON.parse(resp.getContentText());
    if(jsonData.error) {
      if(jsonData.error.indexOf('AuthenticationException')>=0) {
        throw "The username and/or password is incorrect."
      }
      throw jsonData.error;
    }
    throw "Your cluster returned an error. Please check your connection details and data."
  }
}

/**
 * Helper function to get the default UrlFetchApp parameters
 *
 * @param {String} username The username for basic auth.
 * @param {String} password The password for basic auth.
 */
function getDefaultOptions(username,password) {
  var options = {
    method : 'GET',
    headers : { },
  }
  if(username) {
    options.headers["Authorization"] = "Basic " + Utilities.base64Encode(username + ":" + password);
  }
  return options;
}

/**
 * Helper function to validate the host object
 *
 * @param {Object} host The set of parameters needed to connect to a cluster - required.
 */
function isValidHost(host) {
  if(!host) {
    throw 'Host cannot be empty.';
  }
  if(!host.host || !host.port) {
    throw 'Please enter your cluster host and port.';
  }
  if(host.host == 'localhost' || host.host == '0.0.0.0') {
    throw 'Your cluster must be externally accessible to use this tool.';
  }
}

/**
 * This is the default template to use. The template ke will
 * be relaced with the index name if required.
 *
 */
var default_template = {
  "order": 0,
  "template": "", // will be replaced with index name
  "settings": {
    "index.refresh_interval": "5s",
    "index.analysis.analyzer.default.type": "standard",
    "index.number_of_replicas": "1",
    "index.number_of_shards": "1",
    "index.analysis.analyzer.default.stopwords": "_none_"
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "string_fields": {
            "mapping": {
              "fields": {
                "{name}": {
                  "index": "analyzed",
                  "omit_norms": true,
                  "type": "string"
                },
                "raw": {
                  "search_analyzer": "keyword",
                  "ignore_above": 256,
                  "index": "not_analyzed",
                  "type": "string"
                }
              },
              "type": "multi_field"
            },
            "match_mapping_type": "string",
            "match": "*"
          }
        }
      ],
      "_all": { "enabled": true }
    }
  },
  "aliases": {}
};
