/**
 * @OnlyCurrentDoc  Limits the script to only accessing the current spreadsheet.
 */

var SIDEBAR_TITLE = 'Sheet To Elasticsearch';

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
 * Returns the value in the active cell.
 *
 * @return {String} The value of the active cell.
 */
function checkClusterConnection(host) {
  if(!host.host || !host.port) {
    throw 'Please enter your cluster host and port.';
  }
  if(host.host == 'localhost' || host.host == '0.0.0.0') {
    throw 'Your cluster must be publicly accessible to use this tool.';
  }
  var url = [(host.use_ssl) ? 'https://' : 'http://',
             host.host,':',host.port,'/_status'].join('');
  var options = getDefaultOptions(host.username,host.password);
  options['muteHttpExceptions'] = true;
  var resp = UrlFetchApp.fetch(url, options);
  if(resp.getResponseCode() != 200) {
    throw 'Server returned: '+resp.getContentText();
  }
}

/**
 * Returns a clean name to use as an index
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

function pushDataToCluster(host,index,index_type,template) {
  var data = SpreadsheetApp.getActiveSheet().getDataRange().getValues();
  if(data.length < 2) {
    throw "No data to push."
  }
  var headers = data.shift();
  for(var i in headers) {
    if(!headers[i]) {
      throw 'Header cell cannot be empty.';
    }
    headers[i] = headers[i].replace(/[^0-9a-zA-Z]/g,'_'); // clean up the column names for index keys
    headers[i] = headers[i].toLowerCase();
  }
  var bulkList = [];
  if(template) { createTemplate(host,index,template); }
  for(var i in data) {
    var row = data[i];
    var toInsert = {};
    for(var c in row) {
      toInsert[headers[c]] = row[c];
    }
    bulkList.push(JSON.stringify({ "index" : { "_index" : index, "_type" : index_type } }));
    bulkList.push(JSON.stringify(toInsert));
    // Don't hit the UrlFetchApp limits of 10MB for POST calls.
    if(bulkList.length >= 2000) {
      postDataToES(host,bulkList.join("\n")+"\n");
      bulkList = [];
    }
  }
  if(bulkList.length > 0) {
    postDataToES(host,bulkList.join("\n")+"\n");
  }
}

function createTemplate(host,index,template_name) {
  var url = [(host.use_ssl) ? 'https://' : 'http://',
             host.host,':',host.port,
            '/_template/',template_name].join('')
  var options = getDefaultOptions(host.username,host.password);
  options['muteHttpExceptions'] = true;
  var resp = UrlFetchApp.fetch(url, options);
  if(resp.getResponseCode() == 404) {
    options = getDefaultOptions(host.username,host.password);
    options.method = 'POST';
    DEFAULT_TEMPLATE.template = index;
    options['payload'] = JSON.stringify(DEFAULT_TEMPLATE);
    options.headers["Content-Type"] = "application/json";
    resp = UrlFetchApp.fetch(url, options);
  } else {
    Logger.log('Template already exists');
  }
}

function postDataToES(host,data) {
  var url = [(host.use_ssl) ? 'https://' : 'http://',
             host.host,':',host.port,'/_bulk'].join('');
  var options = getDefaultOptions(host.username,host.password);
  options.method = 'POST';
  options['payload'] = data;
  options.headers["Content-Type"] = "application/json";
  var resp = UrlFetchApp.fetch(url, options);
  if(resp.getResponseCode() != 200) {
    throw 'Error sending data to cluster: '+resp.getContentText();
  }
}

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

var DEFAULT_TEMPLATE = {
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
      "_all": {
        "enabled": true
      }
    }
  },
  "aliases": {
  }
}
