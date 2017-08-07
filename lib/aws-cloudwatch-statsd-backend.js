var util = require('util');
var AWS = require('aws-sdk');

var LogLevels = {
  VERBOSE: 0,
  ERROR: 1
};

function CloudwatchBackend(startupTime, config, emitter) {
  var self = this;

  this.config = config || {};
  AWS.config = this.config;

  this.containerIndexMapping = {};
  this.nextContainerIndex = 0;

  function setEmitter() {
    self.cloudwatch = new AWS.CloudWatch(self.config);
    emitter.on('flush', function(timestamp, metrics) { self.flush(timestamp, metrics); });
  }

  // if iamRole is set attempt to fetch credentials from the Metadata Service
  if (this.config.iamRole) {
    if (this.config.iamRole == 'any') {
      // If the iamRole is set to any, then attempt to fetch any available credentials
      ms = new AWS.EC2MetadataCredentials();
      ms.refresh(function(err) {
        if (err) { self.log('Failed to fetch IAM role credentials: ' + err, LogLevels.ERROR); }
        self.config.credentials = ms;
        setEmitter();
      });
    } else {
      // however if it's set to specify a role, query it specifically.
      ms = new AWS.MetadataService();
      ms.request('/latest/meta-data/iam/security-credentials/' + this.config.iamRole, function(err, rdata) {
        var data = JSON.parse(rdata);

        if (err) { self.log('Failed to fetch IAM role credentials: ' + err, LogLevels.ERROR); }
        self.config.credentials = new AWS.Credentials(data.AccessKeyId, data.SecretAccessKey, data.Token);
        setEmitter();
      });
    }
  } else {
    setEmitter();
  }
};

// Levels: 0 info, 1 err
CloudwatchBackend.prototype.log = function(msg, level) {
  if (level > 0 || this.config.verbose) {
    console.log(msg);
  }
};

CloudwatchBackend.prototype.processKey = function(key) {
  var parts = key.split(/[\.\/-]/);

  return {
    metricName: parts[parts.length - 1],
    namespace: parts.length > 1 ? parts.splice(0, parts.length - 1).join("/") : null
  };
};

/**
 * Pops the first two segments as instance ID and container UUID.
 * Incoming key format is expected to be: [instance-index (int)].[container-uuid].[prefix].[serviceName].[metricName]
 * 
 * Remaps the container ID to an actual index, keeping the metrics cardinality lower in CloudWatch.
 * CloudWatch will show Combinations of indices, which can be reused during restarts or reshuffles of containers, e.g.
 * [instance-1.container-1] and [instance-1.container-2]. Deleting gauges via config ensures that some channels "die" as 
 * containers move or restart.
 * 
 * This solution is flawed but it is the best we have at the moment without putting a huge amount of work into it.
 * 
 * Returns the reformatted key: [prefix].[serviceName].[metricName].[instance-index].[container-index]
 */
CloudwatchBackend.prototype.graphcoolKeyProcessing = function(key) {
  if (key.indexOf('statsd.') == 0) {
    return key; // Ignore statsd metrics
  }

  var parts          = key.split(".");
  var instanceIndex  = parts[0];
  var containerUUID  = parts[1];
  var containerIndex = this.containerUUIDToLinearIndex(containerUUID);

  parts.push('instance-' + instanceIndex, 'container-' + containerIndex);

  return parts.splice(2, parts.length).join(".");
};

/*
 * This is of course not bulletproof. If the statsd container goes down the mapping is lost and
 * rebuild, potentially causing a disconnect in metrics, e.g. instance 1 container 2 might suddenly be connected
 * to the others metric stream, causing weirdness in the graphs. We'll see how it pans out and if the implementation
 * requires rethinking.
 */
CloudwatchBackend.prototype.containerUUIDToLinearIndex = function(containerUUID) {
  var index = this.containerIndexMapping[containerUUID];

  if (index === undefined) {
    index = this.nextContainerIndex;

    // Use next index
    this.containerIndexMapping[containerUUID] = index;

    // Increment nextContainerIndex
    this.nextContainerIndex += 1;
    
    // Save containerUUID -> index mapping
    this.containerIndexMapping[containerUUID] = index;
  }

  return index;
}

/**
 * Takes the first two segments and simply builds a namespace [serviceName]-[env].
 * E.g. "SimpleService-prod" or "SimpleService-dev".
 */
CloudwatchBackend.prototype.graphcoolNamespaceProcessing = function(key) {
  if (key.indexOf('statsd.') == 0)
    return "AwsCloudWatchStatsdBackend"; // Ignore statsd metrics

  var parts   = key.split(".");
  var env     = parts[0];
  var service = parts[1];

  return service + '-' + env;
};

CloudwatchBackend.prototype.isBlacklisted = function(key) {

  var blacklisted = false;

  // First check if key is whitelisted
  if (this.config.whitelist && this.config.whitelist.length > 0 && this.config.whitelist.indexOf(key) >= 0) {
    this.log("Key (counter) " + key + " is whitelisted", LogLevels.VERBOSE);
    return false;
  }

  if (this.config.blacklist && this.config.blacklist.length > 0) {
    for (var i = 0; i < this.config.blacklist.length; i++) {
      if (key.indexOf(this.config.blacklist[i]) >= 0) {
        blacklisted = true;
        break;
      }
    }
  }
  return blacklisted;
};

CloudwatchBackend.prototype.chunk = function(arr, chunkSize) {

  var groups = [], i;

  for (i = 0; i < arr.length; i += chunkSize) {
    groups.push(arr.slice(i, i + chunkSize));
  }
  return groups;
};

CloudwatchBackend.prototype.batchSend = function(currentMetricsBatch) {

  var self = this;

  Object.keys(currentMetricsBatch).forEach(function(namespace) {
    var metrics = currentMetricsBatch[namespace];

    if (metrics.length > 0) {
      var chunkedGroups = self.chunk(metrics, 20);

      for (var i = 0, len = chunkedGroups.length; i < len; i++) {
        self.cloudwatch.putMetricData({
          MetricData: chunkedGroups[i],
          Namespace: namespace
        }, function(err, data) {
          if (err) {
            self.log(err, LogLevels.ERROR);
          } else {
            self.log(JSON.stringify(data), LogLevels.VERBOSE);
          }
        });
      }
    }
  });
};

CloudwatchBackend.prototype.flush = function(timestamp, metrics) {

  this.log('Flushing metrics at ' + new Date(timestamp * 1000).toISOString(), LogLevels.VERBOSE);
  this.log(JSON.stringify(metrics), LogLevels.VERBOSE);

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;

  var currentCounterMetrics = {};
  var namespace = "AwsCloudWatchStatsdBackend";

  for (key in counters) {
    if (key.indexOf('statsd.') == 0 || this.isBlacklisted(key))
      continue;

    var names = this.config.processKeyForNamespace ? this.processKey(key) : {};
    namespace = this.config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";

    var metricName = this.config.metricName || names.metricName || key;
    metricName = this.config.graphcoolKeyProcessing ? this.graphcoolKeyProcessing(metricName) : metricName;
    namespace = this.config.graphcoolKeyProcessing ? this.graphcoolNamespaceProcessing(metricName) : namespace;

    var metric = {
      MetricName: metricName,
      Unit: 'Count',
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: counters[key]
    };

    if (currentCounterMetrics[namespace] === undefined)
      currentCounterMetrics[namespace] = [];

    currentCounterMetrics[namespace].push(metric);
  }

  this.batchSend(currentCounterMetrics);

  var currentTimerMetrics = [];
  for (key in timers) {
    if (timers[key].length > 0) {
      if (this.isBlacklisted(key))
        continue;

      var values = timers[key].sort(function(a, b) {
        return a - b;
      });

      var count = values.length;
      var min = values[0];
      var max = values[count - 1];

      var cumulativeValues = [min];
      for (var i = 1; i < count; i++) {
        cumulativeValues.push(values[i] + cumulativeValues[i - 1]);
      }

      var sum = min;
      var mean = min;
      var maxAtThreshold = max;

      var message = "";

      var key2;

      sum = cumulativeValues[count - 1];
      mean = sum / count;

      var names = this.config.processKeyForNamespace ? this.processKey(key) : {};
      namespace = this.config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
      var metricName = this.config.metricName || names.metricName || key;
      metricName = this.config.graphcoolKeyProcessing ? this.graphcoolKeyProcessing(metricName) : metricName;
      namespace = this.config.graphcoolKeyProcessing ? this.graphcoolNamespaceProcessing(metricName) : metricName;

      var metric = {
        MetricName: metricName,
        Unit: 'Milliseconds',
        Timestamp: new Date(timestamp * 1000).toISOString(),
        StatisticValues: {
          Minimum: min,
          Maximum: max,
          Sum: sum,
          SampleCount: count
        }
      };

      if (currentTimerMetrics[namespace] === undefined)
        currentTimerMetrics[namespace] = [];

      currentTimerMetrics[namespace].push(metric);
    }
  }

  this.batchSend(currentTimerMetrics);

  var currentGaugeMetrics = [];

  for (key in gauges) {
    if (this.isBlacklisted(key))
      continue;

    var names = this.config.processKeyForNamespace ? this.processKey(key) : {};
    namespace = this.config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = this.config.metricName || names.metricName || key;
    metricName = this.config.graphcoolKeyProcessing ? this.graphcoolKeyProcessing(metricName) : metricName;
    namespace = this.config.graphcoolKeyProcessing ? this.graphcoolNamespaceProcessing(metricName) : metricName;

    var metric = {
      MetricName: metricName,
      Unit: 'None',
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: gauges[key]
    };

    if (currentGaugeMetrics[namespace] === undefined)
      currentGaugeMetrics[namespace] = [];

    currentGaugeMetrics[namespace].push(metric);
  }

  this.batchSend(currentGaugeMetrics);

  var currentSetMetrics = [];
  
  for (key in sets) {
    if (this.isBlacklisted(key))
      continue;

    var names = this.config.processKeyForNamespace ? this.processKey(key) : {};
    namespace = this.config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = this.config.metricName || names.metricName || key;
    metricName = this.config.graphcoolKeyProcessing ? this.graphcoolKeyProcessing(metricName) : metricName;
    namespace = this.config.graphcoolKeyProcessing ? this.graphcoolNamespaceProcessing(metricName) : metricName;

    var metric = {
      MetricName: metricName,
      Unit: 'None',
      Timestamp: new Date(timestamp * 1000).toISOString(),
      Value: sets[key].values().length
    };

    if (currentSetMetrics[namespace] === undefined)
      currentSetMetrics[namespace] = [];

    currentSetMetrics[namespace].push(metric);
  }

  this.batchSend(currentSetMetrics);
};

function isValidConfig(config) {
  return config.hasOwnProperty('region') && config.hasOwnProperty('accessKeyId') && config.hasOwnProperty('secretAccessKey')
}

function configFromEnv(refConfig) {
  var config = {
    region: process.env[refConfig.regionEnvKey],
    accessKeyId: process.env[refConfig.accessKeyIdEnvKey],
    secretAccessKey: process.env[refConfig.secretAccessKeyEnvKey]
  };

  if (refConfig.hasOwnProperty('verbose')) config.verbose = refConfig.verbose;
  if (refConfig.hasOwnProperty('metricName')) config.metricName = refConfig.metricName;
  if (refConfig.hasOwnProperty('namespace')) config.namespace = refConfig.namespace;
  if (refConfig.hasOwnProperty('processKeyForNames')) config.processKeyForNames = refConfig.processKeyForNames;
  if (refConfig.hasOwnProperty('graphcoolKeyProcessing')) config.graphcoolKeyProcessing = refConfig.graphcoolKeyProcessing;

  return config;
}

exports.init = function(startupTime, config, events) {
  var cloudwatch = config.cloudwatch || {};
  var instances = cloudwatch.instances || [cloudwatch];

  console.log("Starting with configuration: " + JSON.stringify(cloudwatch))

  for (key in instances) {
    var instanceConfig = instances[key];

    if (instanceConfig.configureFromEnv) {
      console.log("Configuring aws backend using ENV vars.");
      instanceConfig = configFromEnv(instanceConfig);
    }

    if (!isValidConfig(instanceConfig)) {
      console.log("Invalid AWS backend config. Required keys: 'region', 'accessKeyId', 'secretAccessKey'");
      return false;
    }

    console.log("Starting cloudwatch reporter instance in region:", instanceConfig.region);
    new CloudwatchBackend(startupTime, instanceConfig, events);
  }

  return true;
};
