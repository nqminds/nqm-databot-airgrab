/**
 * Air grab and store:
 * @param {Object} tdx Api object.
 * @param {Object} output functions.
 * @param {Object} packageParams of the databot.
 */
function GrabAir(tdxApi, output, packageParams) {
    var req = function (cb) {
        var ttl;

        output.debug("Processing element Host:%s", packageParams.host);

        request
            .get(packageParams.host + packageParams.path + packageParams.groupName + packageParams.prefixPath)
            .accept('json')
            .end((error, response) => {
                if (error) {
                    output.error("API request error: %s", error);
                    cb(error, null);
                } else {
                    var entryList = [];

                    ttl = Number(response.body.HourlyAirQualityIndex['@TimeToLive']);
                    output.debug("Data TTL:%d", ttl);

                    _.forEach(response.body.HourlyAirQualityIndex.LocalAuthority, (valSite) => {
                        if (valSite.Site !== undefined) {

                            if (!_.isArray(valSite.Site))
                                valSite.Site = [valSite.Site];

                            _.forEach(valSite.Site, (speciesObj) => {
                                var timestamp = new Date(speciesObj['@BulletinDate']).getTime();
                                var siteCode = speciesObj['@SiteCode'];
                                var species = {};

                                if (!_.isArray(speciesObj.Species))
                                    speciesObj.Species = [speciesObj.Species];

                                _.forEach(speciesObj.Species, (val) => {
                                    species[val['@SpeciesCode']] = Number(val['@AirQualityIndex']);
                                });

                                var entry = {
                                    'timestamp': timestamp,
                                    'SiteCode': siteCode,
                                    'Species': species
                                };

                                entryList.push(entry);
                            });
                        }
                    })

                    tdxApi.addDatasetDataAsync(packageParams.airDataTable, entryList)
                        .then((res) => {
                            // TDX API result.
                            output.error("Added %d entries to dataset", entryList.length);
                            output.debug(res);
                            output.debug("Saving %d entries to airDataTableLatest", entryList.length);
                            return tdxApi.updateDatasetDataAsync(packageParams.airDataTableLatest, entryList, true);
                        })
                        .catch((error) => {
                            output.error("Error adding data to dataset:%s", JSON.stringify(error));
                            return cb(error, ttl);
                        })
                        .then((res) => {
                            return cb(null, ttl);
                        });
                }

            });
    };

    var timerFun = function() {
        req((error, ttl) => {
                var delay = packageParams.defaultDelay;

                if (ttl!=null)
                    delay = (ttl+1)*60*1000;
                
                output.debug("Set timer to: %d", delay);

                setTimeout(timerFun, delay);
        });
    };

    setTimeout(timerFun, 0);

}

/**
 * Main databot entry function:
 * @param {Object} input schema.
 * @param {Object} output functions.
 * @param {Object} context of the databot.
 */
function databot(input, output, context) {
    "use strict"
    output.progress(0);

    var tdxApi = new TDXAPI({
        commandHost: context.commandHost,
        queryHost: context.queryHost,
        accessTokenTTL: context.packageParams.accessTokenTTL
    });

    Promise.promisifyAll(tdxApi);

    tdxApi.authenticate(context.shareKeyId, context.shareKeySecret, function (err, accessToken) {
        if (err) {
            output.error("%s", JSON.stringify(err));
            process.exit(1);
        } else {
            GrabAir(tdxApi, output, context.packageParams);
        }
    });
}

var input;
var _ = require('lodash');
var request = require("superagent");
var Promise = require("bluebird");
var TDXAPI = require("nqm-api-tdx");

if (process.env.NODE_ENV == 'test') {
    // Requires nqm-databot-airgrab.json file for testing
    input = require('./databot-test.js')(process.argv[2]);
} else {
    // Load the nqm input module for receiving input from the process host.
    input = require("nqm-databot-utils").input;
}

// Read any data passed from the process host. Specify we're expecting JSON data.
input.pipe(databot);