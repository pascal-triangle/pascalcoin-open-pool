/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * Market Exchanges
 **/

var requestPromise = require("request-promise");

// Initialize log system
var logSystem = 'market';
require('./exceptionWriter.js')(logSystem);

/**
 * Get market prices
 **/
var cache = {};
var cache_time = 0;
exports.get = function(callback) {
    if(Date.now() - cache_time < 60000) {
        // if requested less than a minute ago
        callback(cache);
        return;
    }
    requestPromise("https://min-api.cryptocompare.com/data/price?fsym=PASC&tsyms=BTC,USD,EUR,CAD,INR,GBP,COP,SGD,JPY&extraParams=pascalcoin_freepool")
        .then((response) => {
            try{
                response = JSON.parse(response);
	    } catch(error) {
                callback({
                    error: {
                        code: -2,
                        message: "Cannot decode JSON"
                    }
                });
                return;
            }
            if(response.hasOwnProperty("error")) {
                callback({
                    error: response.error
                });
                return;
            }
            cache = response;
            cache_time = Date.now();
            callback(response);
            return;
        }).catch(error => {
            callback({
                error: {
                    code: -1,
                    message: "Cannot fetch price"
                }
            });
            return;
        });
}
