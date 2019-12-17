/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * PascalCoin Daemon
 **/

var net = require("net");
var request = require("request");
var requestPromise = require("request-promise");

/**
 * Initialize log system
 **/
var logSystem = "daemon";
require("./exceptionWriter.js")(logSystem);

/**
 * This class opens a connection to the mining port
 * and listens to miner-notify and allows blocks to
 * be submitted back to the network.
 **/
var DaemonMiner = exports.Miner = function() {
    var host = config.daemon.host;
    var port = config.daemon.miningPort;
    var client = new net.Socket();
    var id = 0;
    var submittedBlocks = [];
    var promises = new Map()

    /**
     * Handle multi-thread messages
     **/
    process.on("message", function(message) {
	switch (message.type) {
	case "submitBlock":
            submitBlock(message.block);
            break;
	}
    });

    /**
     * Submit block to network
     **/
    function submitBlock(block) {
        if(!config.poolServer.submitDuplicateBlockHeight && submittedBlocks.includes(block.height)) {
	    log("warn", logSystem, "Block already found at height %s, ignoring...", [block.height]);
        } else {
	    client.write(JSON.stringify({"id":block.height, "method":"miner-submit","params":[block]})+"\n");
	    log("info", logSystem, "Submitted Block at height %s successfully to daemon", [block.height]);
            // remember this block height
            submittedBlocks.push(block.height);
            // trim off from submitted block list
            while(submittedBlocks.length > 10) {
                submittedBlocks.shift();
            }
        }
    }

    this.checkhash = function(algo, digest) {
        return new Promise((resolve, reject) => {
            var _id = ++id
            promises.set(id, [resolve, reject])
            client.write(JSON.stringify({"id":_id, "method":algo,"params":[digest]})+"\n");
        })
    }

    /**
     * Receive miner-notify or submitBlock result
     **/
    client.on("data", (data) => {

	/**
	 * The miner port sends some garbage sometimes
	 * so we clean it up here.
	 **/

	// trim null and newlines from end
	while(data[data.length-1] == 0x0 ||
	      data[data.length-1] == 0x0A ||
	      data[data.length-1] == 0x0D) {
	    data = data.slice(0, data.length-1);
	}

	// add a single new line to end to make splitting easier
	data = Buffer.concat([data, Buffer.from([0x0A])]);

	// sometimes daemon sends two messages at once. might be separated
	// by either 0x0A (newline), 0x0D (carriage return) or 0x0 (null)
	// so, let's just replace any 0x0D & 0x0 with 0x0A so we can split
	for(var i = 0; i < data.length; i++) {
	    if(data[i] == 0x0D || data[i] == 0x0) {
		data[i] = 0x0A;
	    }
	}

	// split buffer into newlines,
	var lines = [],
	    index = 0;
	while(data.indexOf(0x0A, index) !== -1) {
            var tmp = data.slice(index, index = data.indexOf(0x0A, index) + 1);
	    // trim null and newlines from end
	    while(tmp[tmp.length-1] == 0x0 ||
	          tmp[tmp.length-1] == 0x0A ||
	          tmp[tmp.length-1] == 0x0D) {
	        tmp = tmp.slice(0, tmp.length-1);
	    }
            // make sure we don't have an empty line
            if(tmp.length) {
	        lines.push(tmp);
            }
	}

	for(var i = 0; i < lines.length; i++) {
	    try {
		var json = JSON.parse(lines[i]);
		if(json.hasOwnProperty("method") && json.method == "miner-notify") {
		    // TODO maybe do check here to make sure fields are correct length
		    // I have not seen the error, but rhminer checks this

		    // Send to all pool threads
		    process.send({
			type: "minerNotify",
			block: json.params[0]
		    });
		    log("info", logSystem, "Miner-notify received at height %s", [json.params[0].block]);
		} else if(json.hasOwnProperty("result") && json.result instanceof Object && json.result.hasOwnProperty('digest')) {
                    if(promises.has(json.id)) {
                        const [resolve, reject] = promises.get(json.id)
                        resolve(json.result.hash)
                        promises.delete(json.id)
                    }
		} else if(json.hasOwnProperty("result") && json.result instanceof Object) {
		    process.send({
			type: "blockAccepted",
			block: json.result.block
		    });
		    log("info", logSystem, "Block accepted at height %s with payload %s",
			[json.result.block, json.result.payload]);
		} else if(json.hasOwnProperty("error") && json.error !== null) {
		    log("warn", logSystem, json.error);
		} else {
		    log("error", logSystem, "Unknown error from %s", [data.toString("hex")]);
		}
	    } catch(error) {
		log("error", logSystem, "Parse error from %s", [data.toString("hex")]);
	    }
	}
    });

    client.on("close", () => {
	log("error", logSystem, "Miner connection closed!");
	log("warn", logSystem, "Waiting 10 seconds to reconnect...");
        setTimeout(() => {
            client.connect(port, host)
        }, 10000);
    });

    client.on("error", (err) => {
	log("error", logSystem, "Miner connection error %s", [err]);
    });

    client.on("connect", () => {
	log("info", logSystem, "Miner interface connected!");
    });

    log("info", logSystem, "Miner interface starting...");
    client.connect(port, host)


}


/**
 * This class implements needed RPC commands for the pool
 **/
var DaemonRpc = exports.Rpc = function() {

    var protocol = "http://";
    var host = config.daemon.host;
    var port = config.daemon.port;

    var id = 0;

    this.sync = function(method, params={}, callback, timeout=0) {
        var options = {
            url: `${protocol}${host}:${port}/json_rpc`,
            method: "POST",
            forever: true,
            json: true,
            body: {
                jsonrpc: "2.0",
                id: ++id,
                method: method
            },
        };
        if (Array.isArray(params) || Object.keys(params).length !== 0) {
            options.body.params = params
        }
        if(timeout) {
            options.timeout = timeout
        }

        return request(options, (error, httpResponse, body) => {
            if(error) {
                callback({
                    method: method,
                    params: params,
                    error: {
                        code: -1,
                        message: "Cannot connect to daemon",
                        cause: error.code
                    }
                });
                return;
            }

            if(body.hasOwnProperty("error")) {
                callback({
                    method: method,
                    params: params,
                    error: body.error
                });
                return;
            }
            callback({
                method: method,
                params: params,
                result: body.result
            });
            return;
        });
    }

    this.async = function(method, params={}, timeout=0) {
        var options = {
            forever: true,
            json: {
                jsonrpc: "2.0",
                id: ++id,
                method: method
            }
        };
        if (Array.isArray(params) || Object.keys(params).length !== 0) {
            options.json.params = params
        }
        if(timeout) {
            options.timeout = timeout
        }

        return requestPromise.post(`${protocol}${host}:${port}/json_rpc`, options)
            .then((response) => {
                if(response.hasOwnProperty("error")) {
                    return {
                        method: method,
                        params: params,
                        error: response.error
                    }
                }
                return {
                    method: method,
                    params: params,
                    result: response.result
                }
            }).catch(error => {
                return {
                    method: method,
                    params: params,
                    error: {
                        code: -1,
                        message: "Cannot connect to daemon",
                        cause: error.cause
                    }
                }
            });
    }

}
