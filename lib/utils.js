/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * Helper utils
 **/

var crypto = require("crypto");
var bignum = require("bignum");

var dateFormat = require('dateformat');
exports.dateFormat = dateFormat;

exports.validateLogin = function(login) {

    // ADDRESS.PAYMENT-ID.WORKER/EMAIL
    var parts = {
        "valid": false,
        "address": "",
        "known_account": "",
        "payment_id": "0",
        "worker_id": "",
        "address_pid": "",
        "password": ""
    }

    if(login == null) {
        return parts;
    }

    // Extract password
    login = login.split("/");
    if(login.length == 2) {
        parts.password = login[1];
    } else if(login.length == 1) {
        // no password provided
    } else {
        return {
            "valid": false,
            "message": "invalid login format"
        }
    }

    // Extract login parts
    login = login[0].split(".");
    if(login.length >= 3) {
        parts.address = login.shift();
        parts.payment_id = login.shift();
        if((parts.payment_id == login[0] || login[0] == "") && login.length >= 2) {
            // work around for nanominer sending two payment ids
            login.shift();
        }
        parts.worker_id = login.join(".");
    } else if(login.length == 2) {
        parts.address = login.shift();
        parts.payment_id = login.shift();
        parts.worker_id = "unnamed";
    } else if(login.length == 1) {
        parts.address = login.shift();
        parts.payment_id = "0";
        parts.worker_id = "unnamed";
    } else {
        return {
            "valid": false,
            "message": "invalid login format"
        }
    }

    // Nanominer adds rigid to end of the password
    if(parts.password.endsWith("."+parts.worker_id)) {
        parts.password = parts.password.substring(0, parts.password.length - parts.worker_id.length - 1);
    }

    // Validate PASA format
    var pasa_valid = false;
    if(/^[0-9\-]+$/.test(parts.address)) {
        switch((parts.address.match(/-/g) || []).length) {
        case 0:
            // no checksum, assume valid and add checksum manually
            parts.address += "-"+(((parts.address*101) % 89) + 10);
            pasa_valid = true;
            break;
        case 1:
            var address_parts = parts.address.split("-");
            if(((address_parts[0]*101) % 89) + 10 == address_parts[1]) {
                // checksum matches
                pasa_valid = true;
            }
            break;
        }
    }

    if(!pasa_valid) {
        return {
            "valid": false,
            "message": "pasa is not valid"
        }
    }

    // validate payment id
    if(parts.payment_id == "") {
        parts.payment_id = "0";
    }
    /*
    if(!/^[0-9A-Fa-f]{16}$/.test(parts.payment_id) && parts.payment_id != "0") {
        return {
            "valid": false,
            "message": "payment_id is not valid"
        }
    }
    */

    // check if we are mining to exchange without payment_id
    for(var i = 0; i < config.knownAccounts.length; i++) {
        if(parts.address == config.knownAccounts[i].account) {
            parts.known_account = config.knownAccounts[i].name;
            if(parts.payment_id == "0") {
                return {
                    "valid": false,
                    "message": "payment_id required when mining to "+config.knownAccounts[i].name
                }
            }
        }
    }

    parts.valid = true;
    parts.address_pid = parts.address+"."+parts.payment_id;
    return parts;

}


/**
 * Shuffles array in place.
 * @param {Array} a items An array containing the items.
 */
exports.shuffleArray = function(a) {
    var j, x, i;
    for (i = a.length - 1; i > 0; i--) {
        j = Math.floor(Math.random() * (i + 1));
        x = a[i];
        a[i] = a[j];
        a[j] = x;
    }
    return a;
}

exports.packUInt16LE = function(num) {
    var buff = new Buffer(2);
    buff.writeUInt16LE(num, 0);
    return buff;
};
exports.packInt32LE = function(num) {
    var buff = new Buffer(4);
    buff.writeInt32LE(num, 0);
    return buff;
};
exports.packInt32BE = function(num) {
    var buff = new Buffer(4);
    buff.writeInt32BE(num, 0);
    return buff;
};
exports.packUInt32LE = function(num) {
    var buff = new Buffer(4);
    buff.writeUInt32LE(num, 0);
    return buff;
};
exports.packUInt32BE = function(num) {
    var buff = new Buffer(4);
    buff.writeUInt32BE(num, 0);
    return buff;
};
exports.packInt64LE = function(num) {
    var buff = new Buffer(8);
    buff.writeUInt32LE(num % Math.pow(2, 32), 0);
    buff.writeUInt32LE(Math.floor(num / Math.pow(2, 32)), 4);
    return buff;
};

exports.getReadableHashRateString = function(hashrate) {
    var i = -1;
    var byteUnits = [" KH", " MH", " GH", " TH", " PH"];
    do {
        hashrate = hashrate / 1024;
        i++;
    } while (hashrate > 1024);
    return hashrate.toFixed(2) + byteUnits[i];
};

exports.targetToCompact = function(target) {
    var buf = Buffer.alloc(4);
    buf.writeUInt32BE(target, 0);
    return buf.toString("hex");
}

exports.targetFromCompact = function(compact) {
    var encoded = bignum(compact);

    var nbits = encoded.shiftRight(24);

    var i = 0x08000000 >> 24;
    if(nbits < i)
        nbits = i; // min nbits
    if(nbits > 231)
        nbits = 231; // max nbits

    var offset = encoded.and(0x00FFFFFF).xor(0x00FFFFFF).or(0x01000000);

    var bn = bignum(offset).shiftLeft(256 - nbits - 25);

    return bn;

    // remove above return if you want to get with correct leading zeros
    var raw = bn.toBuffer();

    var buf = Buffer.alloc(32);

    for(var i = 0; i < raw.length; i++) {
        buf[i+32-raw.length] = raw[i];
    }

    return bignum.fromBuffer(buf);
}
