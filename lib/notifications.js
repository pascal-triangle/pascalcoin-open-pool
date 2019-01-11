/**
 * PascalCoin Open Pool
 * https://github.com/PascalCoinPool/pascalcoin-open-pool
 *
 * Notifications system
 **/

var fs = require("fs");
var path = require("path");
var utils = require("./utils.js");

var nodemailer = require("nodemailer");
var mailgun = require("mailgun.js");

/**
 * Initialize log system
 **/
var logSystem = "notifications";
require("./exceptionWriter.js")(logSystem);

/**
 * Load notification templates, and use a watcher to look for changed files
 **/
var templateDir = "./email_templates";
var templates = {};

if(fs.existsSync(templateDir)) {
    fs.readdirSync(templateDir).forEach(function(file, index) {
        var template = file.substring(0, file.indexOf("."));
        var extension = file.substring(file.indexOf("."));
        if(/^[a-zA-Z0-9_]+$/.test(template)) {
            var contents = fs.readFileSync(path.join(templateDir, file), 'utf8');
            if(!templates.hasOwnProperty(template)) {
                templates[template] = {};
            }
            templates[template][extension=='.txt'?'subject':'body'] = contents;
        }
    });
}

fs.watch(templateDir, {}, (eventType, file) => {
    var template = file.substring(0, file.indexOf("."));
    var extension = file.substring(file.indexOf("."));
    if(/^[a-zA-Z0-9_]+$/.test(template)) {
        var contents = fs.readFileSync(path.join(templateDir, file), 'utf8');
        if(!templates.hasOwnProperty(template)) {
            templates[template] = {};
        }
        templates[template][extension=='.txt'?'subject':'body'] = contents;
    }
});

/**
 * Send miner notification
 **/
exports.sendToMiner = function(miner, id, variables) {
    if (config.email && config.email.enabled) {

        // get miner notify settings
        redisClient.hget(redisPrefix + ":workers:" + miner, "notify", function(error, notify) {
            if(notify == null) {
                notify = config.email.defaultNotifications;
            }
            notify = notify.split(",");

            if(!notify.includes(id)) {
                return;
            }

            // Set custom variables
            variables = setCustomVariables(variables);

            // Send email
            var subject = getEmailSubject(id, variables);
            var content = getEmailContent(id, variables);
            if (!content || content === "") {
                log("info", logSystem, "Notification disabled for %s: empty email content.", [id]);
                return;
            }

            redisClient.zrevrangebyscore(
                redisPrefix + ":worker_passwords:" + miner,
                "+inf",
                "(" + (Math.floor(Date.now() / 1000) - (config.poolServer.passwordExpireTime | 3600)),
                function(error, result) {
                    if(!Array.isArray(result)) {
                        return;
                    }
                    for(var i = 0; i < result.length; i++) {
                        var email = result[i];
                        // very simple check so we don't send emails to
                        // passwords that are obviously not email addresses
                        if(email.indexOf("@") !== -1) {
                            sendEmail(email, subject, content);
                        }
                    }
                }
            );
        });
    }
}


/**
 * Send email notification to a specific email address
 **/
exports.sendToEmail = function(email, id, variables) {
    // Set custom variables
    variables = setCustomVariables(variables);

    // Send notification
    if (config.email && config.email.enabled) {
        var subject = getEmailSubject(id, variables);
        var content = getEmailContent(id, variables);
        if (!content || content === "") {
            log("info", logSystem, "Notification disabled for %s: empty email content.", [id]);
            return;
        }

        sendEmail(email, subject, content);
    }
}

/**
 * Email helper functions
 **/

// Get email subject
function getEmailSubject(id, variables) {
    return replaceVariables(templates[id].subject, variables) || "";
}

// Get email content
function getEmailContent(id, variables) {
    var message = templates[id].body || "";
    if(!message || message === "") return "";

    var content = message;
    if(config.email.useHTML) {
        if(templates.template.body) {
            content = templates.template.body.replace(/%MESSAGE%/g, message);
        }
    } else {
        if(templates.template.subject) {
            content = templates.template.subject.replace(/%MESSAGE%/g, message);
        }
    }
    content = replaceVariables(content, variables);
    return content;
}

function sendEmail(email, subject, content) {
    // Return error if no destination email address
    if(!email) {
        log("warn", logSystem, "Unable to send e-mail: no destination email.");
        return ;
    }

    // Set content data
    var messageData = {
        from: config.email.fromAddress,
        to: email,
        subject: subject
    };

    if(config.email.useHTML) {
        messageData.html = content;
    } else {
        messageData.text = content;
    }


    // Get email transport
    var transportMode = config.email.transport;
    var transportCfg = config.email[transportMode] ? config.email[transportMode] : {};

    if(transportMode === "mailgun") {
        var mg = mailgun.client({username: "api", key: transportCfg.key});
        mg.messages.create(transportCfg.domain, messageData);
        log("info", logSystem, "E-mail sent to %s: %s", [messageData.to, messageData.subject]);
    } else {
        transportCfg["transport"] = transportMode;
        var transporter = nodemailer.createTransport(transportCfg);
        transporter.sendMail(messageData, function(error){
            if(error){
                log("error", logSystem, "Unable to send e-mail to %s: %s", [messageData.to, error.toString()]);
            } else {
                log("info", logSystem, "E-mail sent to %s: %s", [email, subject]);
            }
        });
    }
}

/**
 * Handle variables in texts
 **/

// Set custom variables
function setCustomVariables(variables) {
    if(!variables) variables = {};
    variables["TIME"] = utils.dateFormat(Date.now(), "yyyy-mm-dd HH:MM:ss Z");
    variables["COIN"] = config.coin || "";
    variables["SYMBOL"] = config.symbol || "";
    variables["POOL_NAME"] = config.poolName || "";
    variables["POOL_HOST"] = config.poolHost || "";
    return variables;
}

// Replace variables in a string
function replaceVariables(string, variables) {
    if(!string) return "";
    if(variables) {
        for(var varName in variables) {
            string = string.replace(new RegExp("%"+varName+"%", "g"), variables[varName]);
        }
    }
    return string;
}
