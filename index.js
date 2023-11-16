require('dotenv').config();
//create websocket server
const WebSocketServer = require('ws');
const Redis = require("ioredis");
const websocketPort = process.env.PORT;
const redisHost = process.env.REDIS_HOST;
const redisPort = process.env.REDIS_PORT;

//import websocket message handlers
const {handleHeavyProof} = require("./handlers");

//main process handling websocket connections
const main = async () => {
    //initiate websocket server
    console.log("redis host = " + redisHost)
    const wss = new WebSocketServer.Server({port: websocketPort});
    console.log(redisHost)
    const redis = new Redis(redisHost, redisPort);
    //on new connection handler
    wss.on("connection", (ws, req) => {
        ws.id = req.headers['sec-websocket-key'];
        console.log("new ws connected");

        ws.on("message", async (message) => {
            let msgObj;
            try {
                msgObj = JSON.parse(message.toString())
            } catch (ex) {
                return ws.send(JSON.stringify({type: "error", error: "Message needs to be a stringified object"}));
            }
            await handleHeavyProof(msgObj.blockNum, redis,ws);
        });

        //find and close existing firehose streams if socket client disconnects
        ws.on("close", () => {
            console.log("the ws has disconnected");
            // closeClientStreams(ws.id);
        });

        ws.onerror = function (e) {
            console.log("Error",e)
        }
    });
    console.log("Listening on", websocketPort)
}

var signals = {
    'SIGHUP': 1,
    'SIGINT': 2,
    'SIGTERM': 15
};
Object.keys(signals).forEach((signal) => {
    process.on(signal, () => {
        console.log(`process received a ${signal} signal`);
        let value = signals[signal];
        process.exit(128 + value);
    });
});

main().catch(error => {
    console.log("unhandled error main", error)
    process.exit(1);
});

process.on("unhandledRejection", function (reason, p) {
    let message = reason ? reason.stack : reason;
    console.error(`Possibly Unhandled Rejection at: ${message}`);
    process.exit(1);
});