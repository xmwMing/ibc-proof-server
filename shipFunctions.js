const {getActionProof3} = require("./ibcFunctions")
const {promisify} = require('util');

const axios = require('axios');

const getShipHeavyProof = req => async () => {
    //收到的区块
    let blocksReceived = [];
    const redis = req.redis;
    const existsAsync = promisify(redis.exists).bind(redis);

    let threshold = 2;
    let stopFlag = false;

    let reversibleBlocks = [];
    let uniqueProducers1 = [];
    let uniqueProducers2 = [];
    let block_to_prove;
    let previous_block;
    let count = 0;

    try {
        while (uniqueProducers2.length < threshold) {

            const key = req.firehoseOptions.start_block_num + count + "_sync";

            /**
             * 如果redis不存在请求快信息,等待redis同步,200ms以后继续请求
             */
            while (await existsAsync(key) === 0) {
                await sleep(200);
            }


            const json_obj = JSON.parse(await redis.get(key));
            count++;

            let add = true;

            const block = {
                block_num: json_obj.block_num,
                number: json_obj.block_num,
                id: json_obj.id,
                header: {
                    timestamp: json_obj.header.timestamp,
                    producer: json_obj.header.producer,
                    confirmed: json_obj.header.confirmed,
                    previous: json_obj.header.previous.toLowerCase(),
                    transaction_mroot: json_obj.header.transaction_mroot.toLowerCase(),
                    action_mroot: json_obj.header.action_mroot.toLowerCase(),
                    schedule_version: json_obj.header.schedule_version,
                    new_producers: json_obj.header.new_producers,
                    header_extensions: json_obj.header.header_extensions
                },
                // merkle : blockrootMerkle,
                traces: json_obj.traces,

                transactions: json_obj.transactions,
                producer_signatures: [json_obj.producer_signature]
            }

            //接收的区块存在请求区块
            if (blocksReceived.includes(json_obj.block_num)) {
                for (var i; i < 10; i++) console.log("UNDO");
                console.log("received the same block again : ", json_obj.block_num);

                var prev_count = uniqueProducers1.length;

                reversibleBlocks = reversibleBlocks.filter(data => data.number != block.number);
                uniqueProducers1 = uniqueProducers1.filter(data => data.number != block.number);
                uniqueProducers2 = uniqueProducers2.filter(data => data.number != block.number);

                //rollback finality candidate
                if (prev_count == threshold && uniqueProducers1.length < threshold) uniqueProducers2 = [];
                blocksReceived = blocksReceived.filter(r => r != block.number);
            }

            blocksReceived.push(block.number);

            //如果从开始证明后500个区块都没有到达阈值,结束证明
            if (block.number > 500 + req.firehoseOptions.start_block_num) {
                stopFlag = true;
            }
            //缓存证明区块上一个区块信息
            //if first block in request
            if (block.number == req.firehoseOptions.start_block_num) {
                // previous_block = preprocessBlock(json_obj, false);
                previous_block = block;
                continue;
            }

            //缓存当前证明区块信息
            //if second block in request
            else if (block.number == req.firehoseOptions.start_block_num + 1) {
                // block_to_prove = preprocessFirehoseBlock(json_obj, true);
                if (block.transactions.length <= 1) {
                    // console.log(JSON.stringify(block))
                    console.log(`${block.number}验证区块无交易,跳过当前区块!`)
                    return null;
                }
                block_to_prove = block;
                add = false;
            }

            //if uniqueProducers1 threshold reached
            if (uniqueProducers1.length == threshold) {

                let producer;

                if (uniqueProducers2.length > 0) producer = uniqueProducers2.find(prod => prod.name == block.header.producer);
                else if (uniqueProducers1[uniqueProducers1.length - 1].name == block.header.producer) producer = block.header.producer;

                if (!producer) uniqueProducers2.push({name: block.header.producer, number: block.number});

                //when enough blocks are collected
                if (uniqueProducers2.length == threshold) {

                    // stopFlag = true;
                    reversibleBlocks.push({number: block.number, block});
                    // last_bft_block = preprocessFirehoseBlock(JSON.parse(JSON.stringify(json_obj)));

                    return await on_proof_complete({reversibleBlocks, uniqueProducers1, uniqueProducers2});
                }

            }

            //if uniqueProducers1 threshold has not been reached
            else {
                if (uniqueProducers1.length > 0) {
                    const producer = uniqueProducers1.find(prod => prod.name == block.header.producer);
                    // console.log("producer",block.number,producer)
                    if (!producer && block.header.producer != block_to_prove.header.producer) uniqueProducers1.push({
                        name: block.header.producer,
                        number: block.number
                    });
                } else if (block.header.producer != block_to_prove.header.producer) uniqueProducers1.push({
                    name: block.header.producer,
                    number: block.number
                });

            }

            if (add) reversibleBlocks.push({number: block.number, block});
        }

        //handler for on_proof_complete event
        async function on_proof_complete(data) {
            // const endTime = +new Date();
            // console.log("ship用时 " + (endTime - beginTime) / 1000 + "s")
            // console.log("\non_proof_complete\n");

            const blockToProveNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${block_to_prove.block_num}`)).data[0].nodes
            const previousBlockNodes = (await axios.get(`${process.env.LIGHTPROOF_API}?blocks=${previous_block.block_num}`)).data[0].nodes


            const proof = {
                blockproof: {
                    chain_id: process.env.CHAIN_ID,
                    blocktoprove: {
                        block: {
                            header: block_to_prove.header,
                            producer_signatures: block_to_prove.producer_signatures,
                            previous_bmroot: blockToProveNodes[blockToProveNodes.length - 1],
                            id: "",
                            bmproofpath: []
                        },
                        active_nodes: previousBlockNodes,
                        node_count: previous_block.block_num - 1
                    },
                    bftproof: []
                }
            }

            // proof.actionproof = getActionProof3(block_to_prove);
            proof.actionproof = getActionProof3(block_to_prove);

            for (let row of data.reversibleBlocks) {
                let up1 = data.uniqueProducers1.find(item => item.number == row.number);
                let up2 = data.uniqueProducers2.find(item => item.number == row.number);
                if (up1 || up2) proof.blockproof.bftproof.push(formatBFTBlock(row.number, row.block));
            }

            let blocksTofetch = [];
            for (let bftproof of proof.blockproof.bftproof) {
                blocksTofetch.push(bftproof.block_num);
            }

            const uniqueList = [];
            for (let num of blocksTofetch) if (!uniqueList.includes(num)) uniqueList.push(num);
            let result = (await axios(`${process.env.LIGHTPROOF_API}?blocks=${uniqueList.join(',')}`)).data;

            for (let i = 0; i < blocksTofetch.length; i++) {
                const b = result.find(r => r.num === blocksTofetch[i]);
                if (!b) {
                    console.log("Error, block not found!", blocksTofetch[i]);
                    return;
                    // process.exit();
                }
                proof.blockproof.bftproof[i].previous_bmroot = b.nodes[b.nodes.length - 1];
            }

            proof.block = block_to_prove;
            return proof;

        }
    } catch (ex) {
        console.log("getHeavyProof ex", ex)
    }
}; //end of getHeavyProof

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const formatBFTBlock = (number, block) => ({
    id: block.id,
    block_num: number,
    header: block.header,
    producer_signatures: block.producer_signatures,
})

module.exports = {
    getShipHeavyProof
}
