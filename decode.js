import fs from 'fs';
import readline from 'readline';
import { BSON, ObjectId as objectId } from 'bson';
import { exec } from 'child_process';

function ksdecode(pattern, hexString) {
    // https://github.com/mongodb/mongo/blob/master/src/mongo/db/storage/key_string_decode.cpp
    const command = `ksdecode -o bson -p ${pattern} ${hexString}`;

    const ObjectId = (str) => objectId.createFromHexString(str);

    return new Promise((resolve, reject) => {
        exec(command, (error, stdout, stderr) => {
            if (error) {
                reject(`Error executing command: ${error.message}`);
                return;
            }

            if (stderr) {
                reject(`Error: ${stderr}`);
                return;
            }

            const jsonStr = stdout.replace(/\n$/, '').replace(/(\w+)\s*:/g, '"$1":');
            resolve(eval(`(${jsonStr})`));
        });
    });
}

function runWtCommand(command) {
    // https://source.wiredtiger.com/develop/command_line.html
    return new Promise((resolve, reject) => {
        exec(command, (error, stdout, stderr) => {
            if (error) {
                reject(`Error executing command: ${error.message}`);
                return;
            }

            if (stderr) {
                reject(`Error: ${stderr}`);
                return;
            }
            resolve(stdout);
        });
    });
}

function wtDump(wtDirectoy, wtType, wtFile, outputFile) {
    const outputCommand = outputFile ? `-f ${outputFile}` : '';
    return runWtCommand(`wt -h ${wtDirectoy} -r dump -x ${outputCommand} ${wtType}:${wtFile}`)
}

async function processLineByLine(inputType, inputName, data, parseMode) {

    const rl = inputType === 'file' ? readline.createInterface({
        input: fs.createReadStream(data),
        crlfDelay: Infinity
    }) : data.split('\n');
    
    let lineNumber = 0;
    let dataLineFound = false;

    const documents = [];
    const shouldCheckOddLines = inputName.startsWith('index-') ? false : true;

    for await (const line of rl) {
        ++lineNumber;

        if(!dataLineFound){
            if(line === 'Data') dataLineFound = true;
            continue;
        }

        if(line === '') continue;
        if((lineNumber % 2 === 0) !== shouldCheckOddLines) continue;

        if(parseMode === 'hex') {
            documents.push(line);
            continue;
        }

        const buffer = Buffer.from(line, 'hex');

        if(parseMode === 'bson') {
            const bsonObject = BSON.deserialize(buffer);
            documents.push(bsonObject);
        }
    }

    return documents;
}

(async () => {

    const WT_FOLDER = '~/MongoDB/data/7.0.0';

    const response = await wtDump(WT_FOLDER, 'file', '_mdb_catalog.wt', false);
    const metadata = await processLineByLine('string', '_mdb_catalog', response, 'bson');
    
    const internalDatabases = ['config', 'local', 'admin']
    const collections = metadata.map(data => {
        const namespace = data.md.ns.split('.');
        return {
            db: namespace.shift(),
            name: namespace.join('.'),
            indexes: data.md.indexes.map(index => {
                return {
                    ...index,
                    file: data.idxIdent[index.spec.name]
                }
            }),
            file: data.ident
        }
    }).filter(collection => !internalDatabases.includes(collection.db));

    for (const collection of collections) {
        for (const index of collection.indexes) {

            const pattern = JSON.stringify(index.spec.key);
            const response = await wtDump(WT_FOLDER, 'table', index.file, false);

            console.log(response);

            const promises = (await processLineByLine('string', index.file, response, 'hex'))
                .map(hex => ksdecode(pattern, hex));

            const indexData = await Promise.all(promises)
            
            console.log(indexData);
        }
    }
})();
