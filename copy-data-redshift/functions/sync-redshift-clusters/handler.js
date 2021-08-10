"use strict";

const {Promise} = require("bluebird");

const {syncRedShiftNodes} = require("./sync-redshift-nodes");
const {processTable, clusterIdentifierDWH, databaseDWH, dbUserDWH} = require("../../lib/constants");
const {awsRedshift} = require("./../../lib/aws")

module.exports.handler = async (event) => {

    console.log("Event :", event);

    /*
        read process table.
    */
    return Promise.try(async ()=>{
        let newSqlQuery = `select * from ${processTable} where active=true`;
        const params = {
            ClusterIdentifier: clusterIdentifierDWH,
            Sql: newSqlQuery,
            Database: databaseDWH,
            DbUser: dbUserDWH
        };
            
        let processData = await new Promise((resolve, reject)=>{
            return awsRedshift.executeStatement(params, function(err, res){
                if (err) reject(err); // an error occurred
                else{
                    return awsRedshift.getStatementResult({Id:res.Id}, function(error, data){
                        if (error) reject(error); // an error occurred
                        else    resolve(data);
                    });
                }
            });
        });

        console.log('processData', processData);

        let processDataMap = {};
        const processDataArr = [];
        const keys = [];

        processData.ColumnMetadata.map(data=> keys.push(data.name));
        processData.Records.map(data=>{
            data.map((rec,index) => processDataMap[keys[index]] = Object.values(rec)[0]);
            processDataArr.push(processDataMap);
            processDataMap = {};
        });

        return Promise.map(processDataArr, row=>{
            console.log('check123', row);
            const redShiftClass = new syncRedShiftNodes(row.tableName, row.lastSync);
            return redShiftClass.execute();
        }, {concurrency:3});
    }).catch(err=> console.log(err))
};

console.log(this.handler());