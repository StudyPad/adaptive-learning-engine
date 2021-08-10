"use strict";

// import packages
const {Promise} = require("bluebird");

//import functions
const { awsRedshift }  = require("../../lib/aws");
const {clusterIdentifierLake, databaseLake, dbUserLake, 
    clusterIdentifierDWH, databaseDWH, dbUserDWH,
    s3Bucket, redshiftRole, processTable} = require("../../lib/constants");
const {saveArraytoS3} = require("../../lib/s3");


/*
    - we assume data in cluster 1 is the source of truth
*/

class syncRedShiftNodes {
    constructor(tableName, lastSyncHours){
        this.tableName = tableName;
        this.lastSyncHours = lastSyncHours;
    }

    lastTime(){
        let syncTimeInMillisSeconds = this.lastSyncHours*60*60*1000;
        this.lastDate = Date.now() = syncTimeInMillisSeconds;
        this.lastDate = new Date(this.lastDate);
    }

    readNewData(){
        let newSqlQuery = `select * from ${this.tableName} limit 3`;// where load_date>${this.lastDate}`;
        const params = {
            ClusterIdentifier: clusterIdentifierLake,
            Sql: newSqlQuery,
            Database: databaseLake,
            DbUser: dbUserLake
        };
          
        return new Promise((resolve, reject)=>{
            return awsRedshift.executeStatement(params, function(err, res){
                    if (err) reject(err); // an error occurred
                    else{
                        return awsRedshift.getStatementResult({Id:res.Id}, function(error, data){
                            if (error) reject(error); // an error occurred
                            else    resolve(data);
                        });
                        //rerun if data.nextToken is returned (all values have not been returned)
                    }
                });
        });
    }

    //TODO : check if table exists in cluster or not. Create a new table if doesn't
    createNewTable(){
        //let sqlQuery = `Create table ${this.tableName}()`
    }

    readUpdatedRows(){
        let updateSqlQuery = `select * from ${this.tableName} limit 2`; // where update_date>${this.lastDate}`;
        const params = {
            ClusterIdentifier: clusterIdentifierLake,
            Sql: updateSqlQuery,
            Database: databaseLake,
            DbUser: dbUserLake
        };
          
        return new Promise((resolve, reject)=>
            awsRedshift.executeStatement(params, function(err, res){
                if(err) reject(err);
                else{
                    return awsRedshift.getStatementResult({Id:res.Id}, function(error, data){
                        if(error)   reject(error);
                        else    resolve(data);
                    })
                }
            })
        )
    }

    async execute(){
        try{
            const result = await Promise.all([
                this.readNewData(),
                this.readUpdatedRows(),
              ]);
            const newData = result[0];
            const updatedData = result[1];
            const keys = [];
            let newDataMap = {};
            const newDataArr = [];
            let updatedDataMap = {};
            const updatedDataArr = [];

            newData.ColumnMetadata.map(data=> keys.push(data.name));
            newData.Records.map(data=>{
                data.map((rec,index) => newDataMap[keys[index]] = Object.values(rec)[0]);
                newDataArr.push(newDataMap);
                newDataMap = {};
            });
            updatedData.Records.map(data=>{
                data.map((rec,index) => updatedDataMap[keys[index]] = Object.values(rec)[0]);
                updatedDataArr.push(updatedDataMap);
                updatedDataMap = {};
            });
            //delete updated data from c2
            await Promise.resolve(this.deleteUpdatedData(updatedDataArr));
            const dataToUpload = updatedDataArr.concat(newDataArr);
            const fileName = `${this.tableName}-${Date.now()}.csv`;
            await this.writeToS3(dataToUpload, fileName, s3Bucket);
            await this.writeToCluster(dataToUpload, fileName, s3Bucket);
            //update process table
            this.updateProcessControlTable("success");
            return true;
        }
        catch (err){
            console.log(err);
            this.updateProcessControlTable("fail");
            return false;
            //update process table

        }
    }

    writeToS3(dataToUpload, fileName, s3Bucket){
        return saveArraytoS3(dataToUpload, fileName, s3Bucket);
    }

    writeToCluster(dataToUpload, fileName, s3Bucket){
        let keys = Object.keys(dataToUpload[0]).join(",");
        // Build header
        const copyQuery = `COPY ${this.tableName}
        (${keys}) FROM 's3://${s3Bucket}/${fileName}'
        credentials 'aws_iam_role=${redshiftRole}'
        CSV`;
        const params = {
            ClusterIdentifier: clusterIdentifierDWH,
            Sql: copyQuery,
            Database: databaseDWH,
            DbUser: dbUserDWH
        };
        return new Promise((resolve, reject) => 
            awsRedshift.executeStatement(params, function(err, res){
                if(err) reject(err);
                else resolve(res);
        }));
    }
    
    updateProcessControlTable(status){
        const updateQuery = `UPDATE ${processTable} SET status = ${status} Where tableName='${this.tableName}'`
        const params = {
            ClusterIdentifier: clusterIdentifierDWH,
            Sql: newSqlQuery,
            Database: databaseDWH,
            DbUser: dbUserDWH
        };
            
        await new Promise((resolve, reject)=>{
            return awsRedshift.executeStatement(params, function(err, res){
                if (err) reject(err); // an error occurred
                else    resolve(data);
            });
        });
        return true;
    }

    deleteUpdatedData(deleteData){
        return Promise.map(deleteData, entry=>{
            let deleteQuery = `delete from ${this.tableName} where id=${entry.id}`
            const params = {
                ClusterIdentifier: clusterIdentifierDWH,
                Sql: deleteQuery,
                Database: databaseDWH,
                DbUser: dbUserDWH
            };
            return new Promise((resolve, reject) =>
                awsRedshift.executeStatement(params, function(err, res){
                    if(err) reject(err);
                    else resolve(res);
            }))
        }, {concurrency : 5});
    }
};

module.exports = {
    syncRedShiftNodes : syncRedShiftNodes
}