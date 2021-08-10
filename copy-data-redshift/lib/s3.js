const {awsS3} = require("./aws");

module.exports.saveArraytoS3 = (array, key, s3bucket) =>{
    // Use first element to choose the keys and the order
    let keys = Object.keys(array[0]);
  
    // Build header
    // let result = keys.join(",") + "\n";
    let result = "";
  
    // Add the rows
    array.forEach(function(obj){
      result += keys.map(k => obj[k]).join(",") + "\n";
    });

    return awsS3.putObject({
      Bucket: s3bucket,
      Key: key,
      Body:result
    }).promise().then( res=> console.log("file upload result::", res, s3bucket,key)).catch(err=> console.log(err));
  }