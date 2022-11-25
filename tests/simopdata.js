require('dotenv').config();
require('../db')  //init mongoose
const Value =require('../models/value');
const SmartDate =require('../smartdate');

const op_data_config= require('../json/op_data_sync.json');

const argv = require('minimist')(process.argv.slice(2));

async function callsLoop(end, dayCount, config, val, user) {

    for (let i = 0; i < config.length; i++) {
      try {
        const el = config[i];
  
        for (let j = 0; j < dayCount; j++) {
          let from = new SmartDate(end).currGasDay().addDay(-1*j).dt; 
          //simulate  
          let value = {"object": +el.object, "parameter":+el.parameter, "value": val, "time_stamp": from, "user" : user};
          //let res = await Value.create(value);
          //console.log(value);    
          let res =  await upsertValue(value);  //
          //console.log(res);         
        }     
  
      } catch (error) {
        console.log(error); 
      }  
  
    }
  
  }
  
  
  async function upsertValue(new_value) {  
  
    let res = await Value.findOne( { "object": new_value.object, "parameter": new_value.parameter,  "time_stamp": new_value.time_stamp } ).exec();    
    
    if (!res) {
      res = await Value.create(new_value);
      return res;
    }
  
    if ( res.value !== new_value.value)  {
      //need update
      res = await Value.updateOne(res, new_value);
      /*
      res.matchedCount; // Number of documents matched
      res.modifiedCount; // Number of documents modified
      res.acknowledged; // Boolean indicating everything went smoothly.
      res.upsertedId; // null or an id containing a document that had to be upserted.
      res.upsertedCount; // Number indicating how many documents had to be upserted. Will either be 0 or 1.
      */
      return res;
    }       
  
    return res;
  }


  //================================================================

let from;
let to;
let days;

if (!argv.to) {
    console.error("--to param missing");
    process.exit(1);
}

if (!argv.days) {
    console.error("--days param missing");
    process.exit(1);
}

days = argv.days;

if (!argv.value) {
    console.error("--value param missing");
    process.exit(1);
}

let user = +process.env.USER_ID

let timer = setTimeout(async () => {
  await callsLoop(argv.to, days, op_data_config, argv.value, user); 
  process.exit(0);
}, 2000);

