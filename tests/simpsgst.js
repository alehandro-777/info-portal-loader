require('dotenv').config();
require('../db')  //init mongoose
const Value =require('../models/value');
const SmartDate =require('../smartdate');


const psg_states_config= require('../json/psg-states.json');

const argv = require('minimist')(process.argv.slice(2));

async function callsLoop(end, dayCount, config, user) {

    for (let i = 0; i < config.length; i++) {
      try {
        const el = config[i];
  
        for (let j = 0; j < dayCount; j++) {
          let from = new SmartDate(end).currGasDay().addDay(-100*j).dt; 
          //simulate  
          let value = simulatePsgState(j, +el.object, 777, from, user);
          //console.log(value);    
          let res =  await upsertValue(value);  //
          console.log(res);         
        }     
  
      } catch (error) {
        console.log(error); 
      }  
  
    }
  
  }
  
  function simulatePsgState(step, object, parameter, time_stamp, upsertedId) {
    let s = step % 4;
    switch (s) {
      case 0:        
        return {"object": object, "parameter":parameter, "state": "Закачка", "value": 1, "time_stamp": time_stamp, "user":user };
      case 1:        
        return {"object": object, "parameter":parameter,"state": "Нейтраль", "value": 0, "time_stamp": time_stamp, "user":user };         
      case 2:        
        return {"object": object, "parameter":parameter,"state": "Відбор", "value": 2, "time_stamp": time_stamp, "user":user };
      case 3:        
        return {"object": object, "parameter":parameter,"state": "Нейтраль", "value": 0, "time_stamp": time_stamp, "user":user };    
 
      default:
        return null;
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
let periods;

if (!argv.to) {
    console.error("--to param missing");
    process.exit(1);
}

if (!argv.periods) {
    console.error("--periods param missing");
    process.exit(1);
}

periods = argv.periods;

let user = +process.env.USER_ID

let timer = setTimeout(async () => {
  await callsLoop(argv.to, periods, psg_states_config, user); 
  process.exit(0);
}, 2000);

