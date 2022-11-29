require('dotenv').config();
const oracledb = require('oracledb');
require('./db')  //init mongoose
oracledb.initOracleClient({libDir: process.env.ORA_INSTANT_DIR});
const Value =require('./models/value');
const SmartDate =require('./smartdate');
const repository =require('./oraRepository');


const op_data_config= require('./json/op_data_sync.json');
const psg_states_config= require('./json/psg-states.json');
const temperatures_config= require('./json/temperature.json');

const argv = require('minimist')(process.argv.slice(2));


function getConnectionPromise() {
  const dbConfig = {
    user: process.env.ORA_USER,
    password: process.env.ORA_PASSW,
    connectString:  process.env.ORA_TNS_KEY
  };
  return oracledb.getConnection(dbConfig);
}

//step = -1 day, 
async function getValueCallsLoop(endIsoDay, dayCount, config) {
  let connection;
  console.log("Starting getValueCallsLoop -> ", endIsoDay, dayCount);
  try {
    connection = await getConnectionPromise();

    await callsLoop(connection, endIsoDay, dayCount, config);

    //process.exit(0);
  } 
  catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }    
}

async function callsLoop(connection, end, dayCount, config) {

  for (let i = 0; i < config.length; i++) {
    try {
      const el = config[i];

      for (let j = 0; j < dayCount; j++) {
        let from = new SmartDate(end).currGasDay().addDay(-1*j).dt; 
  
        let value = await repository.execGetValueProc(connection, +el.object, +el.parameter, from);
        //let res = await Value.create(value);   
        let res =  await upsertValue(value);  //
        console.log(res);         
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

async function upsertValuesArray(values) {
  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    await upsertValue(value);
  }
}

async function InsertFrom_psg_states(from, to, config) {
  let connection;
  console.log("Starting InsertFrom_psg_states -> ", from, to);

  try {

    connection = await getConnectionPromise();

    for (let i = 0; i < config.length; i++) {
      const el = config[i];
      let values = await repository.selectPHGstatesBetween(connection, +el.object, 777, from, to);
      await upsertValuesArray(values)
    }  
    
    //process.exit(0);
  } 
  catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }    
}

async function InsertFromTemperatures(from, to, config) {
  let connection;
  console.log("Starting InsertFromTemperatures -> ", from, to);
  try {

    connection = await getConnectionPromise();

    for (let i = 0; i < config.length; i++) {
      const el = config[i];
      let values = await repository.selectTemperaturesBetween(connection, +el.location_id, from, to);
      await upsertValuesArray(values)
    }  
    
    //process.exit(0);
  } 
  catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }    
}


//------------------------------------------------------------------
let timer;
let timeout = +process.env.TIMER_MS;

function schedule() {
  timer = setTimeout(() => {
    clearTimeout(timer); 
    timerCallback(); 
    schedule();
  }, timeout);  
}

async function timerCallback() {
  let dayCounter = +argv.days || +process.env.HISTORY_DAYS;
  let from;
  let to;

  if (!argv.from) {
      from = new SmartDate().currGasDay().addDay(-dayCounter);
  } else {
      from = new SmartDate(argv.from);
  }

  if (!argv.to) {
    to = new SmartDate().currGasDay();
  } else {
    to = new SmartDate(argv.to);
  } 

  let forecastDayCount = new SmartDate().currGasDay().addDay(7);

  await InsertFromTemperatures(to.dt, forecastDayCount.dt, temperatures_config);
  await getValueCallsLoop(to.dt, dayCounter, op_data_config);
  await InsertFrom_psg_states(from.dt, to.dt, psg_states_config);
}

timerCallback();
//schedule();


