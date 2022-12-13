require('dotenv').config();
const oracledb = require('oracledb');
require('./db')  //init mongoose
//oracledb.initOracleClient({libDir: process.env.ORA_INSTANT_DIR});
const Value =require('./models/value');
const SmartDate =require('./smartdate');
const repository =require('./oraRepository');

const service = require('./service');

const op_data_config= require('./json/op_data_sync.json');
const psg_states_config= require('./json/psg-states.json');
const temperatures_config= require('./json/temperature.json');
const hh_config= require('./json/op_data_sync_hh.json');

const argv = require('minimist')(process.argv.slice(2));

const schedule = require('node-schedule');

const rule1 = new schedule.RecurrenceRule();// 7- 10 a.m. every hour, 5 min offset
rule1.minute = 5;
rule1.hour = [7,8,9];

const rule2 = new schedule.RecurrenceRule();  //every hour
rule2.minute = 5;

const rule3 = new schedule.RecurrenceRule();  //every hour temperatures
rule3.minute = 15;

const rule4 = new schedule.RecurrenceRule();//every day get values loop
rule4.minute = 0;
rule4.hour = 10;

const rule5 = new schedule.RecurrenceRule();//every hour psg states
rule5.minute = 1;



//------------------------------------------------------------------------------------
function getConnectionPromise() {
  const dbConfig = {
    user: process.env.ORA_USER,
    password: process.env.ORA_PASSW,
    connectString:  process.env.ORA_TNS_KEY
  };
  return oracledb.getConnection(dbConfig);
}

//step = -1 day, 
async function getValueCallsLoop(config) {

  let dayCount = +process.env.HISTORY_DAYS || 7;
  let connection;
  let gasDayIso = new SmartDate().nextGasDay().dt.toISOString();

  console.log("Starting getValueCallsLoop -> ", gasDayIso, dayCount);

  try {
    connection = await getConnectionPromise();

    await callsLoop(connection, gasDayIso, dayCount, config);

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

//step = -1 hour, 
async function getValueHHCallsLoop(config) {
  const hoursCount = 30;
  let connection;
  let now = new Date();
  now.setMinutes(0);

  console.log("Starting getValueHHCallsLoop -> ", now, hoursCount);

  try {
    connection = await getConnectionPromise();

    await callsLoopHH(connection, now, hoursCount, config);

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
        let dt = new Date(end);
        dt.setTime(dt.getTime() + -1*j* 24 * 3600 * 1000);
        
        let ts = dt.toISOString().substring(0,10);//ddate param for Oracle proc

        console.log( +el.object, +el.parameter, ts);

        let value = await repository.execGetValueProc(connection, +el.object, +el.parameter, ts);
        //let res = await Value.create(value);
        console.log(value);
        if (!value)   continue; //skip if no value
        let res =  await service.upsertValue(value);  //
        //console.log(res);         
      }     

    } catch (error) {
      console.log(error); 
    }  

  }

}

async function callsLoopHH(connection, end, hoursCount, config) {

  for (let i = 0; i < config.length; i++) {
    try {
      const el = config[i];

      for (let j = 0; j < hoursCount; j++) {

        let dt = new Date(end);
        dt.setTime(dt.getTime() + -1*j * 3600 * 1000);
        let ts = dt.toISOString();

        console.log("HH: ", +el.object, +el.parameter, ts);

        let value = await repository.execGetValueProc(connection, +el.object, +el.parameter, ts);
        //let res = await Value.create(value);
        console.log(value);
        if (!value)   continue; //skip if no value
        let res =  await service.upsertValue(value);  //
        //console.log(res);         
      }     

    } catch (error) {
      console.log(error); 
    }  

  }

}


async function InsertFrom_psg_states(config) {
  let connection;
  let from = new Date("2019-01-01");
  let to = new Date("2030-01-01");

  console.log("Starting InsertFrom_psg_states -> ", from, to);

  try {

    connection = await getConnectionPromise();

    for (let i = 0; i < config.length; i++) {
      const el = config[i];
      let values = await repository.selectPHGstatesBetween(connection, +el.object, 777, from, to);
      await service.upsertValuesArray(values)
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

async function InsertFromTemperatures(config) {
  const weatherForecastLength = 7;
  let connection;
  let from = new Date();  //now
  from.setHours(0);
  let to = new Date();
  to.setHours(0);
  to.setTime(to.getTime() + weatherForecastLength * 24 * 3600 * 1000);

  console.log("Starting InsertFromTemperatures -> ", from, to);
  try {

    connection = await getConnectionPromise();

    for (let i = 0; i < config.length; i++) {
      const el = config[i];
      let values = await repository.selectTemperaturesBetween(connection, +el.location_id, from, to);
      await service.upsertValuesArray(values)
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
const job1 = schedule.scheduleJob(rule1, function() {
  console.log(new Date().toISOString(), 'rule 1');
  //service.updateDaysFromHours();
});

const job2 = schedule.scheduleJob(rule2, function() {
  console.log(new Date().toISOString(), 'rule 2');
  //getValueHHCallsLoop(hh_config);
});

const job3 = schedule.scheduleJob(rule3, function() {
  console.log(new Date().toISOString(), 'rule 3');
  //InsertFromTemperatures(temperatures_config);
});

const job4 = schedule.scheduleJob(rule4, function() {
  console.log(new Date().toISOString(), 'rule 4');
  //getValueCallsLoop(op_data_config);
});

const job5 = schedule.scheduleJob(rule5, function() {
  console.log(new Date().toISOString(), 'rule 5');
  //InsertFrom_psg_states(psg_states_config);
});

async function timerCallback() {
  await service.updateDaysFromHours();
  await getValueHHCallsLoop(hh_config);
  await InsertFromTemperatures(temperatures_config);
  await getValueCallsLoop(op_data_config);
  await InsertFrom_psg_states(psg_states_config);
}

//timerCallback();


