require('dotenv').config();
const oracledb = require('oracledb');
require('./db')  //init mongoose
oracledb.initOracleClient({libDir: process.env.ORA_INSTANT_DIR});
const Value =require('./models/value');
const SmartDate =require('./smartdate');
const repository =require('./oraRepository');


const op_data_config= require('./json/op_data_sync.json');
const psg_states_config= require('./json/psg-states.json');

const argv = require('minimist')(process.argv.slice(2));


function getConnectionPromise() {
  const dbConfig = {
    user: process.env.ORA_USER,
    password: process.env.ORA_PASSW,
    connectString:  process.env.ORA_TNS_KEY
  };
  return oracledb.getConnection(dbConfig);
}

async function bulkInsertFrom_op_data(from, to, config) {
  let connection;

  try {

    connection = await getConnectionPromise();

    await loadLoop(connection, from, to, config);

    process.exit(0);
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

async function loadLoop(connection, from, to, config) {
    for (let i = 0; i < config.length; i++) {
        try {
            const el = config[i];
            let values = await repository.selectOpDataBetween(connection, +el.object, +el.parameter, from, to);
            let res = await Value.insertMany(values);
            console.log(res);           
        } catch (error) {
            console.log(error);
        }

      }  
}

async function bulkInsertFrom_psg_states(from, to, config) {
  let connection;

  try {

    connection = await getConnectionPromise();

    for (let i = 0; i < config.length; i++) {
      const el = config[i];
      let values = await repository.selectPHGstatesBetween(connection, +el.object, 777, from, to);
      let res = await Value.insertMany(values);
      console.log(res);
    }  
    
    process.exit(0);
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

//---------------------------------------------------------------

    let from;
    let to;

    if (!argv.from) {
        console.error("ERROR: --from parameter missing set to 2018-01-01");
        from = new Date("2018-01-01");
    } else {
        from = new Date(argv.from);
    }

    if (!argv.to) {
    console.error("ERROR: --to parameter missing set to 2030-01-01");
    to = new Date("2030-01-01");
    } else {
    to = new Date(argv.to);
    }



    bulkInsertFrom_op_data(from, to, op_data_config);
    bulkInsertFrom_psg_states(from, to, psg_states_config)