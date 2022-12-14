const oracledb = require('oracledb');

  module.exports.selectOpDataBetween = async (connection, object, parameter, fromDate, toDate) => {
    
    let fromDateISO = fromDate.toISOString().slice(0, 10);
    let toDateISO = toDate.toISOString().slice(0, 10);

    let from = `TO_DATE('${fromDateISO}','YYYY-MM-DD')`;
    let to = `TO_DATE('${toDateISO}','YYYY-MM-DD')`;
    const sql_cmd = `SELECT * FROM OP_DATA WHERE OBJECT = ${object} AND PARAMETER = ${parameter} AND DDATE BETWEEN ${from} AND ${to}`;
    binds = {};

    // For a complete list of options see the documentation.
    options = {
      outFormat: oracledb.OUT_FORMAT_OBJECT,   // query result format
      // extendedMetaData: true,               // get extra metadata
      // prefetchRows:     100,                // internal buffer allocation size for tuning
      // fetchArraySize:   100                 // internal buffer allocation size for tuning
    };

    let result = await connection.execute(sql_cmd, binds, options);
  
    //console.dir(result.rows, { depth: null }); 
      
    let values = result.rows.map(r=>{ return {"object": object, "parameter":parameter, "value": r.VALUEN, "time_stamp": r.EDIT_DATE}}  );
    
    return values;  
  }

  module.exports.selectPHGstatesBetween = async (connection, object, parameter, fromDate, toDate) => {

    let fromDateISO = fromDate.toISOString().slice(0, 10);
    let toDateISO = toDate.toISOString().slice(0, 10);

    let from = `TO_DATE('${fromDateISO}','YYYY-MM-DD')`;
    let to = `TO_DATE('${toDateISO}','YYYY-MM-DD')`;
    binds = {};

    // For a complete list of options see the documentation.
    options = {
      outFormat: oracledb.OUT_FORMAT_OBJECT,   // query result format
      // extendedMetaData: true,               // get extra metadata
      // prefetchRows:     100,                // internal buffer allocation size for tuning
      // fetchArraySize:   100                 // internal buffer allocation size for tuning
    };

    const sql_cmd = `SELECT * FROM OP_PHG_STATES WHERE OBJECT = ${object} AND DDATE BETWEEN ${from} AND ${to}`;

      let result = await connection.execute(sql_cmd, binds, options);
  
      //console.dir(result.rows, { depth: null }); 
      //1- inject, 2-withdraw, 3-neutral
      let values = result.rows.map(r=> { 
          switch (r.STATE) {
            case 1:        
              return {"object": object, "state": "??????????????", "value": 1, "time_stamp": r.DDATE, "parameter": parameter };
            case 2:        
              return {"object": object, "state": "????????????", "value": 2, "time_stamp": r.DDATE, "parameter": parameter };
            case 3:        
              return {"object": object, "state": "????????????????", "value": 0, "time_stamp": r.DDATE, "parameter": parameter };    
            default:
              return null;
          }        
      });  
      return values;  
  }

  module.exports.execGetValueProc = async (connection, object, parameter, fromDateISO) => {
    let value;
    let ts = "";
    let from ="";

    if (fromDateISO.length == 10) {
      ts = fromDateISO + "T07:00:00"; //shift to contract hour ISO format
      from = `TO_DATE('${fromDateISO}','YYYY-MM-DD"T"HH24:MI:SS"Z"')`;
    } else {
      ts = fromDateISO.substring(0,19) + "Z"; //remove msec part
      from = `TO_DATE('${ts}','YYYY-MM-DD"T"HH24:MI:SS"Z"')`;
    } 


    binds = {};

    // For a complete list of options see the documentation.
    options = {
      outFormat: oracledb.OUT_FORMAT_OBJECT,   // query result format
      // extendedMetaData: true,               // get extra metadata
      // prefetchRows:     100,                // internal buffer allocation size for tuning
      // fetchArraySize:   100                 // internal buffer allocation size for tuning
    };
    const sql_cmd = `SELECT DIXML.PG_DATA.GET_VALUE(${object}, ${parameter}, ${from}) FROM DUAL`;

    let result = await connection.execute(sql_cmd, binds, options);

          //console.dir(result.rows, { depth: null }); 
          //result.rows : [ { "DIXML.PG_DATA.GET_VALUE(903001,33,'01-01-2018')": null } ]
          //[ { "DIXML.PG_DATA.GET_VALUE(903001,4,'01-01-2018')": '11,58' } ]        
          let kvp = result.rows[0];

          for (const key in kvp) {
            const v = kvp[key];
            if (v == null) continue;

            let val = v.replace(",", ".");  // ???

            value = {"object": object, "parameter":parameter, "value": val, "time_stamp": ts};
            //console.log(data); 
          }

          return value;
  }

  /*
  CREATE TABLE WEATHER_FORECAST
( 
  id number NOT NULL,
  location_id number NOT NULL,
  ddate date NOT NULL,
  t_min number,
  t_max number,
  t_avg number,
  CONSTRAINT weateher_forecast_pk PRIMARY KEY (id)
);

  */
module.exports.selectTemperaturesBetween = async (connection, object, fromDate, toDate) => {
    
  let fromDateISO = fromDate.toISOString().slice(0, 10);
  let toDateISO = toDate.toISOString().slice(0, 10);

  let from = `TO_DATE('${fromDateISO}','YYYY-MM-DD')`;
  let to = `TO_DATE('${toDateISO}','YYYY-MM-DD')`;
  const sql_cmd = `SELECT * FROM WEATHER_FORECAST WHERE location_id = ${object} AND DDATE BETWEEN ${from} AND ${to}`;
  binds = {};

  // For a complete list of options see the documentation.
  options = {
    outFormat: oracledb.OUT_FORMAT_OBJECT,   // query result format
    // extendedMetaData: true,               // get extra metadata
    // prefetchRows:     100,                // internal buffer allocation size for tuning
    // fetchArraySize:   100                 // internal buffer allocation size for tuning
  };

   let result = await connection.execute(sql_cmd, binds, options);

  //console.dir(result.rows, { depth: null }); 
    
  let values = [];
  result.rows.forEach(r => {
    let yyyy = r.DDATE.getFullYear();
    let mm = r.DDATE.getMonth();
    let dd = r.DDATE.getDate();

    let dt = new Date(yyyy, mm, dd);

    let v1 = {"object": object, "parameter":1, "value": r.T_MIN, "time_stamp": dt};
    values.push(v1);
    let v2 = {"object": object, "parameter":2, "value": r.T_MAX, "time_stamp": dt};
    values.push(v2);
    let v3 = {"object": object, "parameter":3, "value": r.T_AVG, "time_stamp": dt};
    values.push(v3);
  });
  return values;  
}