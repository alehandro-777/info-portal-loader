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
              return {"object": object, "state": "Закачка", "value": 1, "time_stamp": r.DDATE, "parameter": parameter };
            case 2:        
              return {"object": object, "state": "Відбор", "value": 2, "time_stamp": r.DDATE, "parameter": parameter };
            case 3:        
              return {"object": object, "state": "Нейтраль", "value": 0, "time_stamp": r.DDATE, "parameter": parameter };    
            default:
              return null;
          }        
      });  
      return values;  
  }

  module.exports.execGetValueProc = async (connection, object, parameter, fromDate) => {
    let value;
    
    let fromDateISO = fromDate.toISOString().slice(0, 10);
    let from = `TO_DATE('${fromDateISO}','YYYY-MM-DD')`;

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

            value = {"object": object, "parameter":parameter, "value": val, "time_stamp": fromDate};
            //console.log(data); 
          }

          return value;
  }