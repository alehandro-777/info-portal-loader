const Value =require('./models/value');
const SmartDate =require('./smartdate');

async function sum_withdraw_injection(gasday) {
    let objects = [9902293, 9902294];
    let parameters = [8];

    let _end = new SmartDate(gasday).currGasDay().dt;
    let _begin = new SmartDate(gasday).lastGasDay().dt; 

    const data = await Value.aggregate([
        { $match: { object:{ $in:objects }, parameter:{ $in:parameters }, time_stamp: { $gte: _begin, $lt: _end }} },
        { $sort: { time_stamp:1 } },
        { $group: { _id: {  object:"$object", parameter:"$parameter" }, 
                count: {$sum: 1},
                sum: {$sum: "$value"},
        } },
        { $set: { "time_stamp": _begin.toISOString() } }
    ]); 
        
    return data;
}

function upsertDaysFromHours(config, hourData) {
    config.forEach(async (cfg) => {
        let val = hourData.find(v=> v._id.object == cfg.object);
        if (val) {
            let newVal = {
                "object": cfg.dobject, 
                "parameter": cfg.dparameter, 
                "value": val.sum, 
                "time_stamp": val.time_stamp, 
                "user":3
            };
            let res = await upsertValue(newVal);
            console.log(newVal);
        }
    });
}

// from 7:00 - 10:00 do update last day withdraw/ injection from hour data
exports.updateDaysFromHours = async () => {
    let now = new Date();
    let config = require('./json/op_data_sync_hh.json');
    let data = await sum_withdraw_injection(now);
    upsertDaysFromHours(config, data);
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

  exports.upsertValue = upsertValue;
  exports.upsertValuesArray = upsertValuesArray;