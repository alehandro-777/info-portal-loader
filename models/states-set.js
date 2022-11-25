const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const model = new Schema({
  _id : Number,
  set_name : String,    //friendly  name
  kvp : [{_id:false, key:String, value:String}],
});

module.exports = mongoose.model('digital_states', model);  