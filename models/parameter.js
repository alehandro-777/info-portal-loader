const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const model = new Schema({
  _id : Number,            // unique id
  
  name: { type: String, default: "Parameter"},
  full_name: { type: String, default: ""},
  short_name: { type: String, default: "T"},

  type: { type: String, default: "text"}, //HTML5 any input type

  eu: { type: String, default: "kgf/cm2"},   //eng units
  min: { type: String, default: "0"},
  max: { type: String, default: "100"},
  step: { type: String, default: "0.1"},
  deadband: { type: Number, default: 0.1},

  fixed: { type: Number, default: 2},     //digits after dec point
  readonly: { type: Boolean, default: false},     //digits after dec point
  options: { type: Number, ref: "digital_states" },  //ref

},
{
  timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' }
});

module.exports = mongoose.model('parameters', model); 





