const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const model = new Schema({
  _id : Number,            // unique id  
  name: { type: String, default: "ObjectName"},
  full_name: { type: String, default: ""},    
  deleted: { type: Boolean, default: false},  
},
{
  timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' }
});

module.exports = mongoose.model('objects', model); 





