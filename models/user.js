const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const model = new Schema({
  _id: Number,
  name: String,
  login: { type: String, required: true },
  password: String,                           //may be password hash ???
  is_domain: { type: Boolean, required: false },   //local, domain
  role: { type: Number, ref: "roles" },     //ref roles collection
  profile: { type: Number, ref: "user_profiles" }, //ref to gui profiles
},
{
  timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' }
});

module.exports = mongoose.model('users', model);  