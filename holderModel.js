const mongoose = require("mongoose");

const holderSchema = new mongoose.Schema({
  address: { type: String, required: true, unique: true },
  balance: { type: Number, required: true },
});

const Holder = mongoose.model("Holder", holderSchema);

module.exports = Holder;
