require("dotenv").config();
const axios = require("axios");
const mongoose = require("mongoose");
const Holder = require("./holderModel");
const cron = require("node-cron");

const PX_ADDRESS = "EQB420yQsZobGcy0VYDfSKHpG2QQlw-j1f_tPu1J488I__PX";
const API_LIMIT = 1000;
const HOLDERS_API_ADDRESS = `https://tonapi.io/v2/jettons/${PX_ADDRESS}/holders?limit=${API_LIMIT}`;
const RATE_LIMIT_DELAY = 15000;
const { MongoClient, ServerApiVersion } = require('mongodb');
const uri = "mongodb+srv://PxAdmin:0JAbDhvWpn3yKmYl@cluster0.hj6dj4y.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";


const client = new MongoClient(uri, {
    serverApi: {
      version: ServerApiVersion.v1,
      strict: true,
      deprecationErrors: true,
    }
  });

async function run() {
    try {
      // Connect the client to the server	(optional starting in v4.7)
      await client.connect();
      // Send a ping to confirm a successful connection
      await client.db("admin").command({ ping: 1 });
      console.log("Pinged your deployment. You successfully connected to MongoDB!");
    } finally {
      // Ensures that the client will close when you finish/error
      await client.close();
    }
  }
  run().catch(console.dir);

mongoose.connect(process.env.MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  const db = mongoose.connection;
  db.once("open", () => console.log("✅ Connected to MongoDB"));
  db.on("error", (err) => console.error("❌ MongoDB Connection Error:", err));
  

  const fetchAndStoreHolders = async () => {
    console.log("🚀 Fetching holders data...");
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
        try {
            const response = await axios.get(`${HOLDERS_API_ADDRESS}&offset=${offset}`);
            const addresses = response.data.addresses;

            if (!addresses || addresses.length === 0) {
                hasMore = false;
                break;
            }

            // Convert data into bulk update operations
            const bulkOps = addresses.map((h) => ({
                updateOne: {
                    filter: { address: h.address }, // Find existing holder
                    update: { $set: { balance: h.balance } }, // Update balance
                    upsert: true, // Insert if not exists
                },
            }));

            // Perform bulk upsert
            await Holder.bulkWrite(bulkOps);

            offset += addresses.length;
            console.log(`✅ Updated ${addresses.length} holders, Offset: ${offset}`);

            await new Promise((resolve) => setTimeout(resolve, RATE_LIMIT_DELAY));
        } catch (error) {
            console.error("❌ API Fetch Error:", error);
            break;
        }
    }
};
  
// Run every hour
cron.schedule("0 * * * *", () => {
    console.log("⏳ Running scheduled holders update...");
    fetchAndStoreHolders();
  });
  
  // Initial Fetch on Start
  fetchAndStoreHolders();
  