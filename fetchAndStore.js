require("dotenv").config();
const axios = require("axios");
const mongoose = require("mongoose");
const Holder = require("./holderModel");
const cron = require("node-cron");

const PX_ADDRESS = "EQB420yQsZobGcy0VYDfSKHpG2QQlw-j1f_tPu1J488I__PX";
const API_LIMIT = 1000;
const HOLDERS_API_ADDRESS = `https://tonapi.io/v2/jettons/${PX_ADDRESS}/holders?limit=${API_LIMIT}`;
const RATE_LIMIT_DELAY = 15000; // Base delay in ms between calls
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
      // Connect the client to the server (optional starting in v4.7)
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

// Global flag to ensure only one instance runs at a time
let isFetching = false;

const fetchAndStoreHolders = async () => {
    if (isFetching) {
         console.log("A fetch is already in progress. Skipping this scheduled run.");
         return;
    }
    
    isFetching = true;
    console.log("🚀 Fetching holders data...");
    let offset = 0;
    let hasMore = true;
    const MAX_RETRIES = 10;

    try {
        while (hasMore) {
            let retries = 0;
            let success = false;
            let response;

            // Retry loop with exponential backoff
            while (!success && retries < MAX_RETRIES) {
                try {
                    response = await axios.get(`${HOLDERS_API_ADDRESS}&offset=${offset}`);
                    success = true;
                } catch (error) {
                    retries++;
                    const delay = RATE_LIMIT_DELAY * (2 ** retries);
                    console.error(`❌ API Fetch Error at offset ${offset}, retry ${retries} after ${delay}ms:`, error.message);
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }

            if (!success) {
                console.error(`❌ Failed to fetch data at offset ${offset} after ${MAX_RETRIES} retries. Exiting fetch loop.`);
                break;
            }

            const addresses = response.data.addresses;
            if (!addresses || addresses.length === 0) {
                hasMore = false;
                break;
            }

            // Convert data into bulk update operations with computed rank
            const bulkOps = addresses.map((h, index) => ({
                updateOne: {
                    filter: { address: h.address },
                    update: { 
                        $set: { 
                            balance: h.balance / (10 ** 9),
                            rank: offset + index + 1  // Compute rank based on batch offset and index
                        }
                    },
                    upsert: true,
                },
            }));

            // Perform bulk upsert
            await Holder.bulkWrite(bulkOps);

            offset += addresses.length;
            console.log(`✅ Updated ${addresses.length} holders, Offset: ${offset}`);

            // Wait between API calls to respect rate limits
            await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY));
        }
    } catch (err) {
        console.error("Unexpected error in fetchAndStoreHolders:", err);
    } finally {
        isFetching = false;
    }
};

// Schedule the job to run every hour.
// If a previous run is still active, the flag prevents a new instance from starting.
cron.schedule("0 * * * *", () => {
    console.log("⏳ Running scheduled holders update...");
    fetchAndStoreHolders();
});

// Initial Fetch on Start
fetchAndStoreHolders();
