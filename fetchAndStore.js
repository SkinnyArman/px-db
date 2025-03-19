require("dotenv").config();
const axios = require("axios");
const mongoose = require("mongoose");
const Holder = require("./holderModel");
const express = require("express");

const app = express();
const PX_ADDRESS = "EQB420yQsZobGcy0VYDfSKHpG2QQlw-j1f_tPu1J488I__PX";
const API_LIMIT = 1000;
const HOLDERS_API_ADDRESS = `https://tonapi.io/v2/jettons/${PX_ADDRESS}/holders?limit=${API_LIMIT}`;
const RATE_LIMIT_DELAY = 15000; // Base delay in ms between calls
const MAX_RETRIES = 10; // Max retry attempts for API calls

mongoose.connect(process.env.MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
});
const db = mongoose.connection;
db.once("open", () => console.log("✅ Connected to MongoDB"));
db.on("error", (err) => console.error("❌ MongoDB Connection Error:", err));

// Global flag to control if the job is running
let isFetching = false;
let cancelFetching = false;  // This flag will be used to stop the fetch process

const fetchAndStoreHolders = async () => {
    if (isFetching) {
        console.log("A fetch is already in progress. Skipping this request.");
        return;
    }

    isFetching = true;
    cancelFetching = false;  // Reset cancel flag when starting a new fetch
    console.log("🚀 Fetching holders data...");
    let offset = 0;
    let hasMore = true;

    try {
        while (hasMore) {
            if (cancelFetching) {
                console.log("❌ Fetching process stopped.");
                break;  // Stop the loop if cancelFetching is set to true
            }

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
            const bulkOps = addresses.map((h) => ({
                updateOne: {
                    filter: { address: h.address },
                    update: { 
                        $set: { 
                            balance: h.balance,
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

// Create the "/run" endpoint to start the process
app.get("/run", (req, res) => {
    if (!isFetching) {
        fetchAndStoreHolders().then(() => {
            res.send("✅ Job started!");
        }).catch((err) => {
            res.status(500).send("❌ Error starting the job: " + err.message);
        });
    } else {
        res.send("⚠️ The job is already running.");
    }
});

// Create the "/stop" endpoint to stop the process
app.get("/stop", (req, res) => {
    if (isFetching) {
        cancelFetching = true;  // Set the flag to true to stop the job
        res.send("✅ Job stopped.");
    } else {
        res.send("⚠️ No job is currently running.");
    }
});

// Set up the server to listen on a port (e.g., 3000)
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`✅ Web service running on port ${PORT}`);
});
