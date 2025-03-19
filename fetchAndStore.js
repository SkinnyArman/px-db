require("dotenv").config();
const axios = require("axios");
const mongoose = require("mongoose");
const Holder = require("./holderModel");

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

const fetchAndStoreHolders = async () => {
    console.log("🚀 Fetching holders data...");
    let offset = 0;
    let hasMore = true;

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
            const bulkOps = addresses.map((h) => ({
                updateOne: {
                    filter: { address: h.address },
                    update: { 
                        $set: { 
                            balance: h.balance / (10 ** 9),
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
    }
};

// Run the function when the script is executed (Render cron job will trigger this)
fetchAndStoreHolders().then(() => {
    console.log("✅ Job finished. Exiting...");
    process.exit(0); // Ensures script stops after execution
});
