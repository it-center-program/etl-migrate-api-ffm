import fs from "fs";
import path from "path";
import fetch from "node-fetch";

// --- ตั้งค่า ---
const API_URL = "http://10.0.0.114:8019";
const LOCK_FILE = path.join(process.cwd(), "migrate.lock");
const RETRY_DELAY_MS = 3000;

// --- ฟังก์ชันสร้าง lock ---
function isLocked() {
  return fs.existsSync(LOCK_FILE);
}

function createLock() {
  fs.writeFileSync(LOCK_FILE, String(Date.now()));
}

function removeLock() {
  if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE);
}

// --- migrate batch ---
async function migrateBatch() {
  try {
    console.log(LOCK_FILE);
    console.log(`${API_URL}/api/migrateV2`);
    const res = await fetch(`${API_URL}/api/migrateV2`);
    const data = await res.json();
    console.log(new Date().toISOString(), "Migrated batch:", data.count);
    return data.count;
  } catch (err) {
    console.error(
      new Date().toISOString(),
      "Error migrating batch:",
      err.message
    );
    return -1; // ใช้สำหรับ retry
  }
}

// --- main loop ---
async function runMigration() {
  if (isLocked()) {
    console.log(LOCK_FILE);
    console.log("Migration already running. Exiting.");
    return;
  }
  let c_error = 0;

  createLock();
  console.log("Migration started...");

  try {
    let hasMore = true;
    while (hasMore) {
      const count = await migrateBatch();

      if (count === -1) {
        c_error += 1;
        if (c_error > 3) {
          console.log("Too many errors. Exiting.");
          break;
        }
        console.log(`Retrying after ${RETRY_DELAY_MS / 1000}s...`);
        await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
        continue; // retry batch
      }

      if (!count || count === 0) {
        hasMore = false; // ไม่มีข้อมูลใหม่ → exit loop
        console.log("No more data. Migration finished.");
      }
    }
  } finally {
    removeLock();
  }
}

runMigration().catch((err) => {
  console.error("Migration failed:", err);
  removeLock();
});
