import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import { sql, poolPromise } from "./db.js";
import poolPG from "./db_pg.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const port = process.env.PORT || 8019;

// ฟังก์ชันช่วยดึง lastId ล่าสุด
async function getLastId() {
  const res = await poolPG.query(
    "SELECT last_id FROM migrate_log_ffm WHERE status='success' ORDER BY id DESC LIMIT 1"
  );
  return res.rows[0]?.last_id || null;
}
async function getLastIdTest() {
  const res = await poolPG.query(
    "SELECT last_id FROM migrate_log_test WHERE status='success' ORDER BY id DESC LIMIT 1"
  );
  return res.rows[0]?.last_id || null;
}

app.get("/", async (req, res) => {
  try {
    // const lastId = 1541756;
    // const lastId = 1622319;
    const lastId = (await getLastId()) || 1541756;
    const pool = await poolPromise;

    const sql_mssql = `SELECT TOP 1000 * FROM [dbo].[View_crm_PB_SALEORDER_ALL_ETL] WHERE ORDER_TYPE = 'CRM' AND SO_DATE >= '2025-11-01' ${
      lastId ? `AND RECID2 > ${lastId}` : ""
    } ORDER BY RECID2 asc`;

    const result = await pool.request().query(sql_mssql);
    const data = result.recordset;
    res.json({ result: data, pg: lastId });
  } catch (err) {
    console.log(err);
    res.status(500).send(err.message);
  }
  // res.send("Hello Api ETL FFM! Port:" + process.env.PORT);
});

async function convertDate(date) {
  const d = new Date(date);
  return d.toISOString();
}

// ฟังก์ชันบันทึก log
async function saveLog({ lastId, recordCount, status, errorMessage }) {
  await poolPG.query(
    "INSERT INTO migrate_log_ffm (last_id, record_count, status, error_message) VALUES ($1,$2,$3,$4)",
    [lastId, recordCount, status, errorMessage]
  );
}

async function saveLogStart({ continueId, batchNo }) {
  const res = await poolPG.query(
    `INSERT INTO migrate_log_ffm (continue_id , batch_no, status)
     VALUES ($1, $2, 'running') RETURNING id`,
    [continueId, batchNo]
  );
  // console.log(res.rows[0].id);
  return res.rows[0].id;
}

async function saveLogFinish({
  logId,
  newLastId,
  recordCount,
  status,
  errorMessage,
}) {
  await poolPG.query(
    `UPDATE migrate_log_ffm
     SET last_id=$1, record_count=$2, status=$3, error_message=$4, finished_at=NOW()
     WHERE id=$5`,
    [newLastId, recordCount, status, errorMessage, logId]
  );
}

async function saveLogDetail(logId, row) {
  const sql = `
    INSERT INTO migrate_log_ffm_detail (log_id, recid, raw_data)
    VALUES ($1, $2, $3)
  `;
  await poolPG.query(sql, [logId, row.RECID2, row]);
}

// API สำหรับ cronjob เรียกทำ migration
app.get("/api/migrate", async (req, res) => {
  const startTime = Date.now();
  let client;
  return res.status(400).json({
    message: "Disable Route",
    count: 0,
  });
  try {
    const pool = await poolPromise;
    // const lastId = 1541756;
    const lastId = (await getLastId()) || 1541756;

    const sql_mssql = `SELECT TOP 1000 * FROM [dbo].[View_crm_PB_SALEORDER_ALL_ETL] WHERE ORDER_TYPE = 'CRM' AND SO_DATE >= '2025-11-01' ${
      lastId ? `AND RECID2 > ${lastId}` : ""
    } ORDER BY RECID2 asc`;

    const result = await pool.request().query(sql_mssql);

    const data = result.recordset;
    const recordCount = data.length;

    // เชื่อมต่อ PostgreSQL
    client = await poolPG.connect();
    await client.query("BEGIN");

    // ตรวจสอบข้อมูล
    if (!Array.isArray(data) || data.length === 0) {
      await saveLog({
        lastId,
        recordCount: 0,
        status: "success",
        errorMessage: null,
      });
      await client.query("COMMIT");
      const durationMs = Date.now() - startTime;
      return res.json({
        message: "No new data",
        count: 0,
        duration_ms: durationMs,
        duration_sec: (durationMs / 1000).toFixed(3),
      });
    }

    // ลบข้อมูลรอบที่ error ก่อนหน้า (ถ้ามี)
    await client.query(
      "DELETE FROM etl_crm WHERE recid > $1 AND rectype = 'FULFILLMENT'",
      [lastId || 0]
    );

    // แทรกข้อมูลใหม่
    for (const row of data) {
      let full_address;
      let datetime = "1900-01-01T00:00:00.000Z";
      let date = "1900-01-01";
      let time = "00:00:00";
      if (row.SO_DATETIME) {
        datetime = await convertDate(row.SO_DATETIME);
        date = datetime.substring(0, 10);
        time = datetime.substring(11, 19);
      }
      if (
        row.CUS_TUMBON === null &&
        row.CUS_AMPHOE === null &&
        row.CUS_CITY === null &&
        row.CUS_ZIP_CODE === null
      ) {
        full_address = null;
      } else {
        full_address = `${row.CUS_TUMBON} ${row.CUS_AMPHOE} ${row.CUS_CITY} ${row.CUS_ZIP_CODE}`;
      }
      await client.query(
        `INSERT INTO etl_crm (
        recid, po_datetime, po_date, po_time, po_no, shipping_code, shipping_by, shipping_name,
        cus_tel_no, hn_code, firstname, lastname, fullname, ship_address, ship_subdistrict,
        ship_district, ship_province, ship_zipcode, ship_psd, remark, cus_full_address,
        line_no, product_code, product_name, product_type_name, productdetail, productother,
        qty, priceperunit, totalprice, unitname, agentcode, empcode, paymenttype, paymentstatus,
        health_detail, sex, birthdate, submit_data_status, submit_call_status, channelname,
        sell_by, customer_status, comment, health, information, rectype, comid, packing_date,
        tax_id, tax_type, tax_email, tax_branch_code, tax_branch_name, description2, currency,
        invoiceno, invoicedate, invoice_req, status, so_nav, so_nav_ship_date, discount, bomid,
        order_qty, itemid, sorting_code, order_type
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15,
        $16,$17,$18,$19,$20,$21,
        $22,$23,$24,$25,$26,$27,
        $28,$29,$30,$31,$32,$33,$34,$35,
        $36,$37,$38,$39,$40,$41,
        $42,$43,$44,$45,$46,$47,$48,$49,$50,
        $51,$52,$53,$54,$55,$56,$57,$58,$59,$60,
        $61,$62,$63,$64,$65,$66
      )
      ON CONFLICT (id) DO NOTHING`,
        [
          row.RECID2,
          datetime,
          date,
          time,
          row.SO_ID,
          row.TRACKING_NO || "",
          "",
          row.SHIPPING || "",
          row.CUS_TEL || "",
          row.CUS_ID || "",
          row.CUS_FRIST_NAME || "",
          row.CUS_LAST_NAME || "",
          row.CUS_NAME || "",
          row.CUS_ADDRESS || "",
          row.CUS_TUMBON || "",
          row.CUS_AMPHOE || "",
          row.CUS_CITY || "",
          row.CUS_ZIP_CODE || "",
          full_address || "",
          row.REMARK,
          full_address || "",
          row.ITEM_NO,
          row.ITEM_MASTER,
          row.ITEM_NAME,
          row.ITEM_TYPE_NAME,
          row.ITEM_TYPE_NAME, // <<=== case-sensitive JSON key
          row.PACK_SET,
          row.QTY,
          row.UNITPRICE,
          row.TOTALPRICE,
          "",
          "",
          row.SALEMAN_ID,
          row.SHIPPING_PAYMENT,
          row.PAYMENT_STATUS,
          row.DISEASE || "",
          row.SEX || "",
          "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.CONSENT || "",
          "",
          row.CHANNEL_NAME || "",
          "",
          row.DESCRIPTION1 || "",
          "",
          "",
          "",
          "FULFILLMENT",
          row.COMID || "",
          row.PACKING_DATE
            ? await convertDate(row.PACKING_DATE)
            : "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.TAX_ID || "",
          row.TAX_TYPE || "",
          row.TAX_EMAIL || "",
          row.TAX_BRANCH_CODE || "",
          row.TAX_BRANCH_NAME || "",
          row.DESCRIPTION2 || "",
          row.CURRENCYID || "",
          row.INVOICENO || "",
          row.INVOICEDATE
            ? await convertDate(row.INVOICEDATE)
            : "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.INVOICE_REQ || "",
          row.STATUS || "",
          row.SO_NAV || "",
          row.SO_NAV_SHIP_DATE
            ? await convertDate(row.SO_NAV_SHIP_DATE)
            : "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.DISCOUNT || 0,
          row.BOMID || "",
          row.ORDER_QTY || 0,
          row.ITEMID || "",
          row.SORTING_CODE || "",
          row.ORDER_TYPE || "",
        ]
      );

      await saveLogDetail(logId, row);
    }

    // บันทึก log
    const newLastId = data[data.length - 1].RECID2;
    await saveLog({
      lastId: newLastId,
      recordCount: recordCount,
      status: "success",
      errorMessage: null,
    });

    await client.query("COMMIT");
    const durationMs = Date.now() - startTime;
    res.json({
      message: "Migrate success",
      count: recordCount,
      lastId: newLastId,
      duration_ms: durationMs,
      duration_sec: (durationMs / 1000).toFixed(3),
    });
  } catch (err) {
    console.error("Migrate error:", err);
    if (client) await client.query("ROLLBACK");

    await saveLog({
      lastId: null,
      recordCount: 0,
      status: "error",
      errorMessage: err.message,
    });

    res.status(500).json({ message: "Migrate failed", error: err.message });
  } finally {
    if (client) client.release();
  }
});

app.get("/api/migrateV2", async (req, res) => {
  const startTime = Date.now();
  let client;
  let logId;
  let lastId;
  try {
    const pool = await poolPromise;

    // ดึง lastId ล่าสุด
    // const lastId = 1541756;
    lastId = (await getLastId()) || 1541756;

    // หา batch_no ล่าสุดของวันนี้
    const batchRes = await poolPG.query(`
      SELECT COALESCE(MAX(batch_no), 0) + 1 AS batch_no
      FROM migrate_log_ffm
      WHERE DATE(started_at) = CURRENT_DATE
    `);
    const batchNo = batchRes.rows[0].batch_no;

    // บันทึกเริ่มต้น log
    logId = await saveLogStart({ continueId: lastId, batchNo });

    const sql = `
      SELECT TOP 1000 * 
      FROM [dbo].[View_crm_PB_SALEORDER_ALL_ETL]
      WHERE ORDER_TYPE = 'CRM'
        AND SO_DATE >= '2025-11-01'
        AND RECID2 > ${lastId}
      ORDER BY RECID2 ASC
    `;

    const result = await pool.request().query(sql);
    const data = result.recordset;
    const recordCount = data.length;

    client = await poolPG.connect();
    await client.query("BEGIN");

    if (recordCount === 0) {
      await saveLogFinish({
        logId,
        newLastId: lastId,
        recordCount: 0,
        status: "success",
        errorMessage: null,
      });

      await client.query("COMMIT");
      return res.json({ message: "All data synced", count: 0 });
    }

    // ลบข้อมูลรอบ error ก่อนหน้า (เฉพาะ record ที่ > lastId)
    await client.query(
      `DELETE FROM etl_crm WHERE recid > $1 AND rectype='FULFILLMENT'`,
      [lastId]
    );

    // INSERT data
    for (const row of data) {
      let full_address;
      let datetime = "1900-01-01T00:00:00.000Z";
      let date = "1900-01-01";
      let time = "00:00:00";
      if (row.SO_DATETIME) {
        datetime = await convertDate(row.SO_DATETIME);
        date = datetime.substring(0, 10);
        time = datetime.substring(11, 19);
      }
      if (
        row.CUS_TUMBON === null &&
        row.CUS_AMPHOE === null &&
        row.CUS_CITY === null &&
        row.CUS_ZIP_CODE === null
      ) {
        full_address = null;
      } else {
        full_address = `${row.CUS_TUMBON} ${row.CUS_AMPHOE} ${row.CUS_CITY} ${row.CUS_ZIP_CODE}`;
      }
      await client.query(
        `INSERT INTO etl_crm (
        recid, po_datetime, po_date, po_time, po_no, shipping_code, shipping_by, shipping_name,
        cus_tel_no, hn_code, firstname, lastname, fullname, ship_address, ship_subdistrict,
        ship_district, ship_province, ship_zipcode, ship_psd, remark, cus_full_address,
        line_no, product_code, product_name, product_type_name, productdetail, productother,
        qty, priceperunit, totalprice, unitname, agentcode, empcode, paymenttype, paymentstatus,
        health_detail, sex, birthdate, submit_data_status, submit_call_status, channelname,
        sell_by, customer_status, comment, health, information, rectype, comid, packing_date,
        tax_id, tax_type, tax_email, tax_branch_code, tax_branch_name, description2, currency,
        invoiceno, invoicedate, invoice_req, status, so_nav, so_nav_ship_date, discount, bomid,
        order_qty, itemid, sorting_code, order_type
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15,
        $16,$17,$18,$19,$20,$21,
        $22,$23,$24,$25,$26,$27,
        $28,$29,$30,$31,$32,$33,$34,$35,
        $36,$37,$38,$39,$40,$41,
        $42,$43,$44,$45,$46,$47,$48,$49,$50,
        $51,$52,$53,$54,$55,$56,$57,$58,$59,$60,
        $61,$62,$63,$64,$65,$66,$67,$68
      )
      ON CONFLICT (id) DO NOTHING`,
        [
          row.RECID2,
          datetime,
          date,
          time,
          row.SO_ID,
          row.TRACKING_NO || "",
          "",
          row.SHIPPING || "",
          row.CUS_TEL || "",
          row.CUS_ID || "",
          row.CUS_FRIST_NAME || "",
          row.CUS_LAST_NAME || "",
          row.CUS_NAME || "",
          row.CUS_ADDRESS || "",
          row.CUS_TUMBON || "",
          row.CUS_AMPHOE || "",
          row.CUS_CITY || "",
          row.CUS_ZIP_CODE || "",
          full_address || "",
          row.REMARK,
          full_address || "",
          row.ITEM_NO,
          row.ITEM_MASTER,
          row.ITEM_NAME,
          row.ITEM_TYPE_NAME,
          row.ITEM_TYPE_NAME, // <<=== case-sensitive JSON key
          row.PACK_SET,
          row.QTY,
          row.UNITPRICE,
          row.TOTALPRICE,
          "",
          "",
          row.SALEMAN_ID,
          row.SHIPPING_PAYMENT,
          row.PAYMENT_STATUS,
          row.DISEASE || "",
          row.SEX || "",
          "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.CONSENT || "",
          "",
          row.CHANNEL_NAME || "",
          "",
          row.DESCRIPTION1 || "",
          "",
          "",
          "",
          "FULFILLMENT",
          row.COMID || "",
          row.PACKING_DATE
            ? await convertDate(row.PACKING_DATE)
            : "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.TAX_ID || "",
          row.TAX_TYPE || "",
          row.TAX_EMAIL || "",
          row.TAX_BRANCH_CODE || "",
          row.TAX_BRANCH_NAME || "",
          row.DESCRIPTION2 || "",
          row.CURRENCYID || "",
          row.INVOICENO || "",
          row.INVOICEDATE
            ? await convertDate(row.INVOICEDATE)
            : "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.INVOICE_REQ || "",
          row.STATUS || "",
          row.SO_NAV || "",
          row.SO_NAV_SHIP_DATE
            ? await convertDate(row.SO_NAV_SHIP_DATE)
            : "1900-01-01T00:00:00.000Z", // แปลงเป็น DATE
          row.DISCOUNT || 0,
          row.BOMID || "",
          row.ORDER_QTY || 0,
          row.ITEMID || "",
          row.SORTING_CODE || "",
          row.ORDER_TYPE || "",
        ]
      );

      await saveLogDetail(logId, row);
    }

    const newLastId = data[data.length - 1].RECID2;

    await saveLogFinish({
      logId,
      newLastId,
      recordCount,
      status: "success",
      errorMessage: null,
    });

    await client.query("COMMIT");

    const duration = Date.now() - startTime;
    res.json({
      message: "Batch completed",
      batch_no: batchNo,
      count: recordCount,
      lastId: newLastId,
      duration_ms: duration,
    });
  } catch (err) {
    if (client) await client.query("ROLLBACK");

    await saveLogFinish({
      logId,
      newLastId: lastId,
      recordCount: 0,
      status: "error",
      errorMessage: err.message,
    });

    res.status(500).json({ error: err.message });
  } finally {
    if (client) client.release();
  }
});

// server.js
app.get("/api/logs", async (req, res) => {
  try {
    const result = await poolPG.query(`
      SELECT id, batch_no, last_id, record_count, status, started_at, finished_at
      FROM migrate_log_test
      ORDER BY started_at DESC
      LIMIT 100
    `);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/logs/errors", async (req, res) => {
  try {
    const result = await poolPG.query(`
      SELECT id, batch_no, last_id, record_count, status, started_at, finished_at, error_message
      FROM migrate_log_test
      WHERE status = 'error'
      ORDER BY started_at DESC
    `);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/logs/:logId", async (req, res) => {
  const { logId } = req.params;
  try {
    const result = await poolPG.query(
      `SELECT id, log_id, recid, raw_data
       FROM migrate_log_test_detail
       WHERE log_id = $1`,
      [logId]
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Batch not found" });
    }

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(port, () => {
  console.log(`✅ Server running at http://localhost:${port}`);
});
