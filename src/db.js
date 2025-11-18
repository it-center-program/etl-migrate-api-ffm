// db.js
import sql from "mssql";
import dotenv from "dotenv";

dotenv.config();

// 🔧 ตั้งค่าการเชื่อมต่อ SQL Server
const config = {
  user: process.env.DB_USER || "sa", // ชื่อผู้ใช้ SQL Server
  password: process.env.DB_PASSWORD || "1234", // รหัสผ่าน
  server: process.env.DB_HOST || "127.0.0.1", // หรือ IP เช่น 192.168.1.10
  database: process.env.DB_NAME || "master",
  options: {
    encrypt: false, // ปิดถ้าเป็น SQL Server local
    trustServerCertificate: true, // เปิดถ้าใช้ self-signed cert
  },
};

// สร้าง connection pool
const poolPromise = new sql.ConnectionPool(config)
  .connect()
  .then((pool) => {
    console.log("✅ Connected to SQL Server");
    return pool;
  })
  .catch((err) => {
    console.error("❌ Database Connection Failed:", err);
    throw err;
  });

export { sql, poolPromise };
