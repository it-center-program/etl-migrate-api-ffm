import pkg from "pg";
import dotenv from "dotenv";

dotenv.config();
const { Pool } = pkg;

const poolPG = new Pool({
  host: process.env.DB_PG_HOST || "10.0.0.114",
  user: process.env.DB_PG_USER || "nova_etl",
  password: process.env.DB_PG_PASSWORD || "etl!@#$",
  database: process.env.DB_PG_NAME || "datamart",
  port: process.env.DB_PG_PORT || 5432,
});

export default poolPG;
