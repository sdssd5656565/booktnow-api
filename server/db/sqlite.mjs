import Database from "better-sqlite3";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { existsSync, mkdirSync } from "fs";
import { randomBytes, scryptSync } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// إنشاء مجلد data إذا لم يكن موجوداً
const dataDir = join(__dirname, "../../data");
if (!existsSync(dataDir)) {
  mkdirSync(dataDir, { recursive: true });
}

// إنشاء قاعدة البيانات
const dbPath =
  globalThis.process?.env?.BACKEND_DB_FILE || join(dataDir, "booktnow.db");

// إنشاء المجلد الأب إذا لم يكن موجودًا
const dbDir = dirname(dbPath);
if (!existsSync(dbDir)) {
  mkdirSync(dbDir, { recursive: true });
}

const db = new Database(dbPath);
db.pragma("foreign_keys = ON");

const ALLOWED_TABLES = new Set([
  "users", "sessions", "devices", "favorites",
  "bookings", "watches", "referrals", "events", "identities",
  "tickets", "ticket_messages",
]);

function assertSafeIdentifier(value, label) {
  if (!ALLOWED_TABLES.has(value) && !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
    throw new Error(`Unsafe SQL identifier for ${label}: "${value}"`);
  }
}

function getTableColumns(tableName) {
  assertSafeIdentifier(tableName, "table");
  return db
    .prepare(`PRAGMA table_info(${tableName})`)
    .all()
    .map((row) => row.name);
}

function ensureColumn(tableName, columnName, columnDefinition) {
  assertSafeIdentifier(tableName, "table");
  assertSafeIdentifier(columnName, "column");
  const columns = new Set(getTableColumns(tableName));
  if (columns.has(columnName)) return;
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnDefinition}`);
}

function hashLegacyPassword(password, salt = randomBytes(16).toString("hex")) {
  const passwordHash = scryptSync(String(password), salt, 64).toString("hex");
  return { passwordHash, passwordSalt: salt };
}

function migrateLegacyUsers() {
  const columns = new Set(getTableColumns("users"));
  const hasLegacyPassword = columns.has("password");
  const hasPasswordHash = columns.has("passwordHash");
  const hasPasswordSalt = columns.has("passwordSalt");

  if (!hasLegacyPassword || !hasPasswordHash || !hasPasswordSalt) return;

  const rows = db
    .prepare(`
      SELECT id, password
      FROM users
      WHERE COALESCE(passwordHash, '') = ''
         OR COALESCE(passwordSalt, '') = ''
    `)
    .all();

  const updateStmt = db.prepare(`
    UPDATE users
    SET passwordHash = ?,
        passwordSalt = ?,
        password = ''
    WHERE id = ?
  `);

  for (const row of rows) {
    const legacyPassword = String(row.password || "").trim();
    if (!legacyPassword) continue;
    const { passwordHash, passwordSalt } = hashLegacyPassword(legacyPassword);
    updateStmt.run(passwordHash, passwordSalt, row.id);
  }
}

function parseJsonPayload(payload) {
  if (payload == null || payload === "") return {};
  if (typeof payload === "object") return payload;
  try {
    return JSON.parse(payload);
  } catch {
    return {};
  }
}

function hydratePayloadRow(row) {
  if (!row) return null;
  const payload = parseJsonPayload(row.payload);
  return {
    ...row,
    payload,
    ...payload,
  };
}

// تفعيل foreign keys
// db.pragma('foreign_keys = ON');

console.log(`✅ SQLite database initialized at: ${dbPath}`);

// إنشاء الجداول
function initializeTables() {
  // جدول المستخدمين
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL,
      passwordHash TEXT NOT NULL,
      passwordSalt TEXT NOT NULL,
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  ensureColumn("users", "name", "name TEXT NOT NULL DEFAULT ''");
  ensureColumn(
    "users",
    "passwordHash",
    "passwordHash TEXT NOT NULL DEFAULT ''",
  );
  ensureColumn(
    "users",
    "passwordSalt",
    "passwordSalt TEXT NOT NULL DEFAULT ''",
  );
  ensureColumn(
    "users",
    "createdAt",
    "createdAt DATETIME",
  );

  // جدول الجلسات
  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      lastSeenAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      expiresAt DATETIME,
      FOREIGN KEY(userId) REFERENCES users(id)
    )
  `);
  ensureColumn(
    "sessions",
    "lastSeenAt",
    "lastSeenAt DATETIME",
  );

  // جدول الأجهزة
  db.exec(`
    CREATE TABLE IF NOT EXISTS devices (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      sessionToken TEXT,
      name TEXT,
      platform TEXT,
      browser TEXT,
      language TEXT,
      biometricEnabled INTEGER NOT NULL DEFAULT 0,
      current INTEGER NOT NULL DEFAULT 0,
      lastSeenAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول المفضلة
  db.exec(`
    CREATE TABLE IF NOT EXISTS favorites (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      productId TEXT NOT NULL,
      productName TEXT NOT NULL,
      category TEXT NOT NULL,
      destination TEXT,
      price REAL,
      currency TEXT DEFAULT 'SAR',
      image TEXT,
      provider TEXT,
      payload TEXT NOT NULL DEFAULT '{}',
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول الحجوزات
  db.exec(`
    CREATE TABLE IF NOT EXISTS bookings (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      ref TEXT NOT NULL,
      category TEXT NOT NULL,
      destination TEXT,
      productName TEXT NOT NULL,
      provider TEXT NOT NULL,
      price REAL NOT NULL,
      currency TEXT DEFAULT 'SAR',
      status TEXT DEFAULT 'pending',
      checkIn TEXT,
      checkOut TEXT,
      passengers INTEGER,
      payload TEXT NOT NULL DEFAULT '{}',
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول الهوية
  db.exec(`
    CREATE TABLE IF NOT EXISTS identities (
      userId TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول المتابعة
  db.exec(`
    CREATE TABLE IF NOT EXISTS watches (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      category TEXT NOT NULL,
      destination TEXT NOT NULL,
      targetPrice REAL NOT NULL,
      currency TEXT DEFAULT 'SAR',
      status TEXT DEFAULT 'active',
      payload TEXT NOT NULL DEFAULT '{}',
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول الإحالات
  db.exec(`
    CREATE TABLE IF NOT EXISTS referrals (
      referralId TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      partnerId TEXT NOT NULL,
      partnerName TEXT NOT NULL,
      productId TEXT NOT NULL,
      productName TEXT NOT NULL,
      amount REAL NOT NULL DEFAULT 0,
      currency TEXT DEFAULT 'SAR',
      commissionModel TEXT NOT NULL DEFAULT 'cps',
      commissionRate REAL NOT NULL DEFAULT 0.03,
      status TEXT NOT NULL DEFAULT 'created',
      commission REAL NOT NULL DEFAULT 0,
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      handoverAt DATETIME,
      conversionAt DATETIME
    )
  `);

  // جدول الأحداث التشغيلية
  db.exec(`
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      name TEXT NOT NULL,
      payload TEXT NOT NULL DEFAULT '{}',
      eventAt INTEGER,
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول تذاكر خدمة العملاء
  db.exec(`
    CREATE TABLE IF NOT EXISTS tickets (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT 'other',
      category TEXT,
      partnerId TEXT,
      partnerName TEXT,
      referralId TEXT,
      status TEXT NOT NULL DEFAULT 'open',
      priority TEXT NOT NULL DEFAULT 'medium',
      summary TEXT,
      resolution TEXT,
      payload TEXT NOT NULL DEFAULT '{}',
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // جدول رسائل التذاكر
  db.exec(`
    CREATE TABLE IF NOT EXISTS ticket_messages (
      id TEXT PRIMARY KEY,
      ticketId TEXT NOT NULL,
      sender TEXT NOT NULL DEFAULT 'customer',
      content TEXT NOT NULL,
      metadata TEXT NOT NULL DEFAULT '{}',
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(ticketId) REFERENCES tickets(id)
    )
  `);

  ensureColumn("favorites", "payload", "payload TEXT NOT NULL DEFAULT '{}' ");
  ensureColumn("bookings", "payload", "payload TEXT NOT NULL DEFAULT '{}' ");
  ensureColumn("watches", "payload", "payload TEXT NOT NULL DEFAULT '{}' ");
  ensureColumn(
    "referrals",
    "commission",
    "commission REAL NOT NULL DEFAULT 0",
  );
  ensureColumn(
    "referrals",
    "handoverAt",
    "handoverAt DATETIME",
  );
  ensureColumn(
    "referrals",
    "conversionAt",
    "conversionAt DATETIME",
  );
  ensureColumn(
    "events",
    "payload",
    "payload TEXT NOT NULL DEFAULT '{}'",
  );
  ensureColumn("events", "eventAt", "eventAt INTEGER");

  ensureColumn("devices", "sessionToken", "sessionToken TEXT");
  ensureColumn("devices", "name", "name TEXT");
  ensureColumn("devices", "platform", "platform TEXT");
  ensureColumn("devices", "browser", "browser TEXT");
  ensureColumn("devices", "language", "language TEXT");
  ensureColumn(
    "devices",
    "biometricEnabled",
    "biometricEnabled INTEGER NOT NULL DEFAULT 0",
  );
  ensureColumn("devices", "current", "current INTEGER NOT NULL DEFAULT 0");
  ensureColumn(
    "devices",
    "lastSeenAt",
    "lastSeenAt DATETIME",
  );

  migrateLegacyUsers();

  console.log("✅ All database tables initialized");
}

// تهيئة الجداول
initializeTables();

// دوال المستخدمين
export const userDB = {
  create: ({
    id,
    username,
    email,
    name,
    password,
    passwordHash,
    passwordSalt,
  }) => {
    const columns = new Set(getTableColumns("users"));
    const normalizedEmail = String(email || "").toLowerCase();
    const normalizedName =
      String(name || normalizedEmail.split("@")[0] || "User").trim() || "User";
    const normalizedUsername =
      String(username || normalizedEmail.split("@")[0] || normalizedName).trim() ||
      normalizedEmail;

    const payload = {
      id,
      email: normalizedEmail,
      name: normalizedName,
      passwordHash,
      passwordSalt,
      username: normalizedUsername,
      password: columns.has("password") ? "" : undefined,
    };

    const insertableColumns = Object.entries(payload)
      .filter(([key, value]) => columns.has(key) && value !== undefined)
      .map(([key]) => key);

    const stmt = db.prepare(`
      INSERT INTO users (${insertableColumns.join(", ")})
      VALUES (${insertableColumns.map(() => "?").join(", ")})
    `);
    stmt.run(...insertableColumns.map((column) => payload[column]));
    return userDB.findById(id);
  },

  findByEmail: (email) => {
    const stmt = db.prepare("SELECT * FROM users WHERE email = ?");
    return stmt.get(email.toLowerCase());
  },

  findById: (id) => {
    const stmt = db.prepare("SELECT * FROM users WHERE id = ?");
    return stmt.get(id);
  },

  count: () => {
    const stmt = db.prepare("SELECT COUNT(*) as count FROM users");
    return Number(stmt.get()?.count || 0);
  },

  updateName: (userId, name) => {
    const stmt = db.prepare('UPDATE users SET name = ? WHERE id = ?')
    const result = stmt.run(String(name || '').trim() || 'User', userId)
    return result.changes > 0
  },

  updatePassword: (userId, passwordHash, passwordSalt) => {
    const columns = new Set(getTableColumns("users"));
    const assignments = ["passwordHash = ?", "passwordSalt = ?"];
    const values = [passwordHash, passwordSalt];
    if (columns.has("password")) {
      assignments.push("password = ?");
      values.push("");
    }
    const stmt = db.prepare(`
      UPDATE users SET ${assignments.join(", ")} 
      WHERE id = ?
    `);
    const result = stmt.run(...values, userId);
    return result.changes > 0;
  },
};

// دوال الجلسات
export const sessionDB = {
  create: ({
    token,
    userId,
    expiresAt,
    profileId,
    email,
    deviceInfo,
    ipAddress,
    lastSeenAt,
  }) => {
    const columns = new Set(getTableColumns("sessions"));
    const payload = {
      token,
      userId,
      expiresAt: expiresAt || null,
      profileId: profileId || "default",
      email: email || `${String(userId || "session")}@booktnow.local`,
      deviceInfo: deviceInfo || null,
      ipAddress: ipAddress || null,
      lastSeenAt: lastSeenAt || new Date().toISOString(),
    };
    const insertableColumns = Object.entries(payload)
      .filter(([key, value]) => columns.has(key) && value !== undefined)
      .map(([key]) => key);
    const stmt = db.prepare(`
      INSERT INTO sessions (${insertableColumns.join(", ")})
      VALUES (${insertableColumns.map(() => "?").join(", ")})
    `);
    stmt.run(...insertableColumns.map((column) => payload[column]));
    return payload;
  },

  findByToken: (token) => {
    const stmt = db.prepare("SELECT * FROM sessions WHERE token = ?");
    const session = stmt.get(token);
    if (!session) return null;
    if (session.expiresAt && new Date(session.expiresAt) <= new Date()) {
      sessionDB.deleteByToken(token);
      return null;
    }
    return session;
  },

  touch: (token) => {
    if (!getTableColumns("sessions").includes("lastSeenAt")) return true;
    const stmt = db.prepare(
      "UPDATE sessions SET lastSeenAt = CURRENT_TIMESTAMP WHERE token = ?",
    );
    const result = stmt.run(token);
    return result.changes > 0;
  },

  deleteByToken: (token) => {
    const stmt = db.prepare("DELETE FROM sessions WHERE token = ?");
    const result = stmt.run(token);
    return result.changes > 0;
  },
};

// دوال المفضلة
export const favoriteDB = {
  create: (favorite) => {
    const stmt = db.prepare(`
      INSERT INTO favorites (id, userId, productId, productName, category, destination, price, currency, image, provider, payload)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      favorite.id,
      favorite.userId,
      favorite.productId,
      favorite.productName,
      favorite.category,
      favorite.destination || "",
      favorite.price || 0,
      favorite.currency || "SAR",
      favorite.image || "",
      favorite.provider || "",
      JSON.stringify(favorite.payload || favorite),
    );
    return hydratePayloadRow({
      ...favorite,
      payload: favorite.payload || favorite,
    });
  },

  deleteByUserId: (userId) => {
    const stmt = db.prepare("DELETE FROM favorites WHERE userId = ?");
    stmt.run(userId);
    return true;
  },

  replaceForUser: (userId, items) => {
    favoriteDB.deleteByUserId(userId);
    const saved = [];
    for (const [index, item] of (Array.isArray(items) ? items : []).entries()) {
      const payload = parseJsonPayload(item);
      const favorite = {
        id: String(payload.id || item?.id || `favorite_${Date.now()}_${index}`),
        userId,
        productId: String(
          payload.productId ||
            item?.productId ||
            item?.id ||
            payload.id ||
            `favorite_${index}`,
        ),
        productName: String(
          payload.productName ||
            payload.name ||
            item?.productName ||
            item?.name ||
            "",
        ),
        category: String(
          payload.category ||
            item?.category ||
            payload.kind ||
            item?.kind ||
            "general",
        ),
        destination: String(
          payload.destination ||
            item?.destination ||
            payload.location ||
            item?.location ||
            "",
        ),
        price: Number(payload.price ?? item?.price ?? 0),
        currency: String(payload.currency || item?.currency || "SAR"),
        image: String(payload.image || item?.image || ""),
        provider: String(payload.provider || item?.provider || ""),
        payload,
      };
      favoriteDB.create(favorite);
      saved.push(hydratePayloadRow(favorite));
    }
    return saved;
  },

  findByUserId: (userId) => {
    const stmt = db.prepare(
      "SELECT * FROM favorites WHERE userId = ? ORDER BY createdAt DESC",
    );
    return stmt.all(userId).map(hydratePayloadRow);
  },

  delete: (id, userId) => {
    const stmt = db.prepare(
      "DELETE FROM favorites WHERE id = ? AND userId = ?",
    );
    const result = stmt.run(id, userId);
    return result.changes > 0;
  },

  exists: (userId, productId) => {
    const stmt = db.prepare(
      "SELECT COUNT(*) as count FROM favorites WHERE userId = ? AND productId = ?",
    );
    const result = stmt.get(userId, productId);
    return result.count > 0;
  },
};

// دوال الحجوزات
export const bookingDB = {
  create: ({
    id,
    userId,
    category,
    destination,
    productName,
    provider,
    price,
    payload,
  }) => {
    const storedPayload = parseJsonPayload(payload);
    const ref = String(storedPayload.ref || storedPayload.id || id);
    const stmt = db.prepare(`
      INSERT INTO bookings (id, userId, ref, category, destination, productName, provider, price, currency, status, checkIn, checkOut, passengers, payload)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      id,
      userId,
      ref,
      String(category || "hotels"),
      String(destination || ""),
      String(productName || ""),
      String(provider || ""),
      Number(price || 0),
      String(storedPayload.currency || "SAR"),
      String(storedPayload.status || "confirmed"),
      storedPayload.checkIn || null,
      storedPayload.checkOut || null,
      storedPayload.passengers || 1,
      JSON.stringify(storedPayload),
    );
    return hydratePayloadRow({
      id,
      userId,
      ref,
      category: String(category || "hotels"),
      destination: String(destination || ""),
      productName: String(productName || ""),
      provider: String(provider || ""),
      price: Number(price || 0),
      currency: String(storedPayload.currency || "SAR"),
      status: String(storedPayload.status || "confirmed"),
      checkIn: storedPayload.checkIn || null,
      checkOut: storedPayload.checkOut || null,
      passengers: storedPayload.passengers || 1,
      payload: storedPayload,
    });
  },

  findByUserId: (userId) => {
    const stmt = db.prepare(
      "SELECT * FROM bookings WHERE userId = ? ORDER BY createdAt DESC",
    );
    return stmt.all(userId).map(hydratePayloadRow);
  },

  findById: (id, userId) => {
    const stmt = db.prepare(
      "SELECT * FROM bookings WHERE id = ? AND userId = ?",
    );
    return stmt.get(id, userId);
  },

  updateStatus: (id, userId, status) => {
    const stmt = db.prepare(
      "UPDATE bookings SET status = ?, updatedAt = CURRENT_TIMESTAMP WHERE id = ? AND userId = ?",
    );
    const result = stmt.run(status, id, userId);
    return result.changes > 0;
  },

  summaryByUserId: (userId) => {
    const stmt = db.prepare(`
      SELECT
        COUNT(*) as total,
        COALESCE(SUM(price), 0) as revenue
      FROM bookings
      WHERE userId = ?
    `);
    const result = stmt.get(userId) || {};
    return {
      total: Number(result.total || 0),
      revenue: Number(result.revenue || 0),
    };
  },
};

// دوال المتابعة
export const watchDB = {
  create: ({ id, userId, payload }) => {
    const storedPayload = parseJsonPayload(payload);
    const category = String(storedPayload.category || "hotels");
    const destination = String(storedPayload.destination || "");
    const targetPrice = Number(
      storedPayload.targetPrice ?? storedPayload.targetValueDisplay ?? 0,
    );
    const currency = String(
      storedPayload.currency || storedPayload.targetCurrency || "SAR",
    );
    const status = String(storedPayload.status || "active");
    const stmt = db.prepare(`
      INSERT INTO watches (id, userId, category, destination, targetPrice, currency, status, payload)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      id,
      userId,
      category,
      destination,
      targetPrice,
      currency,
      status,
      JSON.stringify(storedPayload),
    );
    return hydratePayloadRow({
      id,
      userId,
      category,
      destination,
      targetPrice,
      currency,
      status,
      payload: storedPayload,
    });
  },

  findByUserId: (userId) => {
    const stmt = db.prepare(
      "SELECT * FROM watches WHERE userId = ? ORDER BY createdAt DESC",
    );
    return stmt.all(userId).map(hydratePayloadRow);
  },

  updateStatus: (id, userId, status) => {
    const stmt = db.prepare(
      "UPDATE watches SET status = ?, updatedAt = CURRENT_TIMESTAMP WHERE id = ? AND userId = ?",
    );
    const result = stmt.run(status, id, userId);
    return result.changes > 0;
  },

  deleteById: (id, userId) => {
    const stmt = db.prepare("DELETE FROM watches WHERE id = ? AND userId = ?");
    const result = stmt.run(id, userId);
    return result.changes > 0;
  },

  delete: (id, userId) => {
    return watchDB.deleteById(id, userId);
  },
};

function normalizeReferralRow(row) {
  if (!row) return null;
  return {
    ...row,
    amount: Number(row.amount || 0),
    commissionRate: Number(row.commissionRate || 0),
    commission: Number(row.commission || 0),
  };
}

export const referralDB = {
  create: ({
    referralId,
    userId,
    partnerId,
    partnerName,
    productId,
    productName,
    amount,
    currency,
    commissionModel,
    commissionRate,
    status = "created",
    commission = 0,
  }) => {
    const stmt = db.prepare(`
      INSERT INTO referrals (
        referralId,
        userId,
        partnerId,
        partnerName,
        productId,
        productName,
        amount,
        currency,
        commissionModel,
        commissionRate,
        status,
        commission
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(referralId) DO UPDATE SET
        userId = excluded.userId,
        partnerId = excluded.partnerId,
        partnerName = excluded.partnerName,
        productId = excluded.productId,
        productName = excluded.productName,
        amount = excluded.amount,
        currency = excluded.currency,
        commissionModel = excluded.commissionModel,
        commissionRate = excluded.commissionRate,
        status = excluded.status,
        commission = excluded.commission
    `);
    stmt.run(
      referralId,
      userId,
      partnerId,
      partnerName,
      productId,
      productName,
      Number(amount || 0),
      currency || "SAR",
      commissionModel || "cps",
      Number(commissionRate || 0.03),
      status,
      Number(commission || 0),
    );
    return referralDB.findById(referralId, userId);
  },

  findById: (referralId, userId) => {
    const stmt = db.prepare(
      "SELECT * FROM referrals WHERE referralId = ? AND userId = ?",
    );
    return normalizeReferralRow(stmt.get(referralId, userId));
  },

  markHandover: (referralId, userId) => {
    const stmt = db.prepare(`
      UPDATE referrals
      SET status = 'handed_over',
          handoverAt = CURRENT_TIMESTAMP
      WHERE referralId = ? AND userId = ?
    `);
    const result = stmt.run(referralId, userId);
    if (result.changes === 0) return null;
    return referralDB.findById(referralId, userId);
  },

  markConversion: (referralId, userId, commission) => {
    const stmt = db.prepare(`
      UPDATE referrals
      SET status = 'converted',
          commission = ?,
          conversionAt = CURRENT_TIMESTAMP
      WHERE referralId = ? AND userId = ?
    `);
    const result = stmt.run(Number(commission || 0), referralId, userId);
    if (result.changes === 0) return null;
    return referralDB.findById(referralId, userId);
  },

  summaryByUserId: (userId) => {
    const stmt = db.prepare(`
      SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN status = 'converted' THEN 1 END) as converted,
        COALESCE(SUM(commission), 0) as commission
      FROM referrals
      WHERE userId = ?
    `);
    const result = stmt.get(userId) || {};
    return {
      total: Number(result.total || 0),
      converted: Number(result.converted || 0),
      commission: Number(result.commission || 0),
    };
  },
};

function normalizeEventRow(row) {
  if (!row) return null;
  return {
    ...row,
    payload: parseJsonPayload(row.payload),
    eventAt: Number(row.eventAt || 0),
  };
}

export const eventDB = {
  create: ({ id, userId, name, payload, eventAt }) => {
    const stmt = db.prepare(`
      INSERT INTO events (id, userId, name, payload, eventAt)
      VALUES (?, ?, ?, ?, ?)
    `);
    stmt.run(
      id,
      userId,
      String(name || ""),
      JSON.stringify(parseJsonPayload(payload)),
      Number(eventAt || Date.now()),
    );
    return eventDB.findById(id, userId);
  },

  findById: (id, userId) => {
    const stmt = db.prepare("SELECT * FROM events WHERE id = ? AND userId = ?");
    return normalizeEventRow(stmt.get(id, userId));
  },

  countByUserId: (userId) => {
    const stmt = db.prepare("SELECT COUNT(*) as count FROM events WHERE userId = ?");
    return Number(stmt.get(userId)?.count || 0);
  },
};

function normalizeDeviceRow(device) {
  if (!device) return null;
  const resolvedLastSeenAt =
    Number(device.lastSeenAt) ||
    Number(device.lastSeen) ||
    Date.parse(String(device.lastSeenAt || device.lastSeen || "")) ||
    Date.now();
  return {
    ...device,
    biometricEnabled: Number(device.biometricEnabled) || 0,
    current: Number(device.current ?? device.isActive) || 0,
    lastSeenAt: resolvedLastSeenAt,
  };
}

// دوال الأجهزة
export const deviceDB = {
  upsert: ({
    id,
    userId,
    sessionToken = null,
    name = "",
    platform = "",
    browser = "",
    language = "",
    biometricEnabled = 0,
    current = 0,
    lastSeenAt = new Date().toISOString(),
  }) => {
    const columns = new Set(getTableColumns("devices"));
    const resolvedName =
      String(name || browser || platform || "Web Device").trim() || "Web Device";
    const resolvedLastSeenAt = lastSeenAt || new Date().toISOString();
    const payload = {
      id,
      userId,
      sessionToken,
      name: resolvedName,
      platform,
      browser,
      language,
      biometricEnabled: biometricEnabled ? 1 : 0,
      current: current ? 1 : 0,
      lastSeenAt: resolvedLastSeenAt,
      type: String(platform || "web"),
      identifier: String(id || resolvedName || "device"),
      isActive: current ? 1 : 0,
      lastSeen: resolvedLastSeenAt,
    };
    const insertableColumns = Object.entries(payload)
      .filter(([key, value]) => columns.has(key) && value !== undefined)
      .map(([key]) => key);
    const updateColumns = insertableColumns.filter((column) => column !== "id");
    const stmt = db.prepare(`
      INSERT INTO devices (
        ${insertableColumns.join(",\n        ")}
      ) VALUES (${insertableColumns.map(() => "?").join(", ")})
      ON CONFLICT(id) DO UPDATE SET
        ${updateColumns.map((column) => `${column} = excluded.${column}`).join(",\n        ")}
    `);
    stmt.run(...insertableColumns.map((column) => payload[column]));
    return normalizeDeviceRow(deviceDB.findById(id, userId));
  },

  findById: (id, userId) => {
    const stmt = db.prepare(
      "SELECT * FROM devices WHERE id = ? AND userId = ?",
    );
    return normalizeDeviceRow(stmt.get(id, userId));
  },

  findByUserId: (userId) => {
    const stmt = db.prepare(
      "SELECT * FROM devices WHERE userId = ? ORDER BY current DESC, lastSeenAt DESC",
    );
    return stmt.all(userId).map(normalizeDeviceRow);
  },

  findBySessionToken: (sessionToken) => {
    if (!sessionToken) return null;
    const stmt = db.prepare("SELECT * FROM devices WHERE sessionToken = ?");
    return normalizeDeviceRow(stmt.get(sessionToken));
  },

  clearCurrentForUser: (userId) => {
    const stmt = db.prepare(
      "UPDATE devices SET current = 0 WHERE userId = ? AND current = 1",
    );
    const result = stmt.run(userId);
    return result.changes > 0;
  },

  clearSessionToken: (sessionToken) => {
    if (!sessionToken) return false;
    const stmt = db.prepare(
      "UPDATE devices SET sessionToken = NULL WHERE sessionToken = ?",
    );
    const result = stmt.run(sessionToken);
    return result.changes > 0;
  },

  deleteById: (id, userId) => {
    const stmt = db.prepare("DELETE FROM devices WHERE id = ? AND userId = ?");
    const result = stmt.run(id, userId);
    return result.changes > 0;
  },
};

// دوال الهوية
export const identityDB = {
  get: (userId) => {
    const stmt = db.prepare("SELECT * FROM identities WHERE userId = ?");
    const result = stmt.get(userId);
    if (result && result.payload) {
      return JSON.parse(result.payload);
    }
    return null;
  },

  upsert: (userId, payload) => {
    const stmt = db.prepare(`
      INSERT OR REPLACE INTO identities (userId, payload, updatedAt)
      VALUES (?, ?, CURRENT_TIMESTAMP)
    `);
    stmt.run(userId, JSON.stringify(payload));
    return payload;
  },
};

// دوال تذ��كر خدمة العملاء
export const ticketDB = {
  create: ({ id, userId, type, category, partnerId, partnerName, referralId, status, priority, summary, payload }) => {
    const stmt = db.prepare(`
      INSERT INTO tickets (id, userId, type, category, partnerId, partnerName, referralId, status, priority, summary, payload)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      id,
      userId,
      String(type || 'other'),
      category || null,
      partnerId || null,
      partnerName || null,
      referralId || null,
      String(status || 'open'),
      String(priority || 'medium'),
      summary || null,
      JSON.stringify(parseJsonPayload(payload))
    );
    return ticketDB.findById(id, userId);
  },

  findById: (id, userId) => {
    const stmt = db.prepare("SELECT * FROM tickets WHERE id = ? AND userId = ?");
    return hydratePayloadRow(stmt.get(id, userId));
  },

  findByUserId: (userId) => {
    const stmt = db.prepare("SELECT * FROM tickets WHERE userId = ? ORDER BY createdAt DESC");
    return stmt.all(userId).map(hydratePayloadRow);
  },

  findOpenByUserId: (userId) => {
    const stmt = db.prepare(
      "SELECT * FROM tickets WHERE userId = ? AND status NOT IN ('resolved', 'closed') ORDER BY createdAt DESC"
    );
    return stmt.all(userId).map(hydratePayloadRow);
  },

  updateStatus: (id, userId, status, resolution) => {
    const stmt = db.prepare(`
      UPDATE tickets
      SET status = ?, resolution = COALESCE(?, resolution), updatedAt = CURRENT_TIMESTAMP
      WHERE id = ? AND userId = ?
    `);
    const result = stmt.run(status, resolution || null, id, userId);
    return result.changes > 0;
  },

  countByUserId: (userId) => {
    const stmt = db.prepare("SELECT COUNT(*) as count FROM tickets WHERE userId = ?");
    return Number(stmt.get(userId)?.count || 0);
  },
};

export const ticketMessageDB = {
  create: ({ id, ticketId, sender, content, metadata }) => {
    const stmt = db.prepare(`
      INSERT INTO ticket_messages (id, ticketId, sender, content, metadata)
      VALUES (?, ?, ?, ?, ?)
    `);
    stmt.run(
      id,
      ticketId,
      String(sender || 'customer'),
      String(content || ''),
      JSON.stringify(parseJsonPayload(metadata))
    );
    // تحديث وقت آخر تعديل للتذكرة
    db.prepare("UPDATE tickets SET updatedAt = CURRENT_TIMESTAMP WHERE id = ?").run(ticketId);
    return ticketMessageDB.findById(id);
  },

  findById: (id) => {
    const stmt = db.prepare("SELECT * FROM ticket_messages WHERE id = ?");
    const row = stmt.get(id);
    if (!row) return null;
    return { ...row, metadata: parseJsonPayload(row.metadata) };
  },

  findByTicketId: (ticketId) => {
    const stmt = db.prepare("SELECT * FROM ticket_messages WHERE ticketId = ? ORDER BY createdAt ASC");
    return stmt.all(ticketId).map(row => ({ ...row, metadata: parseJsonPayload(row.metadata) }));
  },
};

export default db;
