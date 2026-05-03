const express  = require('express');
const Razorpay = require('razorpay');
const crypto   = require('crypto');
const cors     = require('cors');
const mqtt     = require('mqtt');
const mongoose = require('mongoose');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(cors());

/* ═══════════════════════════════════════
   MONGODB — robust connect with retry
═══════════════════════════════════════ */
let dbReady = false;

async function connectDB() {
  const uri = process.env.MONGO_URI;
  if (!uri) {
    console.error('FATAL: MONGO_URI env variable is not set. Check your Render environment variables.');
    process.exit(1);
  }
  try {
    await mongoose.connect(uri, {
      serverSelectionTimeoutMS: 10000,
      socketTimeoutMS: 45000,
    });
    dbReady = true;
    console.log('MongoDB connected ✓');
    await seedStock();
  } catch (err) {
    console.error('MongoDB connect failed:', err.message);
    console.log('Retrying in 5s…');
    setTimeout(connectDB, 5000);
  }
}

mongoose.connection.on('disconnected', () => {
  dbReady = false;
  console.warn('MongoDB disconnected');
});
mongoose.connection.on('reconnected', () => {
  dbReady = true;
  console.log('MongoDB reconnected ✓');
});

// Middleware: returns 503 instead of crashing if DB not ready
function requireDB(req, res, next) {
  if (!dbReady) {
    return res.status(503).json({
      error: 'Database not ready yet. Please retry in a few seconds.',
      hint:  'Check /health for DB status and env var presence.',
    });
  }
  next();
}

/* ── Schemas ── */
const itemSchema = new mongoose.Schema({
  slot:     String,
  name:     String,
  qty:      Number,
  price:    Number,
  subtotal: Number,
});

const transactionSchema = new mongoose.Schema({
  transactionId:     { type: String, unique: true },
  razorpayOrderId:   String,
  razorpaySignature: String,
  timestamp:         { type: Date, default: Date.now },
  paymentMethod:     { type: String, default: 'ONLINE' },
  items:             [itemSchema],
  totalAmount:       Number,
  currency:          { type: String, default: 'INR' },
  changeDue:         { type: Number, default: 0 },
  status:            { type: String, enum: ['SUCCESS','FAILED'], default: 'SUCCESS' },
});

const stockSchema = new mongoose.Schema({
  slot:      { type: String, unique: true },
  name:      String,
  price:     Number,
  maxStock:  Number,
  stock:     Number,
  updatedAt: { type: Date, default: Date.now },
});

const Transaction = mongoose.model('Transaction', transactionSchema);
const Stock       = mongoose.model('Stock', stockSchema);

/* ── Seed initial stock if empty ── */
async function seedStock() {
  try {
    const count = await Stock.countDocuments();
    if (count === 0) {
      await Stock.insertMany([
        { slot:'A1', name:'Khatta Meetha',  price:1, maxStock:4, stock:4 },
        { slot:'A2', name:'Tikha Meetha',   price:1, maxStock:4, stock:4 },
        { slot:'B1', name:'Farali Chiwda',  price:1, maxStock:4, stock:4 },
        { slot:'B2', name:'Chataka Pataka', price:1, maxStock:4, stock:4 },
      ]);
      console.log('Stock seeded ✓');
    } else {
      console.log('Stock already exists (' + count + ' slots) — skipping seed');
    }
  } catch (err) {
    console.error('Seed error:', err.message);
  }
}

/* ═══════════════════════════════════════
   RAZORPAY
═══════════════════════════════════════ */
if (!process.env.RAZORPAY_KEY_ID || !process.env.RAZORPAY_KEY_SECRET) {
  console.warn('WARNING: RAZORPAY_KEY_ID or RAZORPAY_KEY_SECRET not set');
}
const razorpay = new Razorpay({
  key_id:     process.env.RAZORPAY_KEY_ID     || '',
  key_secret: process.env.RAZORPAY_KEY_SECRET || '',
});

/* ═══════════════════════════════════════
   MQTT
═══════════════════════════════════════ */
let mqttClient = null;
if (process.env.MQTT_BROKER) {
  mqttClient = mqtt.connect(process.env.MQTT_BROKER, {
    username:        process.env.MQTT_USERNAME,
    password:        process.env.MQTT_PASSWORD,
    clientId:        'backend_' + Math.random().toString(36).slice(2,8),
    clean:           true,
    reconnectPeriod: 5000,
  });
  mqttClient.on('connect', () => console.log('MQTT connected ✓'));
  mqttClient.on('error',   e  => console.error('MQTT error:', e.message));
} else {
  console.warn('WARNING: MQTT_BROKER not set — MQTT disabled');
}

function mqttPublish(topic, payload) {
  if (!mqttClient || !mqttClient.connected) {
    console.warn('MQTT not connected — skipping publish to', topic);
    return;
  }
  mqttClient.publish(topic, JSON.stringify(payload), { qos: 1 }, err => {
    if (err) console.error('MQTT publish error:', err.message);
  });
}

/* ═══════════════════════════════════════
   HEALTH — visit /health to debug 500s
═══════════════════════════════════════ */
app.get('/health', (req, res) => {
  const states = ['disconnected','connected','connecting','disconnecting'];
  res.json({
    status:      'ok',
    dbReady,
    dbState:     mongoose.connection.readyState,
    dbStateText: states[mongoose.connection.readyState] || 'unknown',
    mqttConnected: mqttClient ? mqttClient.connected : false,
    envPresent: {
      MONGO_URI:           !!process.env.MONGO_URI,
      RAZORPAY_KEY_ID:     !!process.env.RAZORPAY_KEY_ID,
      RAZORPAY_KEY_SECRET: !!process.env.RAZORPAY_KEY_SECRET,
      MQTT_BROKER:         !!process.env.MQTT_BROKER,
      MQTT_USERNAME:       !!process.env.MQTT_USERNAME,
      MQTT_PASSWORD:       !!process.env.MQTT_PASSWORD,
    },
    uptime: Math.round(process.uptime()) + 's',
  });
});

/* ═══════════════════════════════════════
   ROUTES
═══════════════════════════════════════ */

/* ── CREATE RAZORPAY ORDER ── */
app.post('/api/create-order', requireDB, async (req, res) => {
  try {
    const { amount } = req.body;
    if (!amount || amount < 1)
      return res.status(400).json({ error: 'Invalid amount' });

    const order = await razorpay.orders.create({
      amount:   amount * 100,
      currency: 'INR',
      receipt:  'rcpt_' + Date.now(),
    });

    res.json({
      order_id: order.id,
      amount:   order.amount,
      currency: order.currency,
      key:      process.env.RAZORPAY_KEY_ID,
    });
  } catch (err) {
    console.error('create-order error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── VERIFY PAYMENT + SAVE TXN + DECREMENT STOCK ── */
app.post('/api/verify-payment', requireDB, async (req, res) => {
  try {
    const {
      razorpay_order_id,
      razorpay_payment_id,
      razorpay_signature,
      items,
      totalAmount,
    } = req.body;

    if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature)
      return res.status(400).json({ error: 'Missing required Razorpay fields' });

    /* Signature check */
    const sigBody  = razorpay_order_id + '|' + razorpay_payment_id;
    const expected = crypto
      .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
      .update(sigBody)
      .digest('hex');

    if (expected !== razorpay_signature)
      return res.status(400).json({ success: false, error: 'Invalid signature' });

    /* Duplicate guard */
    const existing = await Transaction.findOne({ transactionId: razorpay_payment_id });
    if (existing)
      return res.json({ success: true, transactionId: razorpay_payment_id, duplicate: true });

    /* Save transaction */
    const txn = await Transaction.create({
      transactionId:     razorpay_payment_id,
      razorpayOrderId:   razorpay_order_id,
      razorpaySignature: razorpay_signature,
      timestamp:         new Date(),
      paymentMethod:     'ONLINE',
      items:             items || [],
      totalAmount:       totalAmount || 0,
      status:            'SUCCESS',
    });

    /* Decrement stock (guarded — stock cannot go below 0) */
    if (items && items.length) {
      for (const item of items) {
        const updated = await Stock.findOneAndUpdate(
          { slot: item.slot, stock: { $gte: item.qty } },
          { $inc: { stock: -item.qty }, $set: { updatedAt: new Date() } },
          { new: true }
        );
        if (!updated) {
          console.warn(`Slot ${item.slot}: stock update skipped (insufficient stock or slot not found)`);
        }
      }
    }

    /* MQTT publish after all DB work done */
    const payload = {
      transactionId:   razorpay_payment_id,
      razorpayOrderId: razorpay_order_id,
      timestamp:       txn.timestamp,
      paymentMethod:   'ONLINE',
      items:           items || [],
      totalAmount:     totalAmount || 0,
      currency:        'INR',
      status:          'SUCCESS',
    };
    mqttPublish('vendbot/order',   payload);
    mqttPublish('vendbot/payment', payload);

    res.json({ success: true, transactionId: razorpay_payment_id });
  } catch (err) {
    console.error('verify-payment error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── GET TRANSACTIONS ── */
app.get('/api/transactions', requireDB, async (req, res) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit)  || 20, 100);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);
    const filter = req.query.status ? { status: req.query.status } : {};

    const [txns, total] = await Promise.all([
      Transaction.find(filter).sort({ timestamp: -1 }).skip(offset).limit(limit).lean(),
      Transaction.countDocuments(filter),
    ]);
    res.json({ transactions: txns, total });
  } catch (err) {
    console.error('transactions error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── GET STOCK ── */
app.get('/api/stock', requireDB, async (req, res) => {
  try {
    const stock = await Stock.find().sort({ slot: 1 }).lean();
    res.json({ stock });
  } catch (err) {
    console.error('stock error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── RESTOCK A SLOT ── */
app.post('/api/stock/restock', requireDB, async (req, res) => {
  try {
    const { slot } = req.body;
    if (!slot) return res.status(400).json({ error: 'slot required' });

    const doc = await Stock.findOne({ slot });
    if (!doc) return res.status(404).json({ error: 'Slot not found' });

    doc.stock     = doc.maxStock;
    doc.updatedAt = new Date();
    await doc.save();

    res.json({ success: true, slot, stock: doc.stock });
  } catch (err) {
    console.error('restock error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── DASHBOARD SUMMARY ── */
app.get('/api/summary', requireDB, async (req, res) => {
  try {
    const [txns, stock] = await Promise.all([
      Transaction.find().lean(),
      Stock.find().sort({ slot: 1 }).lean(),
    ]);

    const success = txns.filter(t => t.status === 'SUCCESS');
    const failed  = txns.filter(t => t.status === 'FAILED');

    const now      = new Date();
    const revByDay = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      const dayStr  = d.toDateString();
      const dayTxns = success.filter(t => new Date(t.timestamp).toDateString() === dayStr);
      revByDay.push({
        label:   d.toLocaleDateString('en-GB', { weekday: 'short' }),
        revenue: dayTxns.reduce((s, t) => s + (t.totalAmount || 0), 0),
      });
    }

    const soldBySlot = {};
    success.forEach(t => {
      (t.items || []).forEach(i => {
        soldBySlot[i.slot] = (soldBySlot[i.slot] || 0) + (i.qty || 0);
      });
    });

    res.json({
      totalRevenue:   success.reduce((s, t) => s + (t.totalAmount || 0), 0),
      totalTxns:      txns.length,
      successTxns:    success.length,
      failedTxns:     failed.length,
      totalUnitsSold: success.reduce((s, t) => s + (t.items||[]).reduce((q, i) => q + (i.qty||0), 0), 0),
      todayRevenue:   success
        .filter(t => new Date(t.timestamp).toDateString() === now.toDateString())
        .reduce((s, t) => s + (t.totalAmount || 0), 0),
      revByDay,
      soldBySlot,
      stock,
    });
  } catch (err) {
    console.error('summary error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════════════════
   START — connect DB after server is up
   so /health is always reachable
═══════════════════════════════════════ */
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log('Server running on port ' + PORT);
  connectDB();
});
