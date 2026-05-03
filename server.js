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
   MONGODB
═══════════════════════════════════════ */
mongoose.connect(process.env.MONGO_URI)
  .catch(err => console.error('MongoDB error:', err));

/* ── Schemas ── */
const itemSchema = new mongoose.Schema({
  slot:     String,
  name:     String,
  qty:      Number,
  price:    Number,
  subtotal: Number,
});

const transactionSchema = new mongoose.Schema({
  transactionId:      { type: String, unique: true },
  razorpayOrderId:    String,
  razorpaySignature:  String,
  timestamp:          { type: Date, default: Date.now },
  paymentMethod:      { type: String, default: 'ONLINE' },
  items:              [itemSchema],
  totalAmount:        Number,
  currency:           { type: String, default: 'INR' },
  changeDue:          { type: Number, default: 0 },
  status:             { type: String, enum: ['SUCCESS','FAILED'], default: 'SUCCESS' },
});

const stockSchema = new mongoose.Schema({
  slot:     { type: String, unique: true },
  name:     String,
  price:    Number,
  maxStock: Number,
  stock:    Number,
  updatedAt:{ type: Date, default: Date.now },
});

const Transaction = mongoose.model('Transaction', transactionSchema);
const Stock       = mongoose.model('Stock', stockSchema);

/* ── Seed initial stock if empty ── */
async function seedStock() {
  const count = await Stock.countDocuments();
  if (count === 0) {
    await Stock.insertMany([
      { slot:'A1', name:'Khatta Meetha',  price:1, maxStock:4, stock:4 },
      { slot:'A2', name:'Tikha Meetha',   price:1, maxStock:4, stock:4 },
      { slot:'B1', name:'Farali Chiwda',  price:1, maxStock:4, stock:4 },
      { slot:'B2', name:'Chataka Pataka', price:1, maxStock:4, stock:4 },
    ]);
    console.log('Stock seeded');
  }
}
mongoose.connection.once('open', seedStock);

/* ═══════════════════════════════════════
   RAZORPAY
═══════════════════════════════════════ */
const razorpay = new Razorpay({
  key_id:     process.env.RAZORPAY_KEY_ID,
  key_secret: process.env.RAZORPAY_KEY_SECRET,
});

/* ═══════════════════════════════════════
   MQTT
═══════════════════════════════════════ */
const mqttClient = mqtt.connect(process.env.MQTT_BROKER, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  clientId: 'backend_' + Math.random().toString(36).slice(2,8),
  clean: true,
  reconnectPeriod: 5000,
});
mqttClient.on('connect', () => console.log('MQTT connected'));
mqttClient.on('error',   e => console.error('MQTT error:', e.message));

/* ═══════════════════════════════════════
   ROUTES
═══════════════════════════════════════ */

/* ── CREATE RAZORPAY ORDER ── */
app.post('/api/create-order', async (req, res) => {
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
    console.error('Razorpay error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── VERIFY PAYMENT + SAVE TXN + DECREMENT STOCK ── */
app.post('/api/verify-payment', async (req, res) => {
  try {
    const {
      razorpay_order_id,
      razorpay_payment_id,
      razorpay_signature,
      items,        // FIX: now received from frontend
      totalAmount,  // FIX: now received from frontend
    } = req.body;

    /* signature check */
    const body     = razorpay_order_id + '|' + razorpay_payment_id;
    const expected = crypto
      .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
      .update(body)
      .digest('hex');

    if (expected !== razorpay_signature)
      return res.status(400).json({ success: false, error: 'Invalid signature' });

    // FIX: Guard against duplicate transaction IDs gracefully
    const existing = await Transaction.findOne({ transactionId: razorpay_payment_id });
    if (existing) {
      return res.json({ success: true, transactionId: razorpay_payment_id, duplicate: true });
    }

    /* save transaction FIRST, then decrement stock, then publish MQTT */
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

    /* FIX: Decrement stock with floor guard — stock cannot go below 0 */
    if (items && items.length) {
      for (const item of items) {
        await Stock.findOneAndUpdate(
          { slot: item.slot, stock: { $gte: item.qty } }, // only decrement if enough stock
          {
            $inc: { stock: -item.qty },
            $set: { updatedAt: new Date() },
          }
        );
      }
    }

    /* FIX: Publish to MQTT only after successful DB save */
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
    mqttClient.publish('vendbot/order',   JSON.stringify(payload), { qos: 1 });
    mqttClient.publish('vendbot/payment', JSON.stringify(payload), { qos: 1 });

    res.json({ success: true, transactionId: razorpay_payment_id });
  } catch (err) {
    console.error('Verify error:', err);
    res.status(500).json({ error: err.message });
  }
});

/* ── GET ALL TRANSACTIONS ── */
app.get('/api/transactions', async (req, res) => {
  try {
    const limit  = parseInt(req.query.limit)  || 100;
    const offset = parseInt(req.query.offset) || 0;
    const status = req.query.status;

    const filter = status ? { status } : {};
    const [txns, total] = await Promise.all([
      Transaction.find(filter).sort({ timestamp: -1 }).skip(offset).limit(limit).lean(),
      Transaction.countDocuments(filter),
    ]);
    res.json({ transactions: txns, total });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── GET STOCK ── */
app.get('/api/stock', async (req, res) => {
  try {
    const stock = await Stock.find().lean();
    res.json({ stock });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── RESTOCK A SLOT ── */
app.post('/api/stock/restock', async (req, res) => {
  try {
    const { slot } = req.body;
    if (!slot) return res.status(400).json({ error: 'slot required' });

    const item = await Stock.findOne({ slot });
    if (!item) return res.status(404).json({ error: 'Slot not found' });

    item.stock     = item.maxStock;
    item.updatedAt = new Date();
    await item.save();

    res.json({ success: true, slot, stock: item.stock });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ── DASHBOARD SUMMARY ── */
app.get('/api/summary', async (req, res) => {
  try {
    const [txns, stock] = await Promise.all([
      Transaction.find().lean(),
      Stock.find().lean(),
    ]);

    const success = txns.filter(t => t.status === 'SUCCESS');
    const failed  = txns.filter(t => t.status === 'FAILED');

    const now     = new Date();
    const revByDay = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      const dayStr = d.toDateString();
      const dayTxns = success.filter(t => new Date(t.timestamp).toDateString() === dayStr);
      revByDay.push({
        label: d.toLocaleDateString('en-GB', { weekday: 'short' }),
        revenue: dayTxns.reduce((s, t) => s + t.totalAmount, 0),
      });
    }

    const soldBySlot = {};
    success.forEach(t => {
      (t.items || []).forEach(i => {
        soldBySlot[i.slot] = (soldBySlot[i.slot] || 0) + i.qty;
      });
    });

    res.json({
      totalRevenue:   success.reduce((s, t) => s + t.totalAmount, 0),
      totalTxns:      txns.length,
      successTxns:    success.length,
      failedTxns:     failed.length,
      totalUnitsSold: success.reduce((s, t) => s + (t.items||[]).reduce((q, i) => q + i.qty, 0), 0),
      todayRevenue:   success
        .filter(t => new Date(t.timestamp).toDateString() === now.toDateString())
        .reduce((s, t) => s + t.totalAmount, 0),
      revByDay,
      soldBySlot,
      stock,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ═══════════════════════════════════════
   START
═══════════════════════════════════════ */
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
