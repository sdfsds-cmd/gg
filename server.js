const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// Render باید روی PORT خودش اجرا شود
const PORT = process.env.PORT || 10000;

// Binance USD-M Futures
const BINANCE_REST =
  "https://fapi.binance.com/fapi/v1/aggTrades";

const BINANCE_WS =
  "wss://fstream.binance.com/ws";

// ======================================================
// HTTP
// ======================================================

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "Binance USD-M Futures 1s Relay",
    endpoints: {
      health: "/health",
      history: "/history?symbol=ETHUSDT&hours=8",
      websocket: "/live?symbol=ETHUSDT"
    }
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    time: Date.now()
  });
});

// ======================================================
// Helpers
// ======================================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function createEmptyBar(time, price) {
  return {
    time,
    open: price,
    high: price,
    low: price,
    close: price
  };
}

function addTradeToBar(map, trade) {
  const timestamp = Number(trade.T);

  // 1-second bucket
  const bucket =
    Math.floor(timestamp / 1000) * 1000;

  const price = Number(trade.p);

  let bar = map.get(bucket);

  if (!bar) {
    bar = createEmptyBar(bucket, price);
    map.set(bucket, bar);
    return;
  }

  if (price > bar.high)
    bar.high = price;

  if (price < bar.low)
    bar.low = price;

  bar.close = price;
}

// ======================================================
// HISTORY
//
// Binance aggTrades gives us recent market trades.
// We reconstruct true 1-second OHLC candles from them.
// ======================================================

async function fetchHistory(symbol, hours) {

  const endTime = Date.now();

  // Binance aggTrades history is limited to recent history.
  const safeHours =
    Math.min(Math.max(hours, 0.1), 48);

  const startTime =
    endTime - safeHours * 60 * 60 * 1000;

  const bars = new Map();

  let nextId = null;

  let requests = 0;

  // Safety limit
  const MAX_REQUESTS = 320;

  while (requests < MAX_REQUESTS) {

    requests++;

    let url;

    if (nextId === null) {

      url =
        `${BINANCE_REST}` +
        `?symbol=${encodeURIComponent(symbol)}` +
        `&startTime=${startTime}` +
        `&endTime=${endTime}` +
        `&limit=1000`;

    } else {

      url =
        `${BINANCE_REST}` +
        `?symbol=${encodeURIComponent(symbol)}` +
        `&fromId=${nextId}` +
        `&limit=1000`;
    }

    const response = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 Binance-Relay/1.0",
        "Accept":
          "application/json"
      }
    });

    const body = await response.text();

    if (!response.ok) {
      throw new Error(
        `Binance REST ${response.status}: ${body.slice(0, 500)}`
      );
    }

    const trades = JSON.parse(body);

    if (!Array.isArray(trades) ||
        trades.length === 0) {
      break;
    }

    let lastId = null;
    let reachedEnd = false;

    for (const trade of trades) {

      const t = Number(trade.T);

      if (t < startTime)
        continue;

      if (t > endTime) {
        reachedEnd = true;
        break;
      }

      addTradeToBar(bars, trade);

      lastId = Number(trade.a);
    }

    if (
      reachedEnd ||
      trades.length < 1000 ||
      lastId === null
    ) {
      break;
    }

    nextId = lastId + 1;

    // کمی فاصله برای جلوگیری از فشار روی rate limit
    if (requests % 8 === 0) {
      await sleep(250);
    }
  }

  return Array
    .from(bars.values())
    .sort((a, b) => a.time - b.time);
}

// ======================================================
// HISTORY ENDPOINT
// ======================================================

app.get("/history", async (req, res) => {

  const symbol =
    String(
      req.query.symbol ||
      req.query.pair ||
      "ETHUSDT"
    ).toUpperCase();

  const hours =
    Number(req.query.hours || 8);

  if (!/^[A-Z0-9_]{5,30}$/.test(symbol)) {
    return res.status(400).json({
      error: "Invalid symbol"
    });
  }

  try {

    console.log(
      `History request: ${symbol}, ${hours}h`
    );

    const bars =
      await fetchHistory(symbol, hours);

    /*
      خروجی را به همان فرمتی می‌دهیم که
      backtest-live.html فعلی تو می‌فهمد:

      [
        openTime,
        open,
        high,
        low,
        close,
        volume,
        closeTime
      ]
    */

    const result = bars.map(bar => {

      const openTime = bar.time;

      return [
        openTime,
        String(bar.open),
        String(bar.high),
        String(bar.low),
        String(bar.close),
        "0",
        openTime + 999,
        "0",
        0,
        "0",
        "0",
        "0"
      ];
    });

    res.set(
      "Cache-Control",
      "no-store"
    );

    res.json(result);

  } catch (error) {

    console.error(
      "History error:",
      error
    );

    res.status(502).json({
      error:
        "Binance history request failed",
      detail:
        String(error.message || error)
    });
  }
});

// ======================================================
// WEBSOCKET
// ======================================================

const wss =
  new WebSocket.Server({
    server,
    path: "/live"
  });

wss.on("connection", (client, request) => {

  const url =
    new URL(
      request.url,
      "http://localhost"
    );

  const symbol =
    String(
      url.searchParams.get("symbol") ||
      "ETHUSDT"
    ).toUpperCase();

  if (!/^[A-Z0-9_]{5,30}$/.test(symbol)) {

    client.close(
      1008,
      "Invalid symbol"
    );

    return;
  }

  console.log(
    `Client connected: ${symbol}`
  );

  const binanceURL =
    `${BINANCE_WS}/${symbol.toLowerCase()}@aggTrade`;

  let upstream = null;

  let reconnectTimer = null;

  let stopped = false;

  let currentBar = null;

  // ----------------------------------------------------
  // Send candle in the format expected by the HTML
  // ----------------------------------------------------

  function sendCurrentBar() {

    if (
      !currentBar ||
      client.readyState !== WebSocket.OPEN
    ) {
      return;
    }

    const message = {

      e: "continuous_kline",

      E: Date.now(),

      s: symbol,

      k: {

        t: currentBar.time,

        T: currentBar.time + 999,

        s: symbol,

        i: "1s",

        o: String(currentBar.open),

        h: String(currentBar.high),

        l: String(currentBar.low),

        c: String(currentBar.close),

        x: false
      }
    };

    client.send(
      JSON.stringify(message)
    );
  }

  // ----------------------------------------------------
  // Connect to Binance
  // ----------------------------------------------------

  function connectBinance() {

    if (
      stopped ||
      client.readyState !== WebSocket.OPEN
    ) {
      return;
    }

    console.log(
      `Connecting Binance: ${binanceURL}`
    );

    upstream =
      new WebSocket(binanceURL);

    upstream.on("open", () => {

      console.log(
        `Binance connected: ${symbol}`
      );

    });

    upstream.on("message", data => {

      try {

        const msg =
          JSON.parse(
            data.toString()
          );

        if (
          msg.e !== "aggTrade"
        ) {
          return;
        }

        const timestamp =
          Number(msg.T);

        const price =
          Number(msg.p);

        const bucket =
          Math.floor(
            timestamp / 1000
          ) * 1000;

        // New 1-second candle
        if (
          !currentBar ||
          currentBar.time !== bucket
        ) {

          // Send previous candle
          if (currentBar) {
            sendCurrentBar();
          }

          currentBar = {
            time: bucket,
            open: price,
            high: price,
            low: price,
            close: price
          };

        } else {

          // Update current candle

          if (
            price > currentBar.high
          ) {
            currentBar.high = price;
          }

          if (
            price < currentBar.low
          ) {
            currentBar.low = price;
          }

          currentBar.close =
            price;
        }

        /*
          مهم:
          هر معامله باعث update همان
          کندل 1 ثانیه‌ای می‌شود.
        */

        sendCurrentBar();

      } catch (error) {

        console.error(
          "Message parse error:",
          error.message
        );
      }
    });

    upstream.on("error", error => {

      console.error(
        "Binance WebSocket error:",
        error.message
      );
    });

    upstream.on("close", () => {

      if (stopped)
        return;

      console.log(
        `Binance disconnected: ${symbol}`
      );

      clearTimeout(
        reconnectTimer
      );

      reconnectTimer =
        setTimeout(
          connectBinance,
          1500
        );
    });
  }

  // Start Binance connection
  connectBinance();

  // ----------------------------------------------------
  // Keepalive
  // ----------------------------------------------------

  const pingTimer =
    setInterval(() => {

      if (
        client.readyState ===
        WebSocket.OPEN
      ) {
        client.ping();
      }

    }, 20000);

  // ----------------------------------------------------
  // Client disconnect
  // ----------------------------------------------------

  client.on("close", () => {

    console.log(
      `Client disconnected: ${symbol}`
    );

    stopped = true;

    clearInterval(
      pingTimer
    );

    clearTimeout(
      reconnectTimer
    );

    if (upstream) {

      try {
        upstream.close();
      } catch {}

      upstream = null;
    }
  });

  client.on("error", () => {

    stopped = true;

    clearInterval(
      pingTimer
    );

    clearTimeout(
      reconnectTimer
    );

    if (upstream) {

      try {
        upstream.close();
      } catch {}

      upstream = null;
    }
  });
});

// ======================================================
// START
// ======================================================

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `Relay running on port ${PORT}`
    );
  }
);