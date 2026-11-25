// SteadyScalp — a conservative paper-trading bot for the Pump.fun Sharpe Sprint.
//
// Ranking is by hourly Sharpe ratio, not by wildest PnL, so this bot is built to
// grind many small, diversified round trips instead of chasing one moonshot:
//   - fixed small stake (0.05 SOL) per mint, at most 5 open positions;
//   - snipe each fresh curve in its first 3 slots, only when the launch
//     looks clean (small creator buy, few early buyers, creator not sold);
//   - exit fast: take-profit +15%, stop-loss -12%, creator-sold exit, or a
//     hard 10-slot time exit;
//   - one entry per mint, ever: no chasing a coin that already ran;
//   - a drawdown brake: no new entries once equity is 15% below its peak.
// Deterministic by construction: every decision comes only from the hello / batch
// stream (events, marks, fills, account) and from counters derived from it.
// No clock, no randomness, no environment reads; iteration is over
// insertion-ordered maps fed by the batch stream, so two runs on the same data
// produce byte-identical orders.
//
// Protocol: agent-fun/replay/v2. The harness sends hello (capital, caps, fees,
// latency_slots), then one batch per slot step (seq, slot, events, marks,
// fills, account), then done. The bot answers each batch with one reply
// echoing its seq and carrying its orders. Orders decided on the batch for
// slot s fill at slot s + latency_slots (2 slots here).

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput } from './vendor/makefun-bot.ts';

// ---------------------------------------------------------------- parameters

const STAKE_LAMPORTS = 50_000_000n; // 0.05 SOL per position: the dev window's
// per-trade gross edge is slightly negative, so a bigger stake only scales
// the loss (0.10 SOL graded -0.701 vs -0.541 at 0.05); the size cap (1% of
// real reserve) already keeps most buys small anyway.
const MIN_BUY_LAMPORTS = 10_000_000n; // guardrail round trip floor: 0.01 SOL.
const BUY_SLIPPAGE_BPS = 10_000; // wide open: on a fresh coin the price moves
// in the 2-slot latency either way; rejecting the fill would only skip the
// entry, not improve its price.
const SELL_SLIPPAGE_BPS = 10_000; // exits must go out: accept any price.

// Entry (the snipe, v3): a curve is bought only in its first 3 slots of life,
// once it holds 0.3-30 SOL of real reserve, and only when the launch looks
// clean: the creator's own buy in the create slot is small (<= 2.5 SOL), at
// most 4 distinct non-creator buyers have piled in yet (a launch bundled by
// many wallets is a dump in waiting), and the creator has not sold. Dev
// grading showed this regime loses the least of every approach tried
// (momentum -9.44 SOL, reversion -3.99, late snipe -1.47, this regime ~-0.5):
// the 2-slot fill latency eats the early pump, so the only defence is to
// enter at the very start and leave within a few slots.
const MAX_AGE_SLOTS = 3; // slots since CreateEvent; older curves are skipped.
const MIN_REAL_SOL_LAMPORTS = 300_000_000n; // >= 0.3 SOL real reserve.
const MAX_REAL_SOL_LAMPORTS = 30_000_000_000n; // ... and <= 30 SOL.
const MAX_DEV_BUY_LAMPORTS = 2_500_000_000n; // creator's create-slot buy cap.
const MAX_BUYERS_AT_ENTRY = 4; // distinct non-creator buyers seen, at most.
const MAX_ENTRIES_PER_MINT = 1; // one entry per mint, ever: no chasing.

// Pace insurance: if the run is somehow starved of fresh curves, after 600
// batches with fewer than 10 round trips, also take curves up to 10 slots
// old so the 20-mint guardrail still clears.
const PACE_BATCH = 600;
const PACE_MIN_ROUND_TRIPS = 10;
const PACE_MAX_AGE_SLOTS = 10;

// Exits. Round-trip cost is ~2.5% curve fees + 2 tx fees, so exits are fast:
// take the small win, cut the small loss, never linger past 10 slots.
const TAKE_PROFIT_BPS = 1_500; // +15% over cost per token.
const STOP_LOSS_BPS = 1_200; // -12% under cost per token.
const MAX_HOLD_SLOTS = 10; // hard time exit: 10 slots after the buy filled.

// Drawdown brake: stop opening new positions once equity is 15% below peak.
const MAX_DRAWDOWN_BPS = 1_500;

// Price is kept as a scaled integer: quote lamports per token * SCALE.
const SCALE = 1_000_000n;
const MAX_TAPE = 120; // trades remembered per curve (must exceed MAX_HOLD_TRADES).

// ---------------------------------------------------------------- per-curve tape

interface TapeTrade {
  slot: number;
  isBuy: boolean;
  solLamports: bigint;
  user: string;
  price: bigint; // scaled price just after this trade.
}

interface CurveTape {
  mint: string;
  createdSlot: number | null;
  createdBatch: number;
  creator: string | null; // from the CreateEvent; null when unseen.
  complete: boolean;
  trades: TapeTrade[];
}

interface OpenPosition {
  mint: string;
  buyLamports: bigint;
  costPerToken: bigint; // scaled: lamports per token, fees included.
  entrySlot: number;
  entryBatch: number;
  peakPrice: bigint; // scaled peak mark price since entry.
}

function scaledPrice(virtualQuote: bigint, virtualToken: bigint): bigint {
  if (virtualToken <= 0n) return 0n;
  return (virtualQuote * SCALE) / virtualToken;
}

// ---------------------------------------------------------------- bot state

const tapes = new Map<string, CurveTape>();
const positions = new Map<string, OpenPosition>();
const lastExitSlot = new Map<string, number>();
const entriesPerMint = new Map<string, number>();
const roundTripMints = new Set<string>();

let batchIndex = 0;
let peakEquityLamports = 10_000_000_000n; // starts at hello capital.
let buysSent = 0;
let sellsSent = 0;
let roundTrips = 0;

function tapeFor(mint: string, batch: number): CurveTape {
  let tape = tapes.get(mint);
  if (tape === undefined) {
    tape = { mint, createdSlot: null, createdBatch: batch, creator: null, complete: false, trades: [] };
    tapes.set(mint, tape);
  }
  return tape;
}

/** True when `now` is at least `pctBps` above `base` (both scaled prices). */
function upAtLeast(now: bigint, base: bigint, pctBps: number): boolean {
  return now * 10_000n >= base * BigInt(10_000 + pctBps);
}

/** True when `now` is at least `pctBps` below `base`. */
function downAtLeast(now: bigint, base: bigint, pctBps: number): boolean {
  return now * 10_000n <= base * BigInt(10_000 - pctBps);
}

// ---------------------------------------------------------------- the bot

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    batchIndex += 1;
    const orders: OrderInput[] = [];
    const { caps } = state.hello;
    const txFee = state.txFeeLamports();

    // --- 1. Fold this batch's events into the tapes. -----------------------
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        const tape = tapeFor(e.mint, batchIndex);
        tape.createdSlot = e.slot;
        tape.creator = e.creator;
        tape.complete = false;
      } else if (e.event === 'TradeEvent') {
        const tape = tapeFor(e.mint, batchIndex);
        if (tape.createdSlot === null) tape.createdSlot = e.slot;
        tape.trades.push({
          slot: e.slot,
          isBuy: e.is_buy,
          solLamports: BigInt(e.sol_amount),
          user: e.user,
          price: scaledPrice(BigInt(e.virtual_quote_reserves), BigInt(e.virtual_token_reserves)),
        });
        if (tape.trades.length > MAX_TAPE) tape.trades.splice(0, tape.trades.length - MAX_TAPE);
      } else {
        // CompleteEvent: the curve migrated; the harness closes positions in it.
        const tape = tapeFor(e.mint, batchIndex);
        tape.complete = true;
      }
    }

    // --- 2. Fold this batch's fills into the position book. ----------------
    for (const f of batch.fills) {
      if (f.status !== 'filled') continue;
      if (f.side === 'buy') {
        const tokens = BigInt(f.token_amount);
        if (tokens > 0n) {
          const costPerToken = (BigInt(f.quote_lamports) * SCALE) / tokens;
          const held = state.positions.get(f.mint);
          const liveCost =
            held !== undefined && BigInt(held.token_amount) > 0n
              ? (BigInt(held.cost_lamports) * SCALE) / BigInt(held.token_amount)
              : costPerToken;
          const curve = state.curves.get(f.mint);
          const mark =
            curve !== undefined
              ? scaledPrice(curve.virtual_quote_reserves, curve.virtual_token_reserves)
              : costPerToken;
          positions.set(f.mint, {
            mint: f.mint,
            buyLamports: BigInt(f.quote_lamports),
            costPerToken: liveCost,
            entrySlot: batch.slot,
            entryBatch: batchIndex,
            peakPrice: mark,
          });
          entriesPerMint.set(f.mint, (entriesPerMint.get(f.mint) ?? 0) + 1);
        }
      } else {
        // A sell filled: the round trip on this mint is complete.
        const pos = positions.get(f.mint);
        if (pos !== undefined && pos.buyLamports >= MIN_BUY_LAMPORTS) {
          if (!roundTripMints.has(f.mint)) {
            roundTripMints.add(f.mint);
            roundTrips += 1;
          }
        }
        positions.delete(f.mint);
        lastExitSlot.set(f.mint, batch.slot);
      }
    }
    // Reconcile: a position the account holds that the book missed (defensive;
    // fills normally arrive first). Its cost basis comes from the account.
    for (const [mint, held] of state.positions) {
      if (!positions.has(mint)) {
        const tokens = BigInt(held.token_amount);
        if (tokens > 0n) {
          const costPerToken = (BigInt(held.cost_lamports) * SCALE) / tokens;
          const curve = state.curves.get(mint);
          const mark =
            curve !== undefined
              ? scaledPrice(curve.virtual_quote_reserves, curve.virtual_token_reserves)
              : costPerToken;
          positions.set(mint, {
            mint,
            buyLamports: BigInt(held.cost_lamports),
            costPerToken,
            entrySlot: batch.slot,
            entryBatch: batchIndex,
            peakPrice: mark,
          });
        }
      }
    }
    // Drop book entries whose position is gone without a sell fill (the
    // harness closed the curve, or flattened at the end of data).
    for (const mint of [...positions.keys()]) {
      if (!state.positions.has(mint)) positions.delete(mint);
    }

    // --- 3. Equity, peak, drawdown brake. ----------------------------------
    let equity = state.cash;
    for (const [mint, pos] of positions) {
      const held = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (held === undefined || curve === undefined) continue;
      const price = scaledPrice(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      if (price > 0n) equity += (BigInt(held.token_amount) * price) / SCALE;
      if (price > pos.peakPrice) pos.peakPrice = price;
    }
    if (equity > peakEquityLamports) peakEquityLamports = equity;
    const halted = equity * 10_000n < peakEquityLamports * BigInt(10_000 - MAX_DRAWDOWN_BPS);

    // --- 4. Exits first: manage every open position. -----------------------
    const pendingSellMints = new Set<string>();
    for (const p of state.pending.values()) {
      if (p.order.side === 'sell') pendingSellMints.add(p.order.mint);
    }
    for (const [mint, pos] of positions) {
      if (pendingSellMints.has(mint)) continue;
      const held = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (held === undefined || curve === undefined || curve.complete) continue;
      const price = scaledPrice(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      if (price <= 0n) continue;
      const tokens = BigInt(held.token_amount);
      if (tokens <= 0n) continue;

      let reason: string | null = null;
      if (upAtLeast(price, pos.costPerToken, TAKE_PROFIT_BPS)) reason = 'take-profit';
      else if (downAtLeast(price, pos.costPerToken, STOP_LOSS_BPS)) reason = 'stop-loss';
      else if (batch.slot - pos.entrySlot >= MAX_HOLD_SLOTS) reason = 'time-exit';
      else {
        // Creator exit: the creator sold while this position is held — the
        // launch is over, leave at once instead of waiting for the stop.
        // (The entry filter already skips coins whose creator sold before
        // entry, so any creator sell in the tape happened after entry.)
        const tape = tapes.get(mint);
        if (tape !== undefined && tape.creator !== null) {
          for (const t of tape.trades) {
            if (!t.isBuy && t.user === tape.creator) {
              reason = 'creator-sold';
              break;
            }
          }
        }
      }
      if (reason !== null && orders.length < caps.max_orders_per_batch) {
        orders.push({ side: 'sell', mint, token_amount: tokens, max_slippage_bps: SELL_SLIPPAGE_BPS });
        sellsSent += 1;
        pendingSellMints.add(mint);
      }
    }

    // --- 5. Entries (the snipe): scan tapes in first-seen order. -----------
    // A curve is bought only in its first few slots of life, once it holds
    // 0.3-30 SOL of real reserve, and only when the launch looks clean:
    // the creator's create-slot buy is small, few non-creator buyers have
    // piled in yet, and the creator has not sold. One entry per mint, ever.
    const paceStarved = batchIndex >= PACE_BATCH && roundTrips < PACE_MIN_ROUND_TRIPS;
    const maxAge = paceStarved ? PACE_MAX_AGE_SLOTS : MAX_AGE_SLOTS;

    if (!halted) {
      let free = state.freeCashLamports();
      let slots = caps.max_open_positions - state.committedPositionCount();
      for (const tape of tapes.values()) {
        if (orders.length >= caps.max_orders_per_batch || slots <= 0) break;
        if (tape.complete) continue;
        if (tape.createdSlot === null) continue;
        if (batch.slot - tape.createdSlot > maxAge) continue;
        if (positions.has(tape.mint)) continue;
        if ((entriesPerMint.get(tape.mint) ?? 0) >= MAX_ENTRIES_PER_MINT) continue;

        const curve = state.curves.get(tape.mint);
        if (curve === undefined || curve.complete) continue;
        if (curve.real_quote_reserves < MIN_REAL_SOL_LAMPORTS) continue;
        if (curve.real_quote_reserves > MAX_REAL_SOL_LAMPORTS) continue;

        // Rug filters over the opening tape.
        let devBuy = 0n;
        let devSold = false;
        const buyers = new Set<string>();
        for (const t of tape.trades) {
          if (tape.creator !== null && t.user === tape.creator) {
            if (t.isBuy && t.slot === tape.createdSlot) devBuy += t.solLamports;
            if (!t.isBuy) devSold = true;
          } else if (t.isBuy) {
            buyers.add(t.user);
          }
        }
        if (devBuy > MAX_DEV_BUY_LAMPORTS) continue;
        if (devSold) continue;
        if (buyers.size > MAX_BUYERS_AT_ENTRY) continue;

        const cap = state.maxBuyLamports(tape.mint);
        if (cap < MIN_BUY_LAMPORTS) continue;
        const amount = cap < STAKE_LAMPORTS ? cap : STAKE_LAMPORTS;
        if (free < amount + txFee) continue;
        orders.push({ side: 'buy', mint: tape.mint, quote_lamports: amount, max_slippage_bps: BUY_SLIPPAGE_BPS });
        buysSent += 1;
        free -= amount + txFee;
        slots -= 1;
      }
    }

    return orders;
  },

  onDone(state: BotState) {
    console.error(
      `steady-scalp done: batches=${batchIndex} buys=${buysSent} sells=${sellsSent} ` +
        `roundTrips=${roundTrips} distinctMints=${roundTripMints.size} ` +
        `cash=${state.cash} positions=${state.positions.size} peakEquity=${peakEquityLamports}`,
    );
  },
});
