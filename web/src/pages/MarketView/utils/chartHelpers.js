/**
 * Sliding-window Simple Moving Average — O(n)
 */
export function calculateMA(data, period) {
  if (data.length < period) return [];
  const result = [];
  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += data[i].close;
  }
  result.push({ time: data[period - 1].time, value: sum / period });
  for (let i = period; i < data.length; i++) {
    sum += data[i].close - data[i - period].close;
    const value = sum / period;
    if (!isNaN(value) && isFinite(value)) {
      result.push({ time: data[i].time, value });
    }
  }
  return result;
}

/**
 * Wilder's smoothed RSI — O(n), correct algorithm.
 * Returns { data: [{ time, value }], state: { avgGain, avgLoss, lastClose, period } }
 * so live ticks can continue incrementally via updateRSIIncremental().
 */
export function calculateRSI(data, period = 14) {
  if (data.length < period + 1) return { data: [], state: null };
  const result = [];

  // Calculate price changes
  let avgGain = 0;
  let avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const change = data[i].close - data[i - 1].close;
    if (change > 0) avgGain += change;
    else avgLoss += -change;
  }
  avgGain /= period;
  avgLoss /= period;

  // First RSI value
  const firstRS = avgLoss === 0 ? 100 : avgGain / avgLoss;
  const firstRSI = avgLoss === 0 ? 100 : 100 - 100 / (1 + firstRS);
  result.push({ time: data[period].time, value: firstRSI });

  // Wilder's exponential smoothing for subsequent values
  for (let i = period + 1; i < data.length; i++) {
    const change = data[i].close - data[i - 1].close;
    const currentGain = change > 0 ? change : 0;
    const currentLoss = change < 0 ? -change : 0;
    avgGain = (avgGain * (period - 1) + currentGain) / period;
    avgLoss = (avgLoss * (period - 1) + currentLoss) / period;
    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    const rsi = avgLoss === 0 ? 100 : 100 - 100 / (1 + rs);
    if (!isNaN(rsi) && isFinite(rsi)) {
      result.push({ time: data[i].time, value: rsi });
    }
  }

  return {
    data: result,
    state: { avgGain, avgLoss, lastClose: data[data.length - 1].close, period },
  };
}

/**
 * O(1) incremental RSI update for a single new bar.
 * @param {{ avgGain, avgLoss, lastClose, period }} prevState — from calculateRSI().state or a prior call
 * @param {number} newClose — the new bar's close price
 * @returns {{ value: number, state: { avgGain, avgLoss, lastClose, period } }}
 */
export function updateRSIIncremental(prevState, newClose) {
  const { avgGain: prevAvgGain, avgLoss: prevAvgLoss, lastClose, period } = prevState;
  const change = newClose - lastClose;
  const gain = change > 0 ? change : 0;
  const loss = change < 0 ? -change : 0;
  const avgGain = (prevAvgGain * (period - 1) + gain) / period;
  const avgLoss = (prevAvgLoss * (period - 1) + loss) / period;
  const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
  const value = avgLoss === 0 ? 100 : 100 - 100 / (1 + rs);
  return { value, state: { avgGain, avgLoss, lastClose: newClose, period } };
}
