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
 * Wilder's smoothed RSI — O(n), correct algorithm
 */
export function calculateRSI(data, period = 14) {
  if (data.length < period + 1) return [];
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
  return result;
}
