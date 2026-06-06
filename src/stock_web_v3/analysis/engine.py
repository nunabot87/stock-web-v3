"""Analysis Engine - Technical indicators and Scoring v3."""

import math
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np

from ..config import get_settings
from ..database import fetchall


@dataclass
class PricePoint:
    """Single price data point."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass 
class TechnicalIndicators:
    """Calculated technical indicators."""
    symbol: str
    timestamp: datetime
    rsi: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_percent_b: Optional[float] = None


@dataclass
class ScoreComponents:
    """Individual scoring components."""
    trend_score: float = 0.0
    momentum_score: float = 0.0
    volume_score: float = 0.0
    volatility_score: float = 0.0


@dataclass
class StockScore:
    """Final scoring result."""
    symbol: str
    timestamp: datetime
    total_score: float
    components: ScoreComponents
    verdict: str  # strong_sell, sell, buy, strong_buy
    confidence: float  # 0-100
    entry_price: float
    target_price: float
    stop_loss: float
    risk_reward_ratio: float


class RSICalculator:
    """RSI calculation with Wilder's smoothing."""
    
    @staticmethod
    def calculate(prices: List[float], period: int = 14) -> Optional[float]:
        """Calculate RSI from price list."""
        if len(prices) < period + 1:
            return None
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        # Initial averages
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        
        if avg_loss == 0:
            return 100.0
        
        # Wilder's smoothing
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return round(rsi, 2)


class MACDCalculator:
    """MACD indicator calculation."""
    
    @staticmethod
    def ema(prices: List[float], period: int) -> List[float]:
        """Calculate EMA series."""
        if len(prices) < period:
            return []
        
        multiplier = 2 / (period + 1)
        ema_values = [np.mean(prices[:period])]
        
        for price in prices[period:]:
            ema_values.append((price - ema_values[-1]) * multiplier + ema_values[-1])
        
        return ema_values
    
    @classmethod
    def calculate(cls, prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate MACD, signal, histogram."""
        if len(prices) < slow + signal:
            return None, None, None
        
        ema_fast = cls.ema(prices, fast)
        ema_slow = cls.ema(prices, slow)
        
        if len(ema_fast) < len(ema_slow):
            ema_fast = [ema_fast[0]] * (len(ema_slow) - len(ema_fast)) + ema_fast
        
        macd_line = [f - s for f, s in zip(ema_fast[-len(ema_slow):], ema_slow)]
        signal_line = cls.ema(macd_line, signal)
        
        if not macd_line or not signal_line:
            return None, None, None
        
        current_macd = macd_line[-1]
        current_signal = signal_line[-1]
        histogram = current_macd - current_signal
        
        return round(current_macd, 4), round(current_signal, 4), round(histogram, 4)


class BollingerCalculator:
    """Bollinger Bands calculation."""
    
    @staticmethod
    def calculate(prices: List[float], period: int = 20, std_multiplier: float = 2.0) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate upper, lower bands and %B."""
        if len(prices) < period:
            return None, None, None
        
        sma = np.mean(prices[-period:])
        std = np.std(prices[-period:])
        
        upper = sma + (std * std_multiplier)
        lower = sma - (std * std_multiplier)
        
        # %B: position within bands
        if upper == lower:
            percent_b = 0.5
        else:
            percent_b = (prices[-1] - lower) / (upper - lower)
        
        return round(upper, 2), round(lower, 2), round(percent_b, 4)


class ScoringEngineV3:
    """
    Scoring Engine v3.
    Thresholds (from memory):
    - Strong Buy: >= 68 (was 76, adjusted for realistic max ~69-70 across 887 IDX stocks)
    - Buy: >= 50
    - Sell: >= 35
    - Strong Sell: < 30
    """
    
    THRESHOLDS = {
        'strong_buy': 68.0,
        'buy': 50.0,
        'sell': 35.0,
        'strong_sell': 30.0
    }
    
    def __init__(self):
        settings = get_settings()
        self.thresholds = {
            'strong_buy': settings.scoring_strong_buy_threshold,
            'buy': settings.scoring_buy_threshold,
            'sell': settings.scoring_sell_threshold
        }
    
    def calculate_trend_score(self, close: float, sma20: Optional[float], sma50: Optional[float]) -> float:
        """Score based on moving average positioning."""
        score = 25  # Base score
        
        if sma20 and sma50:
            # Price above both MAs = bullish
            if close > sma20 > sma50:
                score += 15
            elif close > sma20:
                score += 10
            elif close < sma20 < sma50:
                score -= 10
            elif close < sma20:
                score -= 5
            
            # Golden cross / death cross proximity
            ma_diff_pct = (sma20 - sma50) / sma50 * 100
            if ma_diff_pct > 2:
                score += 5
            elif ma_diff_pct < -2:
                score -= 5
        
        return max(0, min(40, score))
    
    def calculate_momentum_score(self, rsi: Optional[float], macd: Optional[float], 
                                  macd_signal: Optional[float]) -> float:
        """Score based on momentum indicators."""
        score = 20  # Base score
        
        if rsi is not None:
            # RSI scoring
            if rsi > 70:
                score -= 10  # Overbought
            elif rsi > 60:
                score += 10
            elif rsi > 50:
                score += 5
            elif rsi < 30:
                score -= 10  # Oversold
            elif rsi < 40:
                score -= 5
        
        if macd is not None and macd_signal is not None:
            # MACD crossover
            if macd > macd_signal:
                score += 10
                if macd > 0:
                    score += 5
            else:
                score -= 5
        
        return max(0, min(35, score))
    
    def calculate_volume_score(self, current_vol: int, avg_vol_20: Optional[float]) -> float:
        """Score based on volume patterns."""
        if not avg_vol_20 or avg_vol_20 == 0:
            return 12.5  # Neutral
        
        vol_ratio = current_vol / avg_vol_20
        
        if vol_ratio > 2:
            return 25  # High volume breakout
        elif vol_ratio > 1.5:
            return 20
        elif vol_ratio > 1.0:
            return 17.5
        elif vol_ratio > 0.5:
            return 10
        else:
            return 5  # Low volume
    
    def calculate_volatility_score(self, prices: List[float], bb_upper: Optional[float], 
                                    bb_lower: Optional[float]) -> float:
        """Score based on volatility and Bollinger Band position."""
        score = 7.5  # Base
        
        if len(prices) >= 20:
            # ATR-like calculation
            atr_window = prices[-20:]
            daily_ranges = [abs(atr_window[i] - atr_window[i-1]) for i in range(1, len(atr_window))]
            avg_range = np.mean(daily_ranges) if daily_ranges else 0
            current_price = prices[-1]
            
            volatility_pct = (avg_range / current_price) * 100 if current_price > 0 else 0
            
            # Moderate volatility is good for trading
            if 1.5 < volatility_pct < 4:
                score += 5
            elif volatility_pct > 6:
                score -= 3  # Too volatile
        
        # Bollinger position
        if bb_upper is not None and bb_lower is not None and prices:
            price = prices[-1]
            band_width = bb_upper - bb_lower
            if band_width > 0:
                position = (price - bb_lower) / band_width
                if position > 0.8:
                    score -= 2  # Near upper band
                elif position < 0.2:
                    score += 2  # Near lower band (potential bounce)
        
        return max(0, min(15, score))
    
    def calculate_targets(self, close: float, components: ScoreComponents) -> Tuple[float, float, float, float]:
        """Calculate entry, target, stop loss, and R/R ratio."""
        # Entry is current close
        entry = close
        
        # Target based on momentum strength
        momentum_factor = components.momentum_score / 35  # 0-1
        target_pct = 0.03 + (momentum_factor * 0.07)  # 3-10%
        target = close * (1 + target_pct)
        
        # Stop loss based on volatility
        volatility_factor = components.volatility_score / 15  # 0-1
        stop_pct = 0.03 + ((1 - volatility_factor) * 0.04)  # 3-7%
        stop_loss = close * (1 - stop_pct)
        
        # Risk/Reward ratio (capped at 4.0 as per user requirement)
        reward = target - close
        risk = close - stop_loss
        
        if risk > 0:
            rr = min(4.0, reward / risk)
        else:
            rr = 1.0
        
        return entry, target, stop_loss, round(rr, 2)
    
    def get_verdict(self, total_score: float) -> str:
        """Determine verdict from total score - returns user-friendly strings."""
        if total_score >= self.thresholds['strong_buy']:
            return 'STRONG BUY'
        elif total_score >= self.thresholds['buy']:
            return 'BUY'
        elif total_score >= self.thresholds['sell']:
            return 'SELL'
        else:
            return 'STRONG SELL'
    
    def calculate_confidence(self, components: ScoreComponents, indicators: TechnicalIndicators) -> float:
        """Calculate confidence percentage based on data quality."""
        confidence = 60  # Base
        
        # +10 for each complete indicator
        if indicators.rsi is not None:
            confidence += 10
        if indicators.macd is not None:
            confidence += 10
        if indicators.sma_20 is not None and indicators.sma_50 is not None:
            confidence += 10
        if indicators.bb_upper is not None:
            confidence += 10
        
        return min(100, confidence)

    def get_component_breakdown(self, components: ScoreComponents) -> Dict[str, Any]:
        """Convert raw component scores to frontend format with score/max/percentage/grade."""
        max_scores = {
            "trend": 40.0,
            "momentum": 35.0,
            "volume": 25.0,
            "volatility": 15.0
        }
        raw = {
            "trend": components.trend_score,
            "momentum": components.momentum_score,
            "volume": components.volume_score,
            "volatility": components.volatility_score
        }
        breakdown = {}
        for key, score in raw.items():
            max_val = max_scores[key]
            pct = (score / max_val * 100.0) if max_val > 0 else 0.0
            if pct >= 60:
                grade = "A"  # STRONG BUY
            elif pct >= 40:
                grade = "B"  # BUY
            elif pct >= 20:
                grade = "C"  # HOLD
            else:
                grade = "D"  # SELL
            breakdown[key] = {
                "score": round(score, 1),
                "max": max_val,
                "percentage": round(pct, 1),
                "grade": grade
            }
        return breakdown


class AnalysisEngine:
    """Main analysis engine orchestrating all calculations."""
    
    def __init__(self):
        self.rsi_calc = RSICalculator()
        self.macd_calc = MACDCalculator()
        self.bb_calc = BollingerCalculator()
        self.scoring = ScoringEngineV3()
    
    async def get_historical_prices(self, symbol: str, days: int = 60) -> List[PricePoint]:
        """Fetch historical prices from database."""
        query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM stock_prices_daily
            WHERE symbol = $1 
            AND timestamp >= NOW() - INTERVAL '%s days'
            ORDER BY timestamp ASC
        """ % days
        
        rows = await fetchall(query, symbol)
        
        return [
            PricePoint(
                symbol=r['symbol'],
                timestamp=r['timestamp'],
                open=float(r['open']),
                high=float(r['high']),
                low=float(r['low']),
                close=float(r['close']),
                volume=r['volume']
            )
            for r in rows
        ]
    
    def calculate_indicators(self, prices: List[PricePoint]) -> TechnicalIndicators:
        """Calculate all technical indicators from price series."""
        if not prices:
            return TechnicalIndicators(symbol="", timestamp=datetime.now())
        
        closes = [p.close for p in prices]
        volumes = [p.volume for p in prices]
        
        symbol = prices[-1].symbol
        timestamp = prices[-1].timestamp
        
        # RSI
        rsi = self.rsi_calc.calculate(closes)
        
        # MACD
        macd, macd_signal, macd_hist = self.macd_calc.calculate(closes)
        
        # SMAs
        sma_20 = np.mean(closes[-20:]) if len(closes) >= 20 else None
        sma_50 = np.mean(closes[-50:]) if len(closes) >= 50 else None
        
        # EMAs (use last values if available)
        ema_12 = None
        ema_26 = None
        if len(closes) >= 12:
            ema_12_values = self.macd_calc.ema(closes, 12)
            ema_12 = ema_12_values[-1] if ema_12_values else None
        if len(closes) >= 26:
            ema_26_values = self.macd_calc.ema(closes, 26)
            ema_26 = ema_26_values[-1] if ema_26_values else None
        
        # Bollinger Bands
        bb_upper, bb_lower, bb_pct = self.bb_calc.calculate(closes)
        
        return TechnicalIndicators(
            symbol=symbol,
            timestamp=timestamp,
            rsi=rsi,
            macd=macd,
            macd_signal=macd_signal,
            macd_histogram=macd_hist,
            sma_20=round(sma_20, 2) if sma_20 else None,
            sma_50=round(sma_50, 2) if sma_50 else None,
            ema_12=round(ema_12, 2) if ema_12 else None,
            ema_26=round(ema_26, 2) if ema_26 else None,
            bb_upper=bb_upper,
            bb_lower=bb_lower,
            bb_percent_b=bb_pct
        )
    
    def calculate_score(self, prices: List[PricePoint], indicators: TechnicalIndicators) -> StockScore:
        """Calculate complete stock score."""
        if not prices or not indicators:
            return StockScore(
                symbol=prices[-1].symbol if prices else "",
                timestamp=datetime.now(),
                total_score=0,
                components=ScoreComponents(),
                verdict="neutral",
                confidence=0,
                entry_price=0,
                target_price=0,
                stop_loss=0,
                risk_reward_ratio=0
            )
        
        closes = [p.close for p in prices]
        volumes = [p.volume for p in prices]
        current_close = closes[-1]
        current_vol = volumes[-1]
        
        # Calculate components
        avg_vol_20 = np.mean(volumes[-20:]) if len(volumes) >= 20 else None
        
        trend_score = self.scoring.calculate_trend_score(
            current_close, indicators.sma_20, indicators.sma_50
        )
        
        momentum_score = self.scoring.calculate_momentum_score(
            indicators.rsi, indicators.macd, indicators.macd_signal
        )
        
        volume_score = self.scoring.calculate_volume_score(current_vol, avg_vol_20)
        
        volatility_score = self.scoring.calculate_volatility_score(
            closes, indicators.bb_upper, indicators.bb_lower
        )
        
        components = ScoreComponents(
            trend_score=trend_score,
            momentum_score=momentum_score,
            volume_score=volume_score,
            volatility_score=volatility_score
        )
        
        total_score = trend_score + momentum_score + volume_score + volatility_score
        
        # Calculate targets
        entry, target, stop, rr = self.scoring.calculate_targets(current_close, components)
        
        verdict = self.scoring.get_verdict(total_score)
        confidence = self.scoring.calculate_confidence(components, indicators)
        
        return StockScore(
            symbol=prices[-1].symbol,
            timestamp=prices[-1].timestamp,
            total_score=round(total_score, 1),
            components=components,
            verdict=verdict,
            confidence=confidence,
            entry_price=round(entry, 2),
            target_price=round(target, 2),
            stop_loss=round(stop, 2),
            risk_reward_ratio=rr
        )
    
    async def get_fundamental_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch fundamental data from stock_fundamentals table."""
        query = """
            SELECT pe_ratio, forward_pe, pbv, psr, peg, eps,
                   gross_margin, operating_margin, profit_margin,
                   roe, roa, total_revenue, revenue_growth,
                   total_debt, debt_to_equity, current_ratio,
                   dividend_rate, dividend_yield, payout_ratio,
                   market_cap, enterprise_value, shares_outstanding,
                   float_shares, held_percent_insiders, held_percent_institutions,
                   beta, avg_volume, fifty_two_week_high, fifty_two_week_low,
                   annual_revenue, fetched_at
            FROM stock_fundamentals
            WHERE symbol = $1
            ORDER BY fetched_at DESC
            LIMIT 1
        """
        rows = await fetchall(query, symbol)
        if not rows:
            return None
        r = rows[0]
        return {
            "valuation": {
                "pe_ratio": float(r["pe_ratio"]) if r.get("pe_ratio") is not None else None,
                "forward_pe": float(r["forward_pe"]) if r.get("forward_pe") is not None else None,
                "pbv": float(r["pbv"]) if r.get("pbv") is not None else None,
                "psr": float(r["psr"]) if r.get("psr") is not None else None,
                "peg": float(r["peg"]) if r.get("peg") is not None else None,
                "eps": float(r["eps"]) if r.get("eps") is not None else None,
                "market_cap": float(r["market_cap"]) if r.get("market_cap") is not None else None,
                "enterprise_value": float(r["enterprise_value"]) if r.get("enterprise_value") is not None else None
            },
            "profitability": {
                "roe": float(r["roe"]) if r.get("roe") is not None else None,
                "roa": float(r["roa"]) if r.get("roa") is not None else None,
                "gross_margin": float(r["gross_margin"]) if r.get("gross_margin") is not None else None,
                "operating_margin": float(r["operating_margin"]) if r.get("operating_margin") is not None else None,
                "profit_margin": float(r["profit_margin"]) if r.get("profit_margin") is not None else None,
                "revenue_growth": float(r["revenue_growth"]) if r.get("revenue_growth") is not None else None
            },
            "financial_health": {
                "debt_to_equity": float(r["debt_to_equity"]) if r.get("debt_to_equity") is not None else None,
                "current_ratio": float(r["current_ratio"]) if r.get("current_ratio") is not None else None,
                "payout_ratio": float(r["payout_ratio"]) if r.get("payout_ratio") is not None else None
            },
            "market_data": {
                "fifty_two_week_high": float(r["fifty_two_week_high"]) if r.get("fifty_two_week_high") is not None else None,
                "fifty_two_week_low": float(r["fifty_two_week_low"]) if r.get("fifty_two_week_low") is not None else None,
                "beta": float(r["beta"]) if r.get("beta") is not None else None,
                "dividend_yield": float(r["dividend_yield"]) if r.get("dividend_yield") is not None else None,
                "dividend_rate": float(r["dividend_rate"]) if r.get("dividend_rate") is not None else None,
                "shares_outstanding": float(r["shares_outstanding"]) if r.get("shares_outstanding") is not None else None,
                "float_shares": float(r["float_shares"]) if r.get("float_shares") is not None else None
            },
            "fetched_at": r["fetched_at"].isoformat() if r.get("fetched_at") else None
        }

    async def analyze_stock(self, symbol: str) -> Dict[str, Any]:
        """Full analysis pipeline for a single stock."""
        prices = await self.get_historical_prices(symbol)

        if not prices:
            return {"error": f"No data available for {symbol}"}

        indicators = self.calculate_indicators(prices)
        score = self.calculate_score(prices, indicators)
        fundamental = await self.get_fundamental_data(symbol)

        result = {
            "symbol": symbol,
            "timestamp": score.timestamp.isoformat(),
            "indicators": {
                "rsi": indicators.rsi,
                "macd": indicators.macd,
                "macd_signal": indicators.macd_signal,
                "macd_histogram": indicators.macd_histogram,
                "sma_20": indicators.sma_20,
                "sma_50": indicators.sma_50,
                "bb_upper": indicators.bb_upper,
                "bb_lower": indicators.bb_lower,
                "bb_percent_b": indicators.bb_percent_b
            },
            "scoring": {
                "total_score": score.total_score,
                "verdict": score.verdict,
                "confidence": score.confidence,
                "component_detail": self.scoring.get_component_breakdown(score.components)
            },
            "recommendation": {
                "entry_price": score.entry_price,
                "target_price": score.target_price,
                "stop_loss": score.stop_loss,
                "risk_reward_ratio": score.risk_reward_ratio
            }
        }
        if fundamental:
            result["fundamental"] = fundamental
        return result


# Singleton instance
analysis_engine = AnalysisEngine()


async def analyze_symbol(symbol: str) -> Dict[str, Any]:
    """Public API for stock analysis."""
    return await analysis_engine.analyze_stock(symbol)