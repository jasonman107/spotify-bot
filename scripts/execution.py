#!/usr/bin/env python3
"""Order execution for the tweet-count trader.

Two executors behind the same interface:
  - PaperExecutor: log-only, assumes full fill at the quoted price.
  - LiveExecutor : real CLOB orders via py-clob-client-v2 using a Polymarket
    deposit wallet (SIGNATURE_TYPE=3 / POLY_1271, funder = deposit wallet).

Both return fill dicts {"price", "shares", "cost"} (or None when nothing
filled). Sizes follow the Polymarket decimal rules: price on the market tick,
size <= 2dp, maker amount (price*size) <= 2dp for buys.

Env vars (live mode):
  PRIVATE_KEY      owner/session EOA key (required)
  FUNDER_ADDRESS   deployed deposit-wallet / proxyWallet address (required)
  SIGNATURE_TYPE   default 3 (POLY_1271)
  CLOB_API_URL     default https://clob.polymarket.com
  CLOB_API_KEY / CLOB_SECRET / CLOB_PASS_PHRASE
                   optional pre-derived API creds; derived at startup if absent
"""

import math
import os
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

CLOB_HOST_DEFAULT = "https://clob.polymarket.com"
CHAIN_ID = 137


# ── Decimal sizing helpers (port of the Rust clean_token_size rules) ─────────

def is_maker_clean(price: Decimal, tokens: Decimal) -> bool:
    """True when price*tokens has at most 2 decimal places."""
    maker = price * tokens
    return maker == maker.quantize(Decimal("0.01"))


def maker_clean_step(price: Decimal) -> Decimal | None:
    """Smallest integer token step n such that price*n is 2dp-clean."""
    exp = -price.as_tuple().exponent
    num = int(price.scaleb(exp))          # price = num / 10^exp
    denom = 10 ** exp
    if denom <= 100:
        return Decimal(1)
    step = (denom // 100) // math.gcd(num, denom // 100)
    return Decimal(step)


def clean_token_size(order_size_usdc: Decimal, price: Decimal) -> Decimal:
    """Largest token size <= order_size/price with size <=2dp and clean maker
    amount. Returns Decimal(0) when nothing valid fits."""
    if price <= 0:
        return Decimal(0)
    raw = order_size_usdc / price
    for dp in (2, 1, 0):
        candidate = raw.quantize(Decimal(1).scaleb(-dp), rounding=ROUND_DOWN)
        if candidate <= 0:
            continue
        if is_maker_clean(price, candidate):
            return candidate.normalize()
    step = maker_clean_step(price)
    if step and step > 0:
        n = (raw / step).to_integral_value(rounding=ROUND_DOWN) * step
        if n > 0:
            return n.normalize()
    return Decimal(0)


def round_to_tick(price: float, tick: str) -> Decimal:
    return Decimal(str(price)).quantize(Decimal(tick), rounding=ROUND_HALF_UP)


def trunc2(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


# ── Executors ────────────────────────────────────────────────────────────────

class PaperExecutor:
    """Log-only execution: full fill at the quoted price, sized like live."""

    live = False

    def buy(self, token_id: str, price: float, size_usdc: float):
        px = Decimal(str(price))
        tokens = clean_token_size(Decimal(str(size_usdc)), px)
        if tokens <= 0:
            return None
        return {"price": float(px), "shares": float(tokens),
                "cost": float(px * tokens)}

    def sell(self, token_id: str, price: float, shares: float):
        px = Decimal(str(price))
        tokens = trunc2(shares)
        if tokens <= 0:
            return None
        return {"price": float(px), "shares": float(tokens),
                "proceeds": float(px * tokens)}


class LiveExecutor:
    """Real FAK orders through the CLOB with the type-3 deposit wallet."""

    live = True

    def __init__(self, client=None):
        if client is not None:  # injected in tests
            self.client = client
            return
        from py_clob_client_v2 import ApiCreds, ClobClient

        key = os.environ.get("PRIVATE_KEY", "")
        funder = os.environ.get("FUNDER_ADDRESS", "")
        if not key or not funder:
            raise RuntimeError(
                "live mode requires PRIVATE_KEY and FUNDER_ADDRESS "
                "(run scripts/setup_live_wallet.py first)")
        sig_type = int(os.environ.get("SIGNATURE_TYPE", "3"))
        host = os.environ.get("CLOB_API_URL", CLOB_HOST_DEFAULT)

        creds = None
        if os.environ.get("CLOB_API_KEY"):
            creds = ApiCreds(
                api_key=os.environ["CLOB_API_KEY"],
                api_secret=os.environ["CLOB_SECRET"],
                api_passphrase=os.environ["CLOB_PASS_PHRASE"],
            )
        self.client = ClobClient(
            host=host, chain_id=CHAIN_ID, key=key, creds=creds,
            signature_type=sig_type, funder=funder,
        )
        if creds is None:
            self.client.set_api_creds(self.client.create_or_derive_api_key())

    def _post_fak(self, token_id: str, price: Decimal, size: Decimal,
                  side: str):
        """Post a marketable FAK limit order; return (shares, quote_amount)
        actually matched. Amounts come from the order response, with a
        get_order fallback for the share count."""
        from py_clob_client_v2 import OrderArgs, OrderType

        try:
            resp = self.client.create_and_post_order(
                order_args=OrderArgs(
                    token_id=token_id, price=float(price), size=float(size),
                    side=side),
                order_type=OrderType.FAK,
            )
        except Exception as e:
            # e.g. 400 "no orders found to match" when the book side is empty
            # — treat as a zero fill, never abort the caller's cycle
            print(f"order failed ({side} {size} @ {price}): {e}")
            return 0.0, 0.0
        if not isinstance(resp, dict):
            return 0.0, 0.0
        if not resp.get("success", False):
            print(f"order rejected: {resp.get('errorMsg', resp)}")
            return 0.0, 0.0
        making = float(resp.get("makingAmount") or 0)
        taking = float(resp.get("takingAmount") or 0)
        # BUY: maker amount is USDC, taker amount is tokens. SELL: reversed.
        shares = taking if side == "BUY" else making
        quote = making if side == "BUY" else taking
        if shares <= 0 and resp.get("orderID"):
            try:
                order = self.client.get_order(resp["orderID"]) or {}
                shares = float(order.get("size_matched")
                               or order.get("sizeMatched") or 0)
                quote = shares * float(price)
            except Exception as e:
                print(f"get_order fallback failed: {e}")
        return shares, quote

    def buy(self, token_id: str, price: float, size_usdc: float):
        tick = self._tick(token_id)
        px = round_to_tick(price, tick)
        tokens = clean_token_size(Decimal(str(size_usdc)), px)
        if tokens <= 0:
            return None
        shares, cost = self._post_fak(token_id, px, tokens, "BUY")
        if shares <= 0:
            return None
        return {"price": (cost / shares) if cost > 0 else float(px),
                "shares": shares, "cost": cost or shares * float(px)}

    def sell(self, token_id: str, price: float, shares: float):
        tick = self._tick(token_id)
        px = round_to_tick(price, tick)
        tokens = trunc2(shares)
        if tokens <= 0:
            return None
        filled, proceeds = self._post_fak(token_id, px, tokens, "SELL")
        if filled <= 0:
            return None
        return {"price": (proceeds / filled) if proceeds > 0 else float(px),
                "shares": filled, "proceeds": proceeds or filled * float(px)}

    def _tick(self, token_id: str) -> str:
        try:
            return str(self.client.get_tick_size(token_id))
        except Exception:
            return "0.001"  # tick size of the tweet-count bucket markets


def make_executor(mode: str | None = None):
    """TRADING_MODE=live places real orders; anything else (or unset) is
    paper. The optional `mode` argument overrides the env var."""
    mode = (mode or os.environ.get("TRADING_MODE") or "paper").strip().lower()
    if mode == "live":
        return LiveExecutor()
    return PaperExecutor()
