"""
Sysco Revenue Management — Analytics Engine
Core pricing analytics: margin decomposition, override recommendations,
lever change impact measurement, and data integrity validation.
"""

import pandas as pd
import numpy as np
from scipy import stats
import json
import warnings
warnings.filterwarnings("ignore")


def load_data():
    products = pd.read_csv("/home/claude/pricing_engine/products.csv")
    customers = pd.read_csv("/home/claude/pricing_engine/customers.csv")
    txns = pd.read_csv("/home/claude/pricing_engine/transactions.csv")
    return products, customers, txns


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 1: PORTFOLIO HEALTH MONITOR (Weekly Pricing Review)
# ═══════════════════════════════════════════════════════════════════════════════

def weekly_portfolio_summary(txns):
    """
    Produces the weekly pricing review pack — the core operating rhythm
    of a Revenue Management Analyst at Sysco.
    """
    weekly = txns.groupby("week_number").agg(
        total_net_sales=("net_sales", "sum"),
        total_cogs=("cogs", "sum"),
        total_gp=("gross_profit_dollars", "sum"),
        total_cases=("cases_ordered", "sum"),
        unique_customers=("customer_id", "nunique"),
        unique_products=("product_id", "nunique"),
        override_count=("has_override", "sum"),
        transaction_count=("transaction_id", "count"),
    ).reset_index()

    weekly["gp_pct"] = (weekly["total_gp"] / weekly["total_net_sales"]).round(4)
    weekly["avg_price_per_case"] = (weekly["total_net_sales"] / weekly["total_cases"]).round(2)
    weekly["avg_cost_per_case"] = (weekly["total_cogs"] / weekly["total_cases"]).round(2)
    weekly["override_rate"] = (weekly["override_count"] / weekly["transaction_count"]).round(4)
    weekly["gp_per_case"] = (weekly["total_gp"] / weekly["total_cases"]).round(2)

    # Week-over-week changes
    weekly["sales_wow"] = weekly["total_net_sales"].pct_change().round(4)
    weekly["gp_wow"] = weekly["total_gp"].pct_change().round(4)
    weekly["volume_wow"] = weekly["total_cases"].pct_change().round(4)
    weekly["gp_pct_delta"] = weekly["gp_pct"].diff().round(4)

    return weekly


def category_performance(txns):
    """Category-level margin and volume analysis for the managed portfolio."""
    cat = txns.groupby(["week_number", "category"]).agg(
        net_sales=("net_sales", "sum"),
        cogs=("cogs", "sum"),
        gp=("gross_profit_dollars", "sum"),
        cases=("cases_ordered", "sum"),
        products=("product_id", "nunique"),
    ).reset_index()
    cat["gp_pct"] = (cat["gp"] / cat["net_sales"]).round(4)
    cat["revenue_per_case"] = (cat["net_sales"] / cat["cases"]).round(2)
    return cat


def segment_performance(txns):
    """Customer segment-level performance tracking."""
    seg = txns.groupby(["week_number", "segment"]).agg(
        net_sales=("net_sales", "sum"),
        cogs=("cogs", "sum"),
        gp=("gross_profit_dollars", "sum"),
        cases=("cases_ordered", "sum"),
        customers=("customer_id", "nunique"),
    ).reset_index()
    seg["gp_pct"] = (seg["gp"] / seg["net_sales"]).round(4)
    return seg


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 2: MARGIN DECOMPOSITION (Price / Cost / Mix Bridge)
# ═══════════════════════════════════════════════════════════════════════════════

def margin_bridge(txns, period_a_weeks=(1, 6), period_b_weeks=(7, 16)):
    """
    Decomposes GP$ change between two periods into:
    - Price effect (what changed because net price moved)
    - Cost effect (what changed because cost moved)
    - Volume effect (what changed because volume moved)
    - Mix effect (residual from product/customer composition shift)
    """
    a = txns[txns["week_number"].between(*period_a_weeks)]
    b = txns[txns["week_number"].between(*period_b_weeks)]

    # Aggregate by product
    def agg_period(df):
        return df.groupby("product_id").agg(
            avg_net_price=("net_price", "mean"),
            avg_cost=("unit_cost", "mean"),
            total_cases=("cases_ordered", "sum"),
            total_sales=("net_sales", "sum"),
            total_cogs=("cogs", "sum"),
            total_gp=("gross_profit_dollars", "sum"),
        ).reset_index()

    agg_a = agg_period(a)
    agg_b = agg_period(b)

    merged = agg_a.merge(agg_b, on="product_id", suffixes=("_a", "_b"), how="inner")

    # Normalize for weeks in each period
    weeks_a = period_a_weeks[1] - period_a_weeks[0] + 1
    weeks_b = period_b_weeks[1] - period_b_weeks[0] + 1

    # Per-week averages
    merged["cases_per_wk_a"] = merged["total_cases_a"] / weeks_a
    merged["cases_per_wk_b"] = merged["total_cases_b"] / weeks_b

    # Price effect: (Price_B - Price_A) * Volume_A
    merged["price_effect"] = (
        (merged["avg_net_price_b"] - merged["avg_net_price_a"]) * merged["cases_per_wk_a"]
    )
    # Cost effect: -(Cost_B - Cost_A) * Volume_A (negative because cost increase hurts GP)
    merged["cost_effect"] = -(
        (merged["avg_cost_b"] - merged["avg_cost_a"]) * merged["cases_per_wk_a"]
    )
    # Volume effect: (Volume_B - Volume_A) * Margin_A_per_case
    merged["margin_per_case_a"] = merged["avg_net_price_a"] - merged["avg_cost_a"]
    merged["volume_effect"] = (
        (merged["cases_per_wk_b"] - merged["cases_per_wk_a"]) * merged["margin_per_case_a"]
    )
    # Mix = residual
    gp_per_wk_a = merged["total_gp_a"].sum() / weeks_a
    gp_per_wk_b = merged["total_gp_b"].sum() / weeks_b
    total_explained = merged["price_effect"].sum() + merged["cost_effect"].sum() + merged["volume_effect"].sum()
    mix_effect = (gp_per_wk_b - gp_per_wk_a) - total_explained

    bridge = {
        "period_a": f"Weeks {period_a_weeks[0]}-{period_a_weeks[1]}",
        "period_b": f"Weeks {period_b_weeks[0]}-{period_b_weeks[1]}",
        "gp_per_week_a": round(gp_per_wk_a, 2),
        "gp_per_week_b": round(gp_per_wk_b, 2),
        "delta_gp_per_week": round(gp_per_wk_b - gp_per_wk_a, 2),
        "price_effect": round(merged["price_effect"].sum(), 2),
        "cost_effect": round(merged["cost_effect"].sum(), 2),
        "volume_effect": round(merged["volume_effect"].sum(), 2),
        "mix_effect": round(mix_effect, 2),
    }

    # Category-level bridge
    cat_bridge = []
    for cat in txns["category"].unique():
        a_cat = a[a["category"] == cat]
        b_cat = b[b["category"] == cat]
        if len(a_cat) == 0 or len(b_cat) == 0:
            continue
        gp_a = a_cat["gross_profit_dollars"].sum() / weeks_a
        gp_b = b_cat["gross_profit_dollars"].sum() / weeks_b
        cost_a = a_cat["unit_cost"].mean()
        cost_b = b_cat["unit_cost"].mean()
        price_a = a_cat["net_price"].mean()
        price_b = b_cat["net_price"].mean()
        cat_bridge.append({
            "category": cat,
            "gp_per_week_a": round(gp_a, 2),
            "gp_per_week_b": round(gp_b, 2),
            "gp_delta": round(gp_b - gp_a, 2),
            "avg_cost_change_pct": round((cost_b - cost_a) / cost_a, 4),
            "avg_price_change_pct": round((price_b - price_a) / price_a, 4),
        })

    return bridge, pd.DataFrame(cat_bridge).sort_values("gp_delta")


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 3: PRICING OVERRIDE RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def compute_price_sensitivity(txns):
    """
    Estimate customer-product level price sensitivity using
    historical price-volume correlation as a proxy for elasticity.
    """
    # Need at least 4 weeks of data per customer-product pair
    pairs = txns.groupby(["customer_id", "product_id"]).filter(lambda x: len(x) >= 4)

    sensitivities = []
    for (cid, pid), group in pairs.groupby(["customer_id", "product_id"]):
        if group["net_price"].std() < 0.01:
            continue
        price_pct_change = group["net_price"].pct_change().dropna()
        vol_pct_change = group["cases_ordered"].pct_change().dropna()
        if len(price_pct_change) < 3:
            continue

        corr, p_val = stats.pearsonr(price_pct_change, vol_pct_change)
        # Elasticity proxy: vol%_change / price%_change
        avg_elasticity = (vol_pct_change.mean() / price_pct_change.mean()
                          if abs(price_pct_change.mean()) > 0.001 else 0)

        sensitivities.append({
            "customer_id": cid,
            "product_id": pid,
            "price_vol_corr": round(corr, 3),
            "elasticity_proxy": round(np.clip(avg_elasticity, -5, 5), 3),
            "p_value": round(p_val, 4),
            "sensitivity_label": (
                "High" if corr < -0.3 else
                "Medium" if corr < -0.1 else
                "Low"
            ),
        })

    return pd.DataFrame(sensitivities)


def generate_override_recommendations(txns, gp_floor=0.18):
    """
    Core override recommendation engine.
    Identifies customer-product pairs below GP target and recommends
    specific price actions with impact estimates and confidence levels.
    """
    # Focus on recent 4 weeks
    recent = txns[txns["week_number"] >= 13]

    # Aggregate at customer-product level
    cp = recent.groupby(["customer_id", "customer_name", "segment",
                          "product_id", "description", "category",
                          "is_commodity", "pricing_tier"]).agg(
        avg_net_price=("net_price", "mean"),
        avg_cost=("unit_cost", "mean"),
        total_cases=("cases_ordered", "sum"),
        total_sales=("net_sales", "sum"),
        total_cogs=("cogs", "sum"),
        total_gp=("gross_profit_dollars", "sum"),
        weeks_ordered=("week_number", "nunique"),
    ).reset_index()

    cp["current_gp_pct"] = cp["total_gp"] / cp["total_sales"]
    cp["gp_gap"] = cp["current_gp_pct"] - gp_floor

    # Filter: below floor
    below_target = cp[cp["gp_gap"] < 0].copy()

    if len(below_target) == 0:
        return pd.DataFrame()

    recommendations = []
    for _, row in below_target.iterrows():
        # Calculate required price to hit floor
        required_price = row["avg_cost"] / (1 - gp_floor)
        price_increase_needed = required_price - row["avg_net_price"]
        price_increase_pct = price_increase_needed / row["avg_net_price"]

        # Estimate volume risk based on increase magnitude
        if price_increase_pct < 0.02:
            vol_risk = "Low"
            confidence = "High"
            est_vol_loss = 0.01
        elif price_increase_pct < 0.05:
            vol_risk = "Medium"
            confidence = "Medium"
            est_vol_loss = 0.04
        elif price_increase_pct < 0.10:
            vol_risk = "Medium-High"
            confidence = "Medium"
            est_vol_loss = 0.08
        else:
            vol_risk = "High"
            confidence = "Low"
            est_vol_loss = 0.15

        # Projected impact
        weekly_cases = row["total_cases"] / row["weeks_ordered"]
        projected_new_vol = weekly_cases * (1 - est_vol_loss)
        projected_new_sales = projected_new_vol * required_price
        projected_new_cogs = projected_new_vol * row["avg_cost"]
        projected_new_gp = projected_new_sales - projected_new_cogs
        current_weekly_gp = row["total_gp"] / row["weeks_ordered"]
        gp_uplift = projected_new_gp - current_weekly_gp

        # Reason code
        if row["is_commodity"]:
            reason = "Commodity cost pass-through required"
        elif price_increase_pct > 0.08:
            reason = "Significant margin erosion — structural reprice needed"
        else:
            reason = "Below-target margin — standard override recommended"

        recommendations.append({
            "customer_id": row["customer_id"],
            "customer_name": row["customer_name"],
            "segment": row["segment"],
            "product_id": row["product_id"],
            "description": row["description"],
            "category": row["category"],
            "is_commodity": row["is_commodity"],
            "pricing_tier": row["pricing_tier"],
            "current_net_price": round(row["avg_net_price"], 2),
            "current_cost": round(row["avg_cost"], 2),
            "current_gp_pct": round(row["current_gp_pct"], 4),
            "target_gp_pct": gp_floor,
            "gp_gap_bps": round(row["gp_gap"] * 10000),
            "recommended_price": round(required_price, 2),
            "price_change_dollars": round(price_increase_needed, 2),
            "price_change_pct": round(price_increase_pct, 4),
            "weekly_cases_current": round(weekly_cases, 1),
            "est_volume_loss_pct": est_vol_loss,
            "volume_risk": vol_risk,
            "confidence": confidence,
            "projected_weekly_gp_uplift": round(gp_uplift, 2),
            "projected_annual_gp_impact": round(gp_uplift * 52, 2),
            "reason_code": reason,
            "action": "OVERRIDE_UP",
        })

    rec_df = pd.DataFrame(recommendations)
    rec_df = rec_df.sort_values("projected_annual_gp_impact", ascending=False)
    return rec_df


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 4: PRICING LEVER CHANGE IMPACT ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def lever_change_impact(txns):
    """
    Identifies the commodity cost increase (lever shift) at Week 7
    and measures its downstream impact on profitability, volume, and basket.
    This is the core "pricing lever change impact" analysis.
    """
    pre = txns[(txns["week_number"] <= 6)]
    post = txns[(txns["week_number"] >= 7)]

    # --- Aggregate impact by commodity vs non-commodity ---
    def period_stats(df, label):
        comm = df[df["is_commodity"] == True]
        non_comm = df[df["is_commodity"] == False]
        n_weeks = df["week_number"].nunique()
        return {
            "period": label,
            "weeks": n_weeks,
            "commodity_gp_pct": round(comm["gross_profit_dollars"].sum() / comm["net_sales"].sum(), 4) if comm["net_sales"].sum() > 0 else 0,
            "non_commodity_gp_pct": round(non_comm["gross_profit_dollars"].sum() / non_comm["net_sales"].sum(), 4) if non_comm["net_sales"].sum() > 0 else 0,
            "blended_gp_pct": round(df["gross_profit_dollars"].sum() / df["net_sales"].sum(), 4),
            "commodity_sales_per_wk": round(comm["net_sales"].sum() / n_weeks, 2),
            "non_commodity_sales_per_wk": round(non_comm["net_sales"].sum() / n_weeks, 2),
            "commodity_cases_per_wk": round(comm["cases_ordered"].sum() / n_weeks, 0),
            "non_commodity_cases_per_wk": round(non_comm["cases_ordered"].sum() / n_weeks, 0),
            "commodity_avg_cost": round(comm["unit_cost"].mean(), 2),
            "non_commodity_avg_cost": round(non_comm["unit_cost"].mean(), 2),
            "commodity_avg_price": round(comm["net_price"].mean(), 2),
            "non_commodity_avg_price": round(non_comm["net_price"].mean(), 2),
        }

    pre_stats = period_stats(pre, "Pre-Lever (Wk 1-6)")
    post_stats = period_stats(post, "Post-Lever (Wk 7-16)")

    # --- Customer-level impact ---
    cust_impact = []
    for cid in txns["customer_id"].unique():
        c_pre = pre[(pre["customer_id"] == cid) & (pre["is_commodity"] == True)]
        c_post = post[(post["customer_id"] == cid) & (post["is_commodity"] == True)]
        if len(c_pre) == 0 or len(c_post) == 0:
            continue

        gp_pct_pre = c_pre["gross_profit_dollars"].sum() / c_pre["net_sales"].sum()
        gp_pct_post = c_post["gross_profit_dollars"].sum() / c_post["net_sales"].sum()
        vol_pre = c_pre["cases_ordered"].sum() / 6
        vol_post = c_post["cases_ordered"].sum() / 10

        cust_name = txns[txns["customer_id"] == cid]["customer_name"].iloc[0]
        segment = txns[txns["customer_id"] == cid]["segment"].iloc[0]

        cust_impact.append({
            "customer_id": cid,
            "customer_name": cust_name,
            "segment": segment,
            "commodity_gp_pct_pre": round(gp_pct_pre, 4),
            "commodity_gp_pct_post": round(gp_pct_post, 4),
            "gp_erosion_bps": round((gp_pct_post - gp_pct_pre) * 10000),
            "commodity_cases_per_wk_pre": round(vol_pre, 1),
            "commodity_cases_per_wk_post": round(vol_post, 1),
            "volume_change_pct": round((vol_post - vol_pre) / vol_pre, 4) if vol_pre > 0 else 0,
        })

    cust_impact_df = pd.DataFrame(cust_impact).sort_values("gp_erosion_bps")

    # --- Category-level impact (commodity only) ---
    cat_impact = []
    for cat in txns[txns["is_commodity"] == True]["category"].unique():
        c_pre = pre[(pre["category"] == cat) & (pre["is_commodity"] == True)]
        c_post = post[(post["category"] == cat) & (post["is_commodity"] == True)]
        if len(c_pre) == 0 or len(c_post) == 0:
            continue

        gp_pre = c_pre["gross_profit_dollars"].sum() / 6
        gp_post = c_post["gross_profit_dollars"].sum() / 10
        cost_pre = c_pre["unit_cost"].mean()
        cost_post = c_post["unit_cost"].mean()

        cat_impact.append({
            "category": cat,
            "weekly_gp_pre": round(gp_pre, 2),
            "weekly_gp_post": round(gp_post, 2),
            "gp_delta_per_week": round(gp_post - gp_pre, 2),
            "avg_cost_increase_pct": round((cost_post - cost_pre) / cost_pre, 4),
        })

    cat_impact_df = pd.DataFrame(cat_impact).sort_values("gp_delta_per_week")

    return {
        "pre_period": pre_stats,
        "post_period": post_stats,
        "customer_impact": cust_impact_df,
        "category_impact": cat_impact_df,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 5: SCENARIO MODELING
# ═══════════════════════════════════════════════════════════════════════════════

def scenario_analysis(txns):
    """
    Models three pricing scenarios for commodity pass-through:
    A) Full pass-through (100% cost increase passed to customer)
    B) Targeted overrides by segment (differentiated approach)
    C) Temporary hold with triggers (absorb short-term, plan recovery)
    """
    recent = txns[txns["week_number"] >= 13]
    commodity = recent[recent["is_commodity"] == True]

    # Baseline
    base_sales = commodity["net_sales"].sum()
    base_cogs = commodity["cogs"].sum()
    base_gp = commodity["gross_profit_dollars"].sum()
    base_cases = commodity["cases_ordered"].sum()
    base_gp_pct = base_gp / base_sales

    scenarios = []

    # Scenario A: Full Pass-Through
    avg_cost_increase = 0.04  # ~4% commodity cost increase
    new_price_a = commodity["net_price"] * (1 + avg_cost_increase)
    vol_loss_a = 0.06  # 6% volume loss from full pass-through
    projected_cases_a = base_cases * (1 - vol_loss_a)
    projected_sales_a = (new_price_a * commodity["cases_ordered"] * (1 - vol_loss_a)).sum()
    projected_cogs_a = base_cogs * (1 - vol_loss_a)
    projected_gp_a = projected_sales_a - projected_cogs_a

    scenarios.append({
        "scenario": "A: Full Pass-Through",
        "description": "Pass 100% of cost increase to all customers. Simple, transparent, but risks volume loss in price-sensitive segments.",
        "price_action": f"+{avg_cost_increase:.1%} across all commodity items",
        "projected_sales": round(projected_sales_a, 2),
        "projected_cogs": round(projected_cogs_a, 2),
        "projected_gp": round(projected_gp_a, 2),
        "projected_gp_pct": round(projected_gp_a / projected_sales_a, 4),
        "projected_volume": round(projected_cases_a, 0),
        "volume_change_pct": round(-vol_loss_a, 4),
        "gp_vs_baseline": round(projected_gp_a - base_gp, 2),
        "risk_level": "Medium",
        "best_for": "Segments with low price sensitivity (Healthcare, Senior Living)",
    })

    # Scenario B: Targeted by Segment
    seg_rates = {
        "Healthcare": 0.035,
        "Senior Living": 0.030,
        "Restaurant/FSR": 0.020,
        "K-12 Education": 0.015,
        "Corrections/Government": 0.010,
    }
    seg_vol_loss = {
        "Healthcare": 0.02,
        "Senior Living": 0.03,
        "Restaurant/FSR": 0.05,
        "K-12 Education": 0.08,
        "Corrections/Government": 0.10,
    }

    proj_sales_b = 0
    proj_cogs_b = 0
    proj_cases_b = 0
    for seg, rate in seg_rates.items():
        seg_data = commodity[commodity["segment"] == seg]
        if len(seg_data) == 0:
            continue
        new_prices = seg_data["net_price"] * (1 + rate)
        vl = seg_vol_loss[seg]
        seg_proj_sales = (new_prices * seg_data["cases_ordered"] * (1 - vl)).sum()
        seg_proj_cogs = seg_data["cogs"].sum() * (1 - vl)
        seg_proj_cases = seg_data["cases_ordered"].sum() * (1 - vl)
        proj_sales_b += seg_proj_sales
        proj_cogs_b += seg_proj_cogs
        proj_cases_b += seg_proj_cases

    proj_gp_b = proj_sales_b - proj_cogs_b
    total_vol_loss_b = (base_cases - proj_cases_b) / base_cases

    scenarios.append({
        "scenario": "B: Targeted Overrides",
        "description": "Differentiated pass-through by segment. Higher recovery from low-sensitivity accounts, protect volume with price-sensitive segments.",
        "price_action": "Healthcare +3.5%, Senior Living +3.0%, FSR +2.0%, K-12 +1.5%, Gov +1.0%",
        "projected_sales": round(proj_sales_b, 2),
        "projected_cogs": round(proj_cogs_b, 2),
        "projected_gp": round(proj_gp_b, 2),
        "projected_gp_pct": round(proj_gp_b / proj_sales_b, 4) if proj_sales_b > 0 else 0,
        "projected_volume": round(proj_cases_b, 0),
        "volume_change_pct": round(-total_vol_loss_b, 4),
        "gp_vs_baseline": round(proj_gp_b - base_gp, 2),
        "risk_level": "Low-Medium",
        "best_for": "Balanced approach — maximizes GP recovery while managing churn risk",
    })

    # Scenario C: Temporary Hold
    hold_weeks = 4
    vol_gain_c = 0.03  # volume boost from holding price
    projected_cases_c = base_cases * (1 + vol_gain_c)
    projected_sales_c = base_sales * (1 + vol_gain_c)
    projected_cogs_c = base_cogs * (1 + vol_gain_c)
    projected_gp_c = projected_sales_c - projected_cogs_c

    scenarios.append({
        "scenario": "C: Temporary Hold + Trigger Plan",
        "description": f"Absorb cost increase for {hold_weeks} weeks to lock volume. Implement phased increase if commodity index stays elevated past trigger date.",
        "price_action": "No change for 4 weeks; +2.5% if commodity cost persists; +4% at week 8",
        "projected_sales": round(projected_sales_c, 2),
        "projected_cogs": round(projected_cogs_c, 2),
        "projected_gp": round(projected_gp_c, 2),
        "projected_gp_pct": round(projected_gp_c / projected_sales_c, 4) if projected_sales_c > 0 else 0,
        "projected_volume": round(projected_cases_c, 0),
        "volume_change_pct": round(vol_gain_c, 4),
        "gp_vs_baseline": round(projected_gp_c - base_gp, 2),
        "risk_level": "High (short-term GP drag)",
        "best_for": "Competitive defense — protect share during volatile period",
    })

    return scenarios


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 6: DATA INTEGRITY & QA CHECKS
# ═══════════════════════════════════════════════════════════════════════════════

def data_integrity_audit(txns, products):
    """
    Validates pricing data for system integrity issues.
    Catches the kind of errors that can create customer-facing price mistakes.
    """
    issues = []

    # Check 1: Negative margins
    neg_margin = txns[txns["gp_pct"] < 0]
    if len(neg_margin) > 0:
        issues.append({
            "check": "Negative Margin Transactions",
            "severity": "CRITICAL",
            "count": len(neg_margin),
            "detail": f"{len(neg_margin)} transactions with negative GP% detected. "
                      f"Affected products: {neg_margin['product_id'].nunique()}. "
                      f"Total GP$ impact: ${neg_margin['gross_profit_dollars'].sum():,.2f}",
            "action": "Immediate review — likely cost update not reflected in pricing"
        })

    # Check 2: Price below cost
    below_cost = txns[txns["net_price"] < txns["unit_cost"]]
    if len(below_cost) > 0:
        issues.append({
            "check": "Net Price Below Cost",
            "severity": "CRITICAL",
            "count": len(below_cost),
            "detail": f"{len(below_cost)} transactions where net price < unit cost. "
                      f"Revenue leakage: ${abs(below_cost['gross_profit_dollars'].sum()):,.2f}",
            "action": "Escalate to pricing system admin — config error likely"
        })

    # Check 3: Unusually high margins (possible data error)
    high_margin = txns[txns["gp_pct"] > 0.50]
    if len(high_margin) > 0:
        issues.append({
            "check": "Abnormally High Margin (>50%)",
            "severity": "WARNING",
            "count": len(high_margin),
            "detail": f"{len(high_margin)} transactions with GP% > 50%. "
                      f"May indicate stale cost data or pricing system misconfiguration.",
            "action": "Validate cost data freshness for flagged products"
        })

    # Check 4: Missing override justification
    overrides_no_reason = txns[txns["has_override"] == True]
    if len(overrides_no_reason) > 0:
        issues.append({
            "check": "Override Audit Trail",
            "severity": "INFO",
            "count": int(overrides_no_reason["has_override"].sum()),
            "detail": f"{int(overrides_no_reason['has_override'].sum())} active overrides in the last 16 weeks. "
                      f"Override rate: {overrides_no_reason['has_override'].mean():.1%}",
            "action": "Ensure all overrides have documented reason codes and expiry dates"
        })

    # Check 5: Price variance within same product (consistency)
    price_cv = txns.groupby("product_id")["net_price"].agg(["mean", "std"]).reset_index()
    price_cv["cv"] = price_cv["std"] / price_cv["mean"]
    high_variance = price_cv[price_cv["cv"] > 0.15]
    if len(high_variance) > 0:
        issues.append({
            "check": "High Price Variance (CV > 15%)",
            "severity": "WARNING",
            "count": len(high_variance),
            "detail": f"{len(high_variance)} products with coefficient of variation > 15% across customers. "
                      f"May indicate inconsistent pricing tiers or unauthorized discounts.",
            "action": "Review tier assignment and discount governance for flagged SKUs"
        })

    # Check 6: Stale pricing (no change in 8+ weeks)
    recent_prices = txns[txns["week_number"] >= 9].groupby("product_id")["net_price"].std()
    stale = recent_prices[recent_prices < 0.01]
    issues.append({
        "check": "Stale Pricing (No Movement 8+ Weeks)",
        "severity": "INFO",
        "count": len(stale),
        "detail": f"{len(stale)} products show virtually no price movement in the last 8 weeks. "
                  f"May miss cost recovery opportunities.",
        "action": "Cross-reference with commodity index movements"
    })

    return issues


# ═══════════════════════════════════════════════════════════════════════════════
#  MODULE 7: BASKET ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def basket_analysis(txns):
    """
    Analyze customer basket composition and co-purchase patterns
    to assess basket risk of pricing actions.
    """
    recent = txns[txns["week_number"] >= 13]

    # Customer basket breadth
    basket = recent.groupby(["customer_id", "customer_name", "segment"]).agg(
        total_sales=("net_sales", "sum"),
        total_gp=("gross_profit_dollars", "sum"),
        unique_products=("product_id", "nunique"),
        unique_categories=("category", "nunique"),
        total_cases=("cases_ordered", "sum"),
    ).reset_index()

    basket["gp_pct"] = basket["total_gp"] / basket["total_sales"]
    basket["avg_basket_value"] = basket["total_sales"] / 4  # 4 weeks

    # Category concentration per customer
    cat_mix = recent.groupby(["customer_id", "category"])["net_sales"].sum().reset_index()
    cat_mix["total"] = cat_mix.groupby("customer_id")["net_sales"].transform("sum")
    cat_mix["category_share"] = cat_mix["net_sales"] / cat_mix["total"]

    # Top categories by revenue
    top_cats = cat_mix.groupby("category")["net_sales"].sum().sort_values(ascending=False).head(10)

    # Commodity share of basket
    comm_share = recent.groupby("customer_id").apply(
        lambda x: x[x["is_commodity"] == True]["net_sales"].sum() / x["net_sales"].sum()
    ).reset_index()
    comm_share.columns = ["customer_id", "commodity_share"]

    basket = basket.merge(comm_share, on="customer_id", how="left")

    return basket, top_cats


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Loading data...")
    products, customers, txns = load_data()

    print("\n" + "="*70)
    print("  MODULE 1: Weekly Portfolio Summary")
    print("="*70)
    weekly = weekly_portfolio_summary(txns)
    print(weekly[["week_number", "total_net_sales", "total_gp", "gp_pct",
                   "total_cases", "override_rate"]].to_string(index=False))

    print("\n" + "="*70)
    print("  MODULE 2: Margin Bridge (Pre vs Post Cost Increase)")
    print("="*70)
    bridge, cat_bridge = margin_bridge(txns)
    print(f"\nGP/week Period A: ${bridge['gp_per_week_a']:,.2f}")
    print(f"GP/week Period B: ${bridge['gp_per_week_b']:,.2f}")
    print(f"Delta:            ${bridge['delta_gp_per_week']:,.2f}")
    print(f"  Price Effect:   ${bridge['price_effect']:,.2f}")
    print(f"  Cost Effect:    ${bridge['cost_effect']:,.2f}")
    print(f"  Volume Effect:  ${bridge['volume_effect']:,.2f}")
    print(f"  Mix Effect:     ${bridge['mix_effect']:,.2f}")

    print("\n" + "="*70)
    print("  MODULE 3: Override Recommendations")
    print("="*70)
    overrides = generate_override_recommendations(txns)
    print(f"\n{len(overrides)} override recommendations generated")
    if len(overrides) > 0:
        print(f"Total projected annual GP impact: ${overrides['projected_annual_gp_impact'].sum():,.2f}")
        print(f"\nTop 10 by impact:")
        print(overrides[["customer_name", "description", "current_gp_pct",
                         "recommended_price", "confidence", "projected_annual_gp_impact"
                         ]].head(10).to_string(index=False))

    print("\n" + "="*70)
    print("  MODULE 4: Lever Change Impact")
    print("="*70)
    impact = lever_change_impact(txns)
    print(f"\nPre-lever commodity GP%:  {impact['pre_period']['commodity_gp_pct']:.2%}")
    print(f"Post-lever commodity GP%: {impact['post_period']['commodity_gp_pct']:.2%}")
    print(f"Blended GP% Pre:         {impact['pre_period']['blended_gp_pct']:.2%}")
    print(f"Blended GP% Post:        {impact['post_period']['blended_gp_pct']:.2%}")

    print("\n" + "="*70)
    print("  MODULE 5: Scenario Analysis")
    print("="*70)
    scenarios = scenario_analysis(txns)
    for s in scenarios:
        print(f"\n{s['scenario']}")
        print(f"  GP$ Impact vs Baseline: ${s['gp_vs_baseline']:,.2f}")
        print(f"  Projected GP%: {s['projected_gp_pct']:.2%}")
        print(f"  Volume Impact: {s['volume_change_pct']:+.1%}")

    print("\n" + "="*70)
    print("  MODULE 6: Data Integrity Audit")
    print("="*70)
    issues = data_integrity_audit(txns, products)
    for issue in issues:
        print(f"\n[{issue['severity']}] {issue['check']}: {issue['count']} items")
        print(f"  {issue['detail']}")

    print("\n" + "="*70)
    print("  MODULE 7: Basket Analysis")
    print("="*70)
    basket, top_cats = basket_analysis(txns)
    print(f"\nAverage basket breadth: {basket['unique_products'].mean():.0f} products")
    print(f"Average commodity share: {basket['commodity_share'].mean():.1%}")

    # ── Export everything to JSON for dashboard ──
    print("\n\nExporting dashboard data...")

    dashboard_data = {
        "weekly_summary": weekly.to_dict(orient="records"),
        "category_performance": category_performance(txns).to_dict(orient="records"),
        "segment_performance": segment_performance(txns).to_dict(orient="records"),
        "margin_bridge": bridge,
        "category_bridge": cat_bridge.to_dict(orient="records"),
        "override_recommendations": overrides.head(50).to_dict(orient="records") if len(overrides) > 0 else [],
        "override_summary": {
            "total_recommendations": len(overrides),
            "total_annual_gp_impact": round(overrides["projected_annual_gp_impact"].sum(), 2) if len(overrides) > 0 else 0,
            "high_confidence": len(overrides[overrides["confidence"] == "High"]) if len(overrides) > 0 else 0,
            "medium_confidence": len(overrides[overrides["confidence"] == "Medium"]) if len(overrides) > 0 else 0,
            "low_confidence": len(overrides[overrides["confidence"] == "Low"]) if len(overrides) > 0 else 0,
            "by_segment": overrides.groupby("segment")["projected_annual_gp_impact"].sum().round(2).to_dict() if len(overrides) > 0 else {},
            "by_category": overrides.groupby("category")["projected_annual_gp_impact"].sum().round(2).sort_values(ascending=False).head(10).to_dict() if len(overrides) > 0 else {},
        },
        "lever_impact": {
            "pre_period": impact["pre_period"],
            "post_period": impact["post_period"],
            "customer_impact_top": impact["customer_impact"].head(20).to_dict(orient="records"),
            "category_impact": impact["category_impact"].to_dict(orient="records"),
        },
        "scenarios": scenarios,
        "data_integrity": issues,
        "basket_summary": {
            "avg_products_per_customer": round(basket["unique_products"].mean(), 1),
            "avg_categories_per_customer": round(basket["unique_categories"].mean(), 1),
            "avg_commodity_share": round(basket["commodity_share"].mean(), 4),
            "avg_weekly_basket_value": round(basket["avg_basket_value"].mean(), 2),
            "top_categories": top_cats.round(2).to_dict(),
        },
        "metadata": {
            "data_source": "Sysco Arkansas Price Sheet (Contract S000000035 / 4600049774)",
            "analysis_period": "Oct 6, 2025 — Jan 25, 2026 (16 weeks)",
            "products_analyzed": int(txns["product_id"].nunique()),
            "customers_analyzed": int(txns["customer_id"].nunique()),
            "total_transactions": len(txns),
            "total_net_sales": round(txns["net_sales"].sum(), 2),
            "generated_date": "2026-02-10",
        },
    }

    with open("/home/claude/pricing_engine/dashboard_data.json", "w") as f:
        json.dump(dashboard_data, f, indent=2, default=str)

    print("Dashboard data exported to dashboard_data.json")
    print("\nPipeline complete.")
