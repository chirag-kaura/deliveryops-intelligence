"""Streamlit Customer E-Commerce Storefront for the DeliveryOps Cloud Pipeline."""

import json
import socket
import sys
from pathlib import Path
from typing import Any

import polars as pl

from deliveryops.config import get_settings
from deliveryops.observability import check_infrastructure_health
from deliveryops.pipelines import AssetGraphOrchestrator, submit_live_ecommerce_order
from deliveryops.storage import GoldWarehouseLoader, LocalLakeStorage, S3LakeAdapter

STORE_PRODUCTS: list[dict[str, Any]] = [
    {
        "id": "grocery",
        "name": "Express Grocery Essentials Pack",
        "short": "Grocery Pack",
        "price": 649.0,
        "cat": "Groceries",
        "tag": "Fresh · 15-22m",
    },
    {
        "id": "earbuds",
        "name": "Wireless ANC Earbuds Pro",
        "short": "ANC Earbuds",
        "price": 1899.0,
        "cat": "Electronics",
        "tag": "Sealed · 20-30m",
    },
    {
        "id": "biryani",
        "name": "Gourmet Biryani & Drink Combo",
        "short": "Biryani Combo",
        "price": 450.0,
        "cat": "Meals",
        "tag": "Hot Meal · 18-28m",
    },
    {
        "id": "watch",
        "name": "Smart Fitness Watch Series X",
        "short": "Fitness Watch",
        "price": 2499.0,
        "cat": "Electronics",
        "tag": "Express · 20-30m",
    },
]

CATALOG: dict[str, float] = {prod["name"]: float(prod["price"]) for prod in STORE_PRODUCTS}

ZONE_LABELS: dict[str, str] = {
    "BLR-KOR-01": "Koramangala",
    "BLR-HSR-02": "HSR Layout",
    "BLR-IND-03": "Indiranagar",
    "BLR-WHT-04": "Whitefield",
}

CATEGORY_OPTIONS: tuple[str, ...] = ("All", "Groceries", "Electronics", "Meals")

SCENARIO_MAP: dict[str, tuple[float, str]] = {
    "On-Time (24 min · CLEAR)": (24.0, "CLEAR"),
    "Delayed (44 min · HEAVY_RAIN)": (44.0, "HEAVY_RAIN"),
}

CLOUD_TOOL_LINKS: list[tuple[str, str]] = [
    (
        "Postgres :8082",
        "http://127.0.0.1:8082/?pgsql=postgres&username=deliveryops&db=deliveryops&ns=public",
    ),
    ("Redpanda :8081", "http://127.0.0.1:8081/topics"),
    ("MinIO :9001", "http://127.0.0.1:9001"),
    ("Jaeger :16686", "http://127.0.0.1:16686"),
    ("LocalStack :4566", "http://127.0.0.1:4566/deliveryops-cloud-archive"),
    ("API :8001", "http://127.0.0.1:8001/docs"),
]


def calculate_cart_summary(cart: dict[str, int], category: str = "All") -> dict[str, Any]:
    """Filter storefront catalog and compute cart items, item count, and total INR."""
    filtered_products = (
        list(STORE_PRODUCTS)
        if category == "All"
        else [prod for prod in STORE_PRODUCTS if prod["cat"] == category]
    )
    product_by_id = {prod["id"]: prod for prod in STORE_PRODUCTS}
    items: list[dict[str, Any]] = []
    total_items = 0
    total_inr = 0.0

    for prod_id, raw_qty in cart.items():
        qty = int(raw_qty)
        if qty <= 0 or prod_id not in product_by_id:
            continue
        prod = product_by_id[prod_id]
        line_total = round(float(prod["price"]) * qty, 2)
        total_items += qty
        total_inr += line_total
        items.append(
            {
                "id": prod_id,
                "name": prod["name"],
                "short": prod["short"],
                "price": float(prod["price"]),
                "qty": qty,
                "line_total": line_total,
            }
        )

    primary_product_name = items[0]["name"] if items else STORE_PRODUCTS[0]["name"]
    return {
        "filtered_products": filtered_products,
        "items": items,
        "total_items": total_items,
        "total_inr": round(total_inr, 2),
        "primary_product_name": primary_product_name,
    }


def _probe_port(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def _load_latest_traces(lake_dir: Path) -> list[dict[str, Any]]:
    traces_dir = lake_dir / "traces"
    if not traces_dir.exists():
        return []
    files = sorted(traces_dir.glob("otel_*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return []
    data = json.loads(files[-1].read_text(encoding="utf-8"))
    return list(data.get("spans", []))


def _trigger_container_sync() -> list[str]:
    """Run live container sync so Postgres, Redpanda, MinIO, Jaeger, and LocalStack update."""
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.sync_live_containers import sync_all_containers

    return sync_all_containers(wait_timeout=1.0)


def load_dashboard_snapshot(lake_dir: Path | str | None = None) -> dict[str, Any]:
    """Load Bronze DLQ, Silver events, Gold KPIs, Warehouse summary, Asset DAG, and Infra state."""
    settings = get_settings()
    target_dir = Path(lake_dir) if lake_dir is not None else settings.local_lake_dir
    lake = LocalLakeStorage(base_dir=target_dir)
    s3 = S3LakeAdapter(local_lake=lake, settings=settings)
    warehouse = GoldWarehouseLoader(lake=lake)

    zone_files = lake.list_parquet_files("gold", "zone_kpi_daily")
    courier_files = lake.list_parquet_files("gold", "courier_performance")
    silver_files = lake.list_parquet_files("silver", "events")
    dlq_records = lake.read_jsonl_dataset("bronze", "dlq")

    zone_df = pl.read_parquet([str(p) for p in zone_files]) if zone_files else pl.DataFrame()
    courier_df = (
        pl.read_parquet([str(p) for p in courier_files]) if courier_files else pl.DataFrame()
    )
    silver_df = (
        pl.read_parquet([str(p) for p in silver_files]) if silver_files else pl.DataFrame()
    )

    total_valid = silver_df.height if not silver_df.is_empty() else 0
    sla_breaches = (
        int(silver_df["is_sla_breached"].sum())
        if not silver_df.is_empty() and "is_sla_breached" in silver_df.columns
        else 0
    )
    breach_rate_pct = round((100.0 * sla_breaches / total_valid), 2) if total_valid > 0 else 0.0

    s3_uris = [s3.local_path_to_s3_uri(p, "silver", "events") for p in silver_files] + [
        s3.local_path_to_s3_uri(p, "gold", "zone_kpi_daily") for p in zone_files
    ]
    infra = check_infrastructure_health(settings=settings, timeout_seconds=0.25).model_dump()
    infra["localstack_reachable"] = _probe_port(4566)

    return {
        "total_valid_events": total_valid,
        "total_dlq_records": len(dlq_records),
        "sla_breach_count": sla_breaches,
        "sla_breach_rate_pct": breach_rate_pct,
        "zone_kpis": zone_df.to_dicts() if not zone_df.is_empty() else [],
        "courier_performance": courier_df.to_dicts() if not courier_df.is_empty() else [],
        "silver_events": silver_df.to_dicts() if not silver_df.is_empty() else [],
        "dlq_records": dlq_records,
        "asset_lineage": AssetGraphOrchestrator.get_asset_lineage(),
        "warehouse_summary": warehouse.query_summary(),
        "infra_status": infra,
        "s3_uris": s3_uris,
        "latest_traces": _load_latest_traces(target_dir),
    }


def _build_stepper_html(active_step: int, bar_color: str) -> str:
    """Build compact 4-stage delivery tracking stepper HTML."""
    stages = ("1. Placed", "2. Assigned", "3. Picked Up", "4. Delivered")
    cells: list[str] = []
    for idx, label in enumerate(stages):
        is_lit = active_step >= idx >= 0
        fill = bar_color if is_lit else "#64748b44"
        text_weight = "600" if is_lit else "400"
        text_opacity = "1" if is_lit else "0.7"
        cells.append(
            f"<div style='flex:1; text-align:center;'>"
            f"<div style='height:6px; border-radius:999px; background:{fill}; "
            f"margin-bottom:4px;'></div>"
            f"<span style='font-size:0.78rem; font-weight:{text_weight}; "
            f"opacity:{text_opacity};'>{label}</span></div>"
        )
    return (
        "<div style='display:flex; gap:8px; margin:6px 0 10px 0;'>"
        + "".join(cells)
        + "</div>"
    )


def _render_header(st: Any) -> str:
    """Render storefront brand header, category pills, and delivery zone selector."""
    hdr_brand, hdr_cat, hdr_zone = st.columns([3.2, 4.3, 2.5])
    with hdr_brand:
        st.markdown(
            "<div style='display:flex; align-items:center; gap:10px; padding-top:4px;'>"
            "<div style='width:32px; height:32px; border-radius:8px; background:#ff4b4b; "
            "color:#fff; display:flex; align-items:center; justify-content:center; "
            "font-weight:700; font-size:1rem;'>D</div>"
            "<span style='font-size:1.3rem; font-weight:700;'>DeliveryOps Mart</span>"
            "<span style='padding:2px 8px; border-radius:6px; background:#64748b26; "
            "font-size:0.78rem; font-weight:600;'>35m SLA Express</span>"
            "</div>",
            unsafe_allow_html=True,
        )

    with hdr_cat:
        cat_cols = st.columns(len(CATEGORY_OPTIONS))
        for idx, cat in enumerate(CATEGORY_OPTIONS):
            btn_type = "primary" if st.session_state["active_cat"] == cat else "secondary"
            if cat_cols[idx].button(
                cat, key=f"cat_{cat}", type=btn_type, use_container_width=True
            ):
                st.session_state["active_cat"] = cat
                st.rerun()

    with hdr_zone:
        zone_id = st.selectbox(
            "Deliver to",
            list(ZONE_LABELS.keys()),
            format_func=lambda zid: f"{ZONE_LABELS[zid]} ({zid})",
            label_visibility="collapsed",
        )
    return str(zone_id)


def _render_catalog_column(
    st: Any,
    zone_id: str,
    products: list[dict[str, Any]],
    cart: dict[str, int],
) -> None:
    """Render featured e-commerce product cards on the left column."""
    cat_l, cat_r = st.columns([3, 2])
    cat_l.markdown("**FEATURED STOREFRONT CATALOG**")
    cat_r.markdown(
        f"<div style='text-align:right; opacity:0.75; font-size:0.85rem;'>"
        f"Hub: {ZONE_LABELS[zone_id]} · Free delivery</div>",
        unsafe_allow_html=True,
    )
    for row_start in range(0, len(products), 2):
        row_cols = st.columns(2)
        for offset, prod in enumerate(products[row_start : row_start + 2]):
            with row_cols[offset], st.container(border=True):
                top_l, top_r = st.columns([2.8, 1.4])
                top_l.caption(prod["cat"])
                top_r.markdown(
                    f"<div style='text-align:right; font-weight:700; font-size:1rem;'>"
                    f"₹{prod['price']:,.0f}</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(f"**{prod['name']}**")
                bot_l, bot_r = st.columns([1.4, 1])
                bot_l.caption(prod["tag"])
                if bot_r.button(
                    "+ Add",
                    key=f"add_{prod['id']}",
                    type="primary",
                    use_container_width=True,
                ):
                    cart[prod["id"]] = int(cart.get(prod["id"], 0)) + 1
                    st.rerun()


def _render_cart_panel(
    st: Any,
    zone_id: str,
    summary: dict[str, Any],
    cart: dict[str, int],
    settings: Any,
) -> dict[str, Any]:
    """Render shopping cart controls, dispatch selectors, and checkout actions."""
    with st.container(border=True):
        c_hdr_l, c_hdr_r = st.columns([2, 1])
        c_hdr_l.markdown(f"**Your Cart ({summary['total_items']} items)**")
        c_hdr_r.markdown(
            f"<div style='text-align:right; font-weight:700; font-size:1.05rem;'>"
            f"₹{summary['total_inr']:,.0f}</div>",
            unsafe_allow_html=True,
        )

        if not summary["items"]:
            st.caption("Cart is empty — add items from the catalog on the left.")
        else:
            for item in summary["items"]:
                ic_name, ic_dec, ic_qty, ic_inc = st.columns([3.2, 0.9, 0.8, 0.9])
                ic_name.markdown(f"{item['short']} · **₹{item['line_total']:,.0f}**")
                if ic_dec.button("-", key=f"dec_{item['id']}", use_container_width=True):
                    if item["qty"] > 1:
                        cart[item["id"]] = item["qty"] - 1
                    else:
                        cart.pop(item["id"], None)
                    st.rerun()
                ic_qty.markdown(
                    f"<div style='text-align:center; padding-top:0.35rem; font-weight:600;'>"
                    f"{item['qty']}</div>",
                    unsafe_allow_html=True,
                )
                if ic_inc.button("+", key=f"inc_{item['id']}", use_container_width=True):
                    cart[item["id"]] = item["qty"] + 1
                    st.rerun()

        opt_l, opt_r = st.columns(2)
        vehicle = opt_l.selectbox("Vehicle", ["EV_VAN", "BIKE", "SCOOTER", "CAR"])
        scenario_label = opt_r.selectbox("Trip Scenario", list(SCENARIO_MAP.keys()))

        btn_l, btn_r = st.columns(2)
        place_clicked = btn_l.button(
            "Place Order & Pay",
            type="primary",
            use_container_width=True,
        )
        corrupt_clicked = btn_r.button(
            "Simulate Bad Order",
            use_container_width=True,
        )

        if place_clicked or corrupt_clicked:
            is_corrupt = bool(corrupt_clicked)
            cart_auto_filled = False
            if not summary["items"]:
                cart[STORE_PRODUCTS[0]["id"]] = 1
                summary = calculate_cart_summary(cart, st.session_state["active_cat"])
                cart_auto_filled = True

            duration, weather = SCENARIO_MAP[scenario_label]
            with st.status("Processing Order & Syncing Cloud Stack...", expanded=False):
                st.write("Streaming order lifecycle through Medallion pipeline...")
                receipt = submit_live_ecommerce_order(
                    product_name=summary["primary_product_name"],
                    order_value_inr=summary["total_inr"] or float(STORE_PRODUCTS[0]["price"]),
                    zone_id=zone_id,
                    vehicle_type=vehicle,
                    weather_condition=weather,
                    actual_duration_minutes=duration,
                    simulate_corrupt=is_corrupt,
                    lake_dir=settings.local_lake_dir,
                    settings=settings,
                )
                st.write("Syncing Postgres, Redpanda, MinIO, Jaeger & LocalStack...")
                receipt["sync_logs"] = _trigger_container_sync()
                receipt["weather_condition"] = weather
                receipt["vehicle_type"] = vehicle
                st.session_state["last_receipt"] = receipt

            if cart_auto_filled:
                st.rerun()

    return summary


def _render_order_tracker(st: Any, receipt: dict[str, Any] | None) -> None:
    """Render live 4-stage delivery stepper and checkout receipt / DLQ status."""
    with st.container(border=True):
        if receipt is None:
            badge_label, badge_bg = "Ready to Order", "#64748b33"
            active_step, bar_color = -1, "#64748b"
            banner_html = (
                "Add items in the catalog and click <strong>Place Order &amp; Pay</strong> "
                "to watch real-time dispatch tracking, or test DLQ validation."
            )
        elif receipt["simulate_corrupt"]:
            badge_label, badge_bg = "Blocked by DLQ", "#ef4444"
            active_step, bar_color = -1, "#ef4444"
            banner_html = (
                f"<div style='font-weight:700; color:#ef4444;'>"
                f"Pydantic v2 Rejected Corrupt Payload ({receipt['order_id']})</div>"
                "<div style='margin-top:4px; opacity:0.85;'>"
                "<code>courier_id=None</code> &amp; <code>order_value_inr=-500.0</code> "
                "routed to <code>wh_dlq_audit</code> &amp; "
                "<code>deliveryops.events.dlq</code>.</div>"
            )
        else:
            is_breach = bool(receipt["sla_breached"])
            badge_label = "Delivered (SLA Breach)" if is_breach else "Delivered On-Time"
            badge_bg = "#f59e0b" if is_breach else "#10b981"
            active_step, bar_color = 3, badge_bg
            weather = receipt.get("weather_condition", "CLEAR")
            duration = int(receipt["actual_duration_minutes"])
            trace_id = receipt.get("trace_id") or "local-span"
            banner_html = (
                f"<div style='display:flex; justify-content:space-between; font-weight:600;'>"
                f"<span>{receipt['order_id']} · ₹{receipt['order_value_inr']:,.0f}</span>"
                f"<span>{duration}m ({weather})</span></div>"
                f"<div style='margin-top:4px; opacity:0.85;'>"
                f"4 events streamed to Postgres, Redpanda, MinIO &amp; LocalStack · "
                f"trace <code>{trace_id}</code></div>"
            )

        pill_text = "#ffffff" if receipt is not None else "inherit"
        st.markdown(
            "<div style='display:flex; justify-content:space-between; align-items:center;'>"
            "<span style='font-weight:600;'>Live Order Status</span>"
            f"<span style='padding:2px 10px; border-radius:6px; background:{badge_bg}; "
            f"color:{pill_text}; font-size:0.78rem; font-weight:600;'>{badge_label}</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        st.markdown(_build_stepper_html(active_step, bar_color), unsafe_allow_html=True)
        st.markdown(
            f"<div style='border:1px solid #64748b44; border-radius:8px; "
            f"padding:10px 12px; font-size:0.85rem;'>{banner_html}</div>",
            unsafe_allow_html=True,
        )


def _render_footer(st: Any) -> None:
    """Render slim footer strip with direct links to external cloud service UIs."""
    st.divider()
    cols = st.columns([1.2] + [1.0] * len(CLOUD_TOOL_LINKS))
    cols[0].caption("**Live Cloud Tabs:**")
    for idx, (label, url) in enumerate(CLOUD_TOOL_LINKS, start=1):
        cols[idx].link_button(label, url, use_container_width=True)


def render_dashboard() -> None:
    """Render the interactive DeliveryOps Mart customer storefront UI."""
    import streamlit as st

    st.set_page_config(page_title="DeliveryOps Mart — 35m SLA Express", layout="wide")
    st.session_state.setdefault("cart", {"grocery": 1})
    st.session_state.setdefault("active_cat", "All")

    settings = get_settings()
    zone_id = _render_header(st)
    st.divider()

    cart: dict[str, int] = st.session_state["cart"]
    summary = calculate_cart_summary(cart, st.session_state["active_cat"])

    left_col, right_col = st.columns([7, 5], gap="large")
    with left_col:
        _render_catalog_column(st, zone_id, summary["filtered_products"], cart)
    with right_col:
        _render_cart_panel(st, zone_id, summary, cart, settings)
        _render_order_tracker(st, st.session_state.get("last_receipt"))

    _render_footer(st)


if __name__ == "__main__":
    render_dashboard()
