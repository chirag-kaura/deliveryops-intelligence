"""Unit tests for the live interactive E-Commerce checkout lifecycle runner."""

from pathlib import Path

from deliveryops.dashboard.app import calculate_cart_summary
from deliveryops.pipelines import submit_live_ecommerce_order


def test_submit_live_ecommerce_order_happy_and_dlq_paths(tmp_path: Path) -> None:
    happy = submit_live_ecommerce_order(
        product_name="Express Grocery Essentials Pack",
        order_value_inr=649.0,
        zone_id="BLR-KOR-01",
        vehicle_type="EV_VAN",
        weather_condition="CLEAR",
        actual_duration_minutes=24.0,
        simulate_corrupt=False,
        lake_dir=tmp_path,
    )
    assert happy["order_id"].startswith("ORD-LIVE-")
    assert happy["valid_events_count"] == 4
    assert happy["dlq_count"] == 0
    assert happy["silver_total_rows"] == 4
    assert happy["sla_breached"] is False
    assert all(happy["dbt_tests_passed"].values())
    assert happy["silver_s3_uri"].startswith("s3://deliveryops-silver/events/dt=")

    corrupt = submit_live_ecommerce_order(
        product_name="Express Grocery Essentials Pack",
        order_value_inr=649.0,
        zone_id="BLR-KOR-01",
        vehicle_type="BIKE",
        weather_condition="RAIN",
        actual_duration_minutes=42.0,
        simulate_corrupt=True,
        lake_dir=tmp_path,
    )
    assert corrupt["valid_events_count"] == 0
    assert corrupt["dlq_count"] == 1
    assert corrupt["warehouse_tables"]["wh_dlq_audit"] == 1


def test_storefront_cart_summary_and_filtering() -> None:
    cart = {"grocery": 2, "earbuds": 1, "unknown": 5, "biryani": 0}

    all_summary = calculate_cart_summary(cart, category="All")
    assert len(all_summary["filtered_products"]) == 4
    assert all_summary["total_items"] == 3
    assert all_summary["total_inr"] == 3197.0
    assert all_summary["primary_product_name"] == "Express Grocery Essentials Pack"

    elec_summary = calculate_cart_summary(cart, category="Electronics")
    assert len(elec_summary["filtered_products"]) == 2
    assert {p["cat"] for p in elec_summary["filtered_products"]} == {"Electronics"}

    meals_summary = calculate_cart_summary({}, category="Meals")
    assert len(meals_summary["filtered_products"]) == 1
    assert meals_summary["filtered_products"][0]["id"] == "biryani"
    assert meals_summary["total_items"] == 0
    assert meals_summary["total_inr"] == 0.0
    assert meals_summary["primary_product_name"] == "Express Grocery Essentials Pack"
