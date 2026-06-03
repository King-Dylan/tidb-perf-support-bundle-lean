#!/usr/bin/env python3
"""Shared optimized physical/query configuration for the benchmark scripts."""

from __future__ import annotations


PROD180_PREAGG_BUNDLES = {
    "group_a_bundle_017",
    "group_a_bundle_018",
    "group_a_bundle_019",
    "group_a_bundle_020",
    "group_b_bundle_017",
    "group_b_bundle_018",
    "group_b_bundle_019",
    "group_b_bundle_020",
    "group_c_bundle_022",
    "group_c_bundle_023",
    "group_c_bundle_024",
    "group_c_bundle_025",
}


EXACT_SERVING_BUNDLES = (
    {f"group_a_bundle_{index:03d}" for index in range(1, 21)}
    | {f"group_b_bundle_{index:03d}" for index in range(1, 21)}
    | {f"group_c_bundle_{index:03d}" for index in range(1, 26)}
)


OPTIMIZED_INDEXES = {
    "pmt_txn_fact": {
        "idx_pmt_merchant_runtime_cov": (
            "merchant_account_number",
            "event_date",
            "amount",
            "mt_gateway",
            "card_type",
            "entry_method",
            "transaction_type",
        ),
        "idx_pmt_card_runtime_cov": (
            "card_holder_number_sha512",
            "event_date",
            "amount",
            "mt_gateway",
            "card_type",
            "entry_method",
            "mt_avs_street_match",
            "transaction_type",
        ),
        "idx_pmt_routing_runtime_cov": (
            "check_bank_routing_number",
            "event_date",
            "amount",
            "mt_gateway",
            "card_type",
            "entry_method",
            "transaction_type",
        ),
        "idx_pmt_routing_account_runtime_cov": (
            "check_bank_routing_number",
            "check_bank_account_number_sha512",
            "event_date",
            "amount",
            "mt_gateway",
            "card_type",
            "entry_method",
        ),
        "idx_pmt_join_runtime_cov": (
            "parsed_interaction_id",
            "event_date",
            "amount",
            "merchant_account_number",
            "card_holder_number_sha512",
            "card_type",
            "entry_method",
            "mt_gateway",
            "check_bank_routing_number",
            "transaction_type",
        ),
        "idx_pmt_merchant_c_join_cov": (
            "merchant_account_number",
            "event_date",
            "parsed_interaction_id",
            "transaction_type",
        ),
        "idx_pmt_card_c_join_cov": (
            "card_holder_number_sha512",
            "event_date",
            "parsed_interaction_id",
            "transaction_type",
        ),
        "idx_pmt_routing_acct_c_join_cov": (
            "check_bank_routing_number",
            "check_bank_account_number_sha512",
            "event_date",
            "parsed_interaction_id",
            "transaction_type",
        ),
    },
    "deviceprofile_fact": {
        "idx_dev_exact_runtime_cov": (
            "exact_id",
            "jms_timestamp",
            "interaction_id",
            "agent_type",
            "agent_os",
            "browser_language",
            "device_fingerprint_score",
            "device_score",
            "device_worst_score",
            "input_ip",
            "input_ip_score",
            "proxy_ip",
            "smart_id",
            "true_ip",
            "true_ip_score",
        ),
        "idx_dev_smart_runtime_cov": (
            "smart_id",
            "jms_timestamp",
            "interaction_id",
            "agent_type",
            "agent_os",
            "request_result",
            "business_transaction",
            "device_fingerprint_score",
            "device_score",
            "device_worst_score",
            "exact_id",
            "input_ip",
            "input_ip_score",
            "proxy_ip",
            "true_ip",
            "true_ip_score",
        ),
        "idx_dev_input_runtime_cov": (
            "input_ip",
            "jms_timestamp",
            "interaction_id",
            "agent_type",
            "device_fingerprint_score",
            "device_score",
            "device_worst_score",
            "exact_id",
            "input_ip_score",
            "smart_id",
            "true_ip",
            "true_ip_score",
        ),
        "idx_dev_true_runtime_cov": (
            "true_ip",
            "jms_timestamp",
            "interaction_id",
            "agent_type",
            "device_fingerprint_score",
            "device_score",
            "device_worst_score",
            "exact_id",
            "input_ip",
            "input_ip_score",
            "proxy_ip",
            "smart_id",
            "true_ip_score",
        ),
    },
}


def index_column_sql(columns: tuple[str, ...]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def create_index_sql(table: str, index_name: str, columns: tuple[str, ...]) -> str:
    return f"CREATE INDEX `{index_name}` ON `{table}` ({index_column_sql(columns)})"
