#!/usr/bin/env python3 -u
"""
INTUIT RISK POC — Schema Setup
Creates query-compatible source tables for the Intuit risk POC.

Tables:
  1. pmt_txn_fact        — Payment transactions
  2. deviceprofile_fact  — Device fingerprinting
"""

import sys
sys.stdout.reconfigure(line_buffering=True)

import os
import pymysql
import time
from datetime import date, datetime, timedelta, timezone
from lib.db_config import get_db_config

PARTITION_GRAIN = os.environ.get("INTUIT_PARTITION_GRAIN", "monthly").lower()
if PARTITION_GRAIN not in {"daily", "weekly", "monthly", "none"}:
    print("ERROR: INTUIT_PARTITION_GRAIN must be daily, weekly, monthly, or none")
    sys.exit(1)

ENABLE_TIFLASH = os.environ.get("INTUIT_SETUP_TIFLASH", "1").lower() not in {
    "0",
    "false",
    "no",
}


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _next_month(d: date) -> date:
    return date(d.year + (d.month // 12), (d.month % 12) + 1, 1)


def _partition_boundaries(start: date, end: date, grain: str) -> list[date]:
    if grain == "none":
        return []
    boundaries: list[date] = []
    cursor = _month_start(start) if grain == "monthly" else start
    while cursor <= end:
        if cursor > start:
            boundaries.append(cursor)
        if grain == "monthly":
            cursor = _next_month(cursor)
        elif grain == "weekly":
            cursor = cursor + timedelta(days=7)
        else:
            cursor = cursor + timedelta(days=1)
    boundaries.append(cursor)
    return boundaries


def _epoch_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _pmt_partition_clause() -> str:
    boundaries = _partition_boundaries(date(2025, 10, 1), date(2026, 5, 20), PARTITION_GRAIN)
    if not boundaries:
        return ""
    parts = [f"    PARTITION p{d:%Y%m%d} VALUES LESS THAN ({_epoch_ms(d)})" for d in boundaries]
    parts.append("    PARTITION pmax VALUES LESS THAN (MAXVALUE)")
    return "PARTITION BY RANGE (event_date) (\n" + ",\n".join(parts) + "\n)"


def _device_partition_clause() -> str:
    boundaries = _partition_boundaries(date(2025, 10, 1), date(2026, 5, 20), PARTITION_GRAIN)
    if not boundaries:
        return ""
    parts = [f"    PARTITION p{d:%Y%m%d} VALUES LESS THAN ('{d:%Y-%m-%d} 00:00:00')" for d in boundaries]
    parts.append("    PARTITION pmax VALUES LESS THAN (MAXVALUE)")
    return "PARTITION BY RANGE COLUMNS (jms_timestamp) (\n" + ",\n".join(parts) + "\n)"


PMT_PARTITION_CLAUSE = _pmt_partition_clause()
DEVICE_PARTITION_CLAUSE = _device_partition_clause()
ROW_ID_SHARD_CLAUSE = os.environ.get("INTUIT_ENABLE_ROW_ID_SHARDING", "1").lower()
ROW_ID_SHARD_BITS = os.environ.get("INTUIT_SHARD_ROW_ID_BITS", "4")
ROW_ID_PRE_SPLIT_REGIONS = os.environ.get("INTUIT_PRE_SPLIT_REGIONS", "3")
ROW_ID_TABLE_OPTIONS = (
    f"SHARD_ROW_ID_BITS = {ROW_ID_SHARD_BITS} PRE_SPLIT_REGIONS = {ROW_ID_PRE_SPLIT_REGIONS}"
    if ROW_ID_SHARD_CLAUSE not in {"0", "false", "no"}
    else ""
)

TABLES = [
    # =========================================================================
    # TABLE 1: pmt_txn_fact
    # Source: ihub-cassandra-sync Kafka topic (35 TPS, 50 GB/day)
    # Payment transaction events — the money flow.
    # Replaces: payment-serving source for the fresh rebuild
    # =========================================================================
    """
    DROP TABLE IF EXISTS pmt_txn_fact
    """,
    f"""
    CREATE TABLE pmt_txn_fact (
        merchant_account_number         BIGINT         DEFAULT NULL COMMENT 'PaymentTransactionSource.accountNumber',
        authorization_date              DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.authorizationDate',
        batch_date                      DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.batchDate',
        transaction_id                  VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.transactionId',
        realm_id                        VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.realmId',
        invoice_number                  VARCHAR(64)    NOT NULL COMMENT 'PaymentTransactionSource.invoiceNumber',
        mas_transaction_netted          INT            DEFAULT NULL COMMENT 'PaymentTransactionSource.masTransactionNetted',
        transaction_status_id           INT            DEFAULT NULL COMMENT 'PaymentTransactionSource.transactionStatusId',
        mt_result_code                  INT            DEFAULT NULL COMMENT 'PaymentTransactionSource.mtResultCode',
        amount                          DECIMAL(15,2)  DEFAULT NULL COMMENT 'PaymentTransactionSource.amount',
        authorization_code              VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.authorizationCode',
        cutoff_date                     DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.cutoffDate',
        transaction_type                VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.transactionType',
        mt_invoice_id                   VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.mtInvoiceId',
        mt_gateway                      VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.mtGateway',
        entry_method                    VARCHAR(32)    DEFAULT NULL COMMENT 'PaymentTransactionSource.entryMethod',
        currency_code                   VARCHAR(16)    DEFAULT NULL COMMENT 'PaymentTransactionSource.currencyCode',
        number_type_code                VARCHAR(32)    DEFAULT NULL COMMENT 'PaymentTransactionSource.numberTypeCode',
        deposit_id                      VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.depositId',
        is_ready_for_risk               DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.isReadyForRisk',
        transaction_date                DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.transactionDate',
        event_date                      BIGINT         NOT NULL COMMENT 'Query-facing event timestamp in epoch millis',
        mt_avs_street_match             VARCHAR(16)    DEFAULT NULL COMMENT 'PaymentTransactionSource.mtAvsStreetMatch',
        application_type                VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.applicationType',
        pts_intuit_qual_code            VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.ptsIntuitQualCode',
        hk_modified                     DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.hkModified',
        pos_entry_mode_code             VARCHAR(32)    DEFAULT NULL COMMENT 'PaymentTransactionSource.posEntryMode',
        mt_vendor_result_code           VARCHAR(64)    DEFAULT NULL COMMENT 'PaymentTransactionSource.mtVendorResultCode',
        mt_response_message             VARCHAR(256)   DEFAULT NULL COMMENT 'PaymentTransactionSource.mtResponseMessage',
        mt_card_country_code            VARCHAR(16)    DEFAULT NULL COMMENT 'PaymentTransactionSource.mtCardCountryCode',
        mt_avs_zip_match                VARCHAR(16)    DEFAULT NULL COMMENT 'PaymentTransactionSource.mtAvsZipMatch',
        mt_card_holder_name             VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.mtCardHolderName',
        mt_ip_address                   VARCHAR(45)    DEFAULT NULL COMMENT 'PaymentTransactionSource.mtIpAddress',
        card_holder_number_sha512       VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.cardHolderNumberSha512',
        card_number_left                VARCHAR(32)    DEFAULT NULL COMMENT 'PaymentTransactionSource.cardNumberLeft',
        check_bank_routing_number       VARCHAR(32)    DEFAULT NULL COMMENT 'PaymentTransactionSource.checkBankRoutingNumber',
        check_bank_account_number_sha512 VARCHAR(128)  DEFAULT NULL COMMENT 'PaymentTransactionSource.checkBankAccountNumberSha512',
        card_type                       VARCHAR(32)    DEFAULT NULL COMMENT 'PaymentTransactionSource.cardType',
        risk_profile_token              VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.riskProfileToken',
        parsed_interaction_id           VARCHAR(64)    DEFAULT NULL COMMENT 'Physical join key parsed from risk_profile_token suffix for Group C',
        interaction_id                  VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.interactionId',
        session_id                      VARCHAR(128)   DEFAULT NULL COMMENT 'PaymentTransactionSource.sessionId',
        is_swiped                       BOOLEAN        DEFAULT NULL COMMENT 'PaymentTransactionSource.isSwiped',
        orig_batch_date                 DATETIME       DEFAULT NULL COMMENT 'PaymentTransactionSource.origBatchDate',
        hk_source_id                    DECIMAL(20,0)  DEFAULT NULL COMMENT 'PaymentTransactionSource.hkSourceId',

        PRIMARY KEY (invoice_number, event_date) NONCLUSTERED,
        INDEX idx_merchant_account_number_event_date (merchant_account_number, event_date),
        INDEX idx_card_holder_number_sha512_event_date (card_holder_number_sha512, event_date),
        INDEX idx_check_bank_routing_number_event_date (check_bank_routing_number, event_date),
        INDEX idx_check_routing_account_event (check_bank_routing_number, check_bank_account_number_sha512, event_date),
        INDEX idx_risk_profile_token   (risk_profile_token),
        INDEX idx_parsed_interaction_id_event_date (parsed_interaction_id, event_date),
        INDEX idx_interaction_id       (interaction_id),
        INDEX idx_session_id           (session_id),
        INDEX idx_event_date           (event_date),
        INDEX idx_pmt_merchant_runtime_cov (merchant_account_number, event_date, amount, mt_gateway, card_type, entry_method, transaction_type),
        INDEX idx_pmt_card_runtime_cov (card_holder_number_sha512, event_date, amount, mt_gateway, card_type, entry_method, mt_avs_street_match, transaction_type),
        INDEX idx_pmt_routing_runtime_cov (check_bank_routing_number, event_date, amount, mt_gateway, card_type, entry_method, transaction_type),
        INDEX idx_pmt_routing_account_runtime_cov (check_bank_routing_number, check_bank_account_number_sha512, event_date, amount, mt_gateway, card_type, entry_method),
        INDEX idx_pmt_join_runtime_cov (parsed_interaction_id, event_date, amount, merchant_account_number, card_holder_number_sha512, card_type, entry_method, mt_gateway, check_bank_routing_number, transaction_type),
        INDEX idx_pmt_merchant_c_join_cov (merchant_account_number, event_date, parsed_interaction_id, transaction_type),
        INDEX idx_pmt_card_c_join_cov (card_holder_number_sha512, event_date, parsed_interaction_id, transaction_type),
        INDEX idx_pmt_routing_acct_c_join_cov (check_bank_routing_number, check_bank_account_number_sha512, event_date, parsed_interaction_id, transaction_type)
    )
    {ROW_ID_TABLE_OPTIONS}
    {PMT_PARTITION_CLAUSE}
    """,

    # =========================================================================
    # TABLE 2: deviceprofile_fact
    # Source: platform-identity-deviceprofile Kafka topic (45 TPS)
    # Device fingerprinting events — joins with payment transactions.
    # Replaces: Part of the Spark join pipeline
    # =========================================================================
    """
    DROP TABLE IF EXISTS deviceprofile_fact
    """,
    f"""
    CREATE TABLE deviceprofile_fact (
        intuit_tid                   VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.intuitTid',
        realm_id                     VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.realmId',
        message_id                   VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.messageId',
        request_id                   VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.requestId',
        event_type                   VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.eventType',
        app_id                       VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.appId',
        api_name                     VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.apiName',
        api_result                   INT            DEFAULT NULL COMMENT 'RssDeviceSource.apiResult',
        offering_id                  VARCHAR(128)   DEFAULT NULL COMMENT 'RssDeviceSource.offeringId',
        transaction_id               VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.transactionId',
        org_id                       VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.orgId',
        user_session_id              VARCHAR(128)   DEFAULT NULL COMMENT 'RssDeviceSource.userSessionId',
        request_duration             VARCHAR(32)    DEFAULT NULL COMMENT 'RssDeviceSource.requestDuration',
        request_result               VARCHAR(32)    DEFAULT NULL COMMENT 'RssDeviceSource.requestResult',
        business_transaction         VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.businessTransaction',
        jms_timestamp                DATETIME       DEFAULT NULL COMMENT 'RssDeviceSource.jmsTimestamp',
        interaction_id               VARCHAR(128)   DEFAULT NULL COMMENT 'RssDeviceSource.interactionId',
        session_id                   VARCHAR(128)   DEFAULT NULL COMMENT 'Optional session component; Group C joins on parsed interaction_id per Henry',
        intuit_jmsbodyapiname        VARCHAR(64)    DEFAULT NULL COMMENT 'RssDeviceSource.intuit_jmsbodyapiname',
        exact_id                     VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.exactId',
        smart_id                     VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.smartId',
        input_ip                     VARCHAR(45)    DEFAULT NULL COMMENT 'DeviceProfile.inputIp',
        true_ip                      VARCHAR(45)    DEFAULT NULL COMMENT 'DeviceProfile.trueIp',
        proxy_ip                     VARCHAR(45)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIp',
        agent_type                   VARCHAR(32)    DEFAULT NULL COMMENT 'DeviceProfile.agentType',
        agent_os                     VARCHAR(32)    DEFAULT NULL COMMENT 'DeviceProfile.agentOs',
        browser_language             VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.browserLanguage',
        browser_string               VARCHAR(256)   DEFAULT NULL COMMENT 'DeviceProfile.browserString',
        device_match_result          VARCHAR(32)    DEFAULT NULL COMMENT 'DeviceProfile.deviceMatchResult',
        dns_ip                       VARCHAR(45)    DEFAULT NULL COMMENT 'DeviceProfile.dnsIp',
        input_ip_geo                 VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.inputIpGeo',
        input_ip_isp                 VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.inputIpIsp',
        input_ip_score               VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.inputIpScore',
        true_ip_geo                  VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.trueIpGeo',
        true_ip_isp                  VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.trueIpIsp',
        true_ip_result               VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.trueIpResult',
        true_ip_score                VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.trueIpScore',
        proxy_ip_geo                 VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIpGeo',
        proxy_ip_isp                 VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIpIsp',
        proxy_type                   VARCHAR(32)    DEFAULT NULL COMMENT 'DeviceProfile.proxyType',
        proxy_ip_score               VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIpScore',
        device_score                 VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.deviceScore',
        device_fingerprint_score     VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.deviceFingerprintScore',
        device_worst_score           VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.deviceWorstScore',
        fuzzy_device_score           VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.fuzzyDeviceScore',
        true_ip_worst_score          VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.trueIpWorstScore',
        proxy_ip_worst_score         VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIpWorstScore',
        device_fingerprint_result    VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.deviceFingerprintResult',
        device_result                VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.deviceResult',
        proxy_ip_result              VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIpResult',
        fuzzy_device_result          VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.fuzzyDeviceResult',
        dns_ip_hosting_facility      VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.dnsIpHostingFacility',
        proxy_ip_hosting_facility    VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.proxyIpHostingFacility',
        true_ip_hosting_facility     VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.trueIpHostingFacility',
        browser_anomaly              VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.browserAnomaly',
        screen_res_anomaly           VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.screenResAnomaly',
        os_anomaly                   VARCHAR(16)    DEFAULT NULL COMMENT 'DeviceProfile.osAnomaly',
        dns_ip_assert_history        VARCHAR(64)    DEFAULT NULL COMMENT 'DeviceProfile.dnsIpAssertHistory',

        INDEX idx_interaction_id_jms_timestamp (interaction_id, jms_timestamp),
        INDEX idx_session_id         (session_id),
        INDEX idx_exact_id_jms_timestamp   (exact_id, jms_timestamp),
        INDEX idx_smart_id_jms_timestamp   (smart_id, jms_timestamp),
        INDEX idx_input_ip_jms_timestamp   (input_ip, jms_timestamp),
        INDEX idx_true_ip_jms_timestamp    (true_ip, jms_timestamp),
        INDEX idx_user_session_id_jms_timestamp (user_session_id, jms_timestamp),
        INDEX idx_agent_type         (agent_type),
        INDEX idx_realm_id           (realm_id),
        INDEX idx_jms_timestamp      (jms_timestamp),
        INDEX idx_dev_exact_runtime_cov (exact_id, jms_timestamp, interaction_id, agent_type, agent_os, browser_language, device_fingerprint_score, device_score, device_worst_score, input_ip, input_ip_score, proxy_ip, smart_id, true_ip, true_ip_score),
        INDEX idx_dev_smart_runtime_cov (smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score),
        INDEX idx_dev_input_runtime_cov (input_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip_score, smart_id, true_ip, true_ip_score),
        INDEX idx_dev_true_runtime_cov (true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)
    )
    {ROW_ID_TABLE_OPTIONS}
    {DEVICE_PARTITION_CLAUSE}
    """
]


def main():
    print("""
============================================================
 INTUIT RISK POC — Step 1 of 3: Schema Setup
============================================================

 This script creates the database and 2 tables based on
 the sample event structures from the POC document.



 SOURCE TABLES FOR THE FRESH REBUILD:

   Table                  Description                           Kafka Source
   ─────────────────────────────────────────────────────────────────────────────────────
   pmt_txn_fact           Payment transactions. 43 pared-down   ihub-cassandra-sync
                          source fields from the PDF.           (shared monthly profile)

   deviceprofile_fact     Device profiles — browser, OS,        platform-identity-
                          geo, device scores.                   deviceprofile



 WITH TiDB: Both source tables live in one cluster and feed the
 event-level benchmark and join validation work.

============================================================
""")

    DB_CONFIG = get_db_config()
    db_name = DB_CONFIG.pop('database', 'intuit_risk')
    print("Connecting to TiDB...")
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create database if it doesn't exist
    print(f"\n  Creating database '{db_name}' (if it doesn't exist)...")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE {db_name}")
    print(f"  ✅ Using database: {db_name}")

    print(f"\n\n\n{'─'*60}")
    print(f"  CREATING TABLES")
    print(f"{'─'*60}\n")

    for sql in TABLES:
        sql_clean = sql.strip()
        if sql_clean.startswith("DROP"):
            table = sql_clean.split()[-1]
            print(f"  Dropping {table}...")
        elif sql_clean.startswith("CREATE"):
            table = sql_clean.split("(")[0].strip().split()[-1]
            print(f"  Creating {table}...")
        cursor.execute(sql_clean)

    # Show DDL for each table so user can see the schema
    print("\n\n\n" + "─"*60)
    print("  TABLE SCHEMAS (DDL):")
    print("─"*60)
    for table in ['pmt_txn_fact', 'deviceprofile_fact']:
        # Note: f-string is safe here — table names are hardcoded string literals,
        # not user input. MySQL/PyMySQL does not support parameterized identifiers.
        cursor.execute(f"SHOW CREATE TABLE {table}")
        ddl = cursor.fetchone()[1]
        print(f"\n  📋 {table}:")
        for line in ddl.split('\n'):
            print(f"     {line}")
        print()

    if ENABLE_TIFLASH:
        # Enable TiFlash replicas for analytical queries (Redshift replacement)
        print("\n\n\n" + "─"*60)
        print("  ENABLING TiFlash REPLICAS")
        print("─"*60)
        print("\nEnabling TiFlash replicas on the 2 source tables...")
        for table in ['pmt_txn_fact', 'deviceprofile_fact']:
            cursor.execute(f"ALTER TABLE {table} SET TIFLASH REPLICA 1")
            print(f"  TiFlash replica set on {table}")

        # Wait for TiFlash replicas to sync before proceeding
        print("\n  Waiting for TiFlash replicas to sync...")
        while True:
            cursor.execute(
                "SELECT TABLE_NAME, PROGRESS FROM information_schema.tiflash_replica "
                "WHERE PROGRESS < 1"
            )
            pending = cursor.fetchall()
            if not pending:
                print("  ✅ All TiFlash replicas are synced.")
                break
            for tbl_name, progress in pending:
                print(f"    {tbl_name}: {float(progress)*100:.1f}% synced")
            time.sleep(5)
    else:
        print("\n\n\n" + "─"*60)
        print("  SKIPPING TiFlash REPLICAS DURING SCHEMA SETUP")
        print("─"*60)
        print("  INTUIT_SETUP_TIFLASH=0, so load can finish first.")
        print("  Run enable_tiflash.py after data load and before benchmark.")

    print(f"""


============================================================
 ✅ Schema setup complete!
============================================================
 Tables created:""")
    cursor.execute("SHOW TABLES")
    for row in cursor.fetchall():
        # Note: f-string is safe here — row[0] comes from SHOW TABLES (server metadata),
        # not user input. MySQL/PyMySQL does not support parameterized identifiers.
        cursor.execute(f"SELECT COUNT(*) FROM {row[0]}")
        cnt = cursor.fetchone()[0]
        print(f"   - {row[0]:<25} ({cnt:,} rows)")

    print(f"""
 TiFlash replicas: {'enabled on all tables' if ENABLE_TIFLASH else 'skipped during setup'}
 Database: {db_name}

 Next step: python3 load_data.py
============================================================""")

    conn.close()


if __name__ == '__main__':
    main()
