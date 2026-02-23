# Facebook Ads Data Pipeline

This project extracts daily conversion metrics from Facebook Ads API and loads them into an Azure SQL database for BI analysis.

## Stack
- Python
- Facebook Marketing API
- Azure SQL
- REST API
- ETL Pipeline

## Pipeline

Facebook Ads API → Python ETL → Azure SQL → Power BI

## Features
- Daily incremental data extraction
- API pagination handling
- Duplicate detection & overwrite logic
- Automated database ingestion
- Ready-to-query BI tables

## Use Case

Marketing performance data is automatically centralized into a structured database to support Power BI dashboards without manual CSV exports.

## Business Impact

Reduced manual reporting time and ensured reliable daily campaign performance tracking.# facebook-ads-data-pipeline
