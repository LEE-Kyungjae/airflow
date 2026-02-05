#!/usr/bin/env python3
"""
MongoDB Initialization Script for Development

Creates all required collections and indexes for the ETL Pipeline.

Usage:
    python scripts/init_mongodb.py
    python scripts/init_mongodb.py --uri mongodb://localhost:27017 --db crawler_system_dev
"""

import os
import sys
import argparse
from datetime import datetime

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    from pymongo.errors import CollectionInvalid
except ImportError:
    print("Error: pymongo is required. Install with: pip install pymongo")
    sys.exit(1)


def create_collections(db):
    """Create all required collections with validation schemas."""

    collections_config = {
        # Core collections
        'sources': {
            'validator': {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['name', 'url', 'type', 'fields', 'schedule', 'status'],
                    'properties': {
                        'name': {'bsonType': 'string'},
                        'url': {'bsonType': 'string'},
                        'type': {'enum': ['html', 'pdf', 'excel', 'csv']},
                        'status': {'enum': ['active', 'inactive', 'error']}
                    }
                }
            }
        },
        'crawlers': {
            'validator': {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['source_id', 'code', 'version', 'status'],
                    'properties': {
                        'status': {'enum': ['active', 'testing', 'deprecated']}
                    }
                }
            }
        },
        'crawl_results': {},
        'crawler_history': {},
        'error_logs': {},

        # Self-healing collections
        'healing_sessions': {},
        'wellknown_cases': {},
        'healing_schedules': {},

        # Authentication collections
        'auth_configs': {
            'validator': {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['source_id', 'auth_type'],
                    'properties': {
                        'auth_type': {'enum': ['form', 'oauth', 'api_key', 'cookie', 'basic', 'bearer', 'custom']},
                        'session_duration_hours': {'bsonType': 'int', 'minimum': 1, 'maximum': 720},
                        'auto_refresh': {'bsonType': 'bool'}
                    }
                }
            }
        },
        'auth_credentials': {
            'validator': {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['source_id', 'auth_type'],
                    'properties': {
                        'auth_type': {'enum': ['form', 'oauth', 'api_key', 'cookie', 'basic', 'bearer', 'custom']},
                        'username': {'bsonType': 'string'},
                        'password': {'bsonType': 'string'},  # Encrypted
                        'api_key': {'bsonType': 'string'},   # Encrypted
                        'oauth_token': {'bsonType': 'string'}  # Encrypted
                    }
                }
            }
        },
        'auth_sessions': {
            'validator': {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['source_id', 'is_valid'],
                    'properties': {
                        'is_valid': {'bsonType': 'bool'},
                        'cookies': {'bsonType': 'object'},
                        'expires_at': {'bsonType': 'string'}
                    }
                }
            }
        },

        # ETL data collections
        'news_articles': {},
        'financial_data': {},
        'stock_prices': {},
        'exchange_rates': {},
        'market_indices': {},
        'announcements': {},
        'crawl_data': {}
    }

    created = []
    existing = []

    for name, config in collections_config.items():
        try:
            if config:
                db.create_collection(name, **config)
            else:
                db.create_collection(name)
            created.append(name)
            print(f"   ✅ Created: {name}")
        except CollectionInvalid:
            existing.append(name)
            print(f"   ⚠️  Exists: {name}")
        except Exception as e:
            print(f"   ❌ Error creating {name}: {e}")

    return created, existing


def create_indexes(db):
    """Create indexes for optimal query performance."""

    indexes = {
        'sources': [
            ([('name', ASCENDING)], {'unique': True}),
            ([('status', ASCENDING)], {}),
            ([('type', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'crawlers': [
            ([('source_id', ASCENDING)], {}),
            ([('dag_id', ASCENDING)], {'unique': True, 'sparse': True}),
            ([('status', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'crawl_results': [
            ([('source_id', ASCENDING), ('executed_at', DESCENDING)], {}),
            ([('crawler_id', ASCENDING)], {}),
            ([('run_id', ASCENDING)], {}),
            ([('status', ASCENDING)], {}),
            ([('executed_at', DESCENDING)], {}),
            ([('executed_at', ASCENDING)], {'expireAfterSeconds': 7776000})  # 90 days TTL
        ],
        'crawler_history': [
            ([('crawler_id', ASCENDING), ('version', DESCENDING)], {}),
            ([('changed_at', DESCENDING)], {})
        ],
        'error_logs': [
            ([('source_id', ASCENDING), ('created_at', DESCENDING)], {}),
            ([('crawler_id', ASCENDING)], {}),
            ([('error_code', ASCENDING)], {}),
            ([('resolved', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'healing_sessions': [
            ([('session_id', ASCENDING)], {'unique': True}),
            ([('source_id', ASCENDING), ('created_at', DESCENDING)], {}),
            ([('status', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'wellknown_cases': [
            ([('error_pattern', ASCENDING)], {}),
            ([('error_category', ASCENDING)], {}),
            ([('success_count', DESCENDING)], {})
        ],
        'healing_schedules': [
            ([('session_id', ASCENDING)], {}),
            ([('scheduled_at', ASCENDING)], {})
        ],
        # Authentication indexes
        'auth_configs': [
            ([('source_id', ASCENDING)], {'unique': True}),
            ([('auth_type', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'auth_credentials': [
            ([('source_id', ASCENDING)], {'unique': True}),
            ([('auth_type', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'auth_sessions': [
            ([('source_id', ASCENDING)], {'unique': True}),
            ([('is_valid', ASCENDING)], {}),
            ([('expires_at', ASCENDING)], {}),
            ([('created_at', DESCENDING)], {})
        ],
        'news_articles': [
            ([('content_hash', ASCENDING)], {'unique': True, 'sparse': True}),
            ([('published_at', DESCENDING)], {}),
            ([('_source_id', ASCENDING), ('_data_date', DESCENDING)], {}),
            ([('_crawled_at', ASCENDING)], {'expireAfterSeconds': 7776000})  # 90 days
        ],
        'financial_data': [
            ([('stock_code', ASCENDING), ('_data_date', DESCENDING)], {}),
            ([('_source_id', ASCENDING)], {}),
            ([('trade_date', DESCENDING)], {})
        ],
        'stock_prices': [
            ([('stock_code', ASCENDING), ('_data_date', ASCENDING)], {'unique': True, 'sparse': True}),
            ([('trade_date', DESCENDING)], {})
        ],
        'exchange_rates': [
            ([('currency_code', ASCENDING), ('_data_date', ASCENDING)], {'unique': True, 'sparse': True})
        ],
        'market_indices': [
            ([('index_code', ASCENDING), ('_data_date', ASCENDING)], {'unique': True, 'sparse': True})
        ],
        'announcements': [
            ([('content_hash', ASCENDING)], {'unique': True, 'sparse': True}),
            ([('published_at', DESCENDING)], {}),
            ([('announcement_type', ASCENDING)], {}),
            ([('_crawled_at', ASCENDING)], {'expireAfterSeconds': 31536000})  # 365 days
        ],
        'crawl_data': [
            ([('_source_id', ASCENDING), ('_data_date', DESCENDING)], {}),
            ([('_crawled_at', DESCENDING)], {})
        ]
    }

    created_count = 0
    for collection, index_list in indexes.items():
        for index_keys, options in index_list:
            try:
                db[collection].create_index(index_keys, **options)
                created_count += 1
            except Exception as e:
                print(f"   ⚠️  Index on {collection}: {e}")

    print(f"   ✅ Created/verified {created_count} indexes")
    return created_count


def main():
    parser = argparse.ArgumentParser(description='Initialize MongoDB for ETL Pipeline')
    parser.add_argument('--uri', default=os.getenv('MONGODB_URI', 'mongodb://localhost:27017'),
                        help='MongoDB connection URI')
    parser.add_argument('--db', default=os.getenv('MONGODB_DATABASE', 'crawler_system_dev'),
                        help='Database name')
    parser.add_argument('--drop', action='store_true',
                        help='Drop existing collections before creating (DANGEROUS)')
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("   MongoDB Initialization for ETL Pipeline")
    print("   " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 60)
    print(f"\n   URI: {args.uri}")
    print(f"   Database: {args.db}")

    try:
        # Connect to MongoDB
        print("\n[1/4] Connecting to MongoDB...")
        client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("   ✅ Connected successfully")

        db = client[args.db]

        # Drop collections if requested
        if args.drop:
            print("\n[2/4] Dropping existing collections...")
            confirm = input("   ⚠️  This will DELETE all data. Type 'YES' to confirm: ")
            if confirm == 'YES':
                for col in db.list_collection_names():
                    db.drop_collection(col)
                    print(f"   Dropped: {col}")
            else:
                print("   Cancelled")
        else:
            print("\n[2/4] Skipping drop (use --drop to reset)")

        # Create collections
        print("\n[3/4] Creating collections...")
        created, existing = create_collections(db)
        print(f"   Summary: {len(created)} created, {len(existing)} already existed")

        # Create indexes
        print("\n[4/4] Creating indexes...")
        index_count = create_indexes(db)

        # Summary
        print("\n" + "=" * 60)
        print("   Initialization Complete!")
        print("=" * 60)
        print(f"\n   Collections: {len(db.list_collection_names())}")
        print(f"   Indexes: {index_count}")
        print("\n   Core Collections:")
        print("     - sources, crawlers, crawl_results, crawler_history, error_logs")
        print("\n   Self-Healing Collections:")
        print("     - healing_sessions, wellknown_cases, healing_schedules")
        print("\n   Authentication Collections:")
        print("     - auth_configs, auth_credentials, auth_sessions")
        print("\n   ETL Data Collections:")
        print("     - news_articles, financial_data, stock_prices")
        print("     - exchange_rates, market_indices, announcements, crawl_data")
        print()

        client.close()
        return 0

    except Exception as e:
        print(f"\n   ❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
