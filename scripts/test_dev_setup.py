#!/usr/bin/env python3
"""
Development Environment Test Script

Tests:
1. MongoDB connection
2. MongoDB collections initialization
3. API health check
4. Quick-add API functionality (optional)

Usage:
    python scripts/test_dev_setup.py
    python scripts/test_dev_setup.py --full  # Include API tests
"""

import sys
import os
import argparse
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_mongodb_connection():
    """Test MongoDB connection."""
    print("\n" + "=" * 50)
    print("1. Testing MongoDB Connection")
    print("=" * 50)

    try:
        from pymongo import MongoClient

        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        database = os.getenv('MONGODB_DATABASE', 'crawler_system_dev')

        print(f"   URI: {uri}")
        print(f"   Database: {database}")

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)

        # Test connection
        client.admin.command('ping')
        print("   ✅ MongoDB connection successful!")

        # Get database
        db = client[database]

        # List collections
        collections = db.list_collection_names()
        print(f"   📦 Collections found: {len(collections)}")
        for col in collections:
            count = db[col].count_documents({})
            print(f"      - {col}: {count} documents")

        client.close()
        return True

    except Exception as e:
        print(f"   ❌ MongoDB connection failed: {e}")
        return False


def test_mongodb_init():
    """Test MongoDB collections initialization."""
    print("\n" + "=" * 50)
    print("2. Testing MongoDB Collections Initialization")
    print("=" * 50)

    try:
        from pymongo import MongoClient

        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        database = os.getenv('MONGODB_DATABASE', 'crawler_system_dev')

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[database]

        # Required collections
        required_collections = [
            # Core collections
            'sources', 'crawlers', 'crawl_results', 'crawler_history', 'error_logs',
            # Self-healing collections
            'healing_sessions', 'wellknown_cases', 'healing_schedules',
            # ETL data collections
            'news_articles', 'financial_data', 'stock_prices',
            'exchange_rates', 'market_indices', 'announcements', 'crawl_data'
        ]

        existing = set(db.list_collection_names())
        missing = []

        for col in required_collections:
            if col in existing:
                print(f"   ✅ {col}")
            else:
                print(f"   ❌ {col} (missing)")
                missing.append(col)

        if missing:
            print(f"\n   ⚠️  Missing collections: {missing}")
            print("   Run MongoDB init script to create them:")
            print("   mongo < mongodb/init-scripts/init.js")

            # Create missing collections
            create = input("\n   Create missing collections now? (y/n): ").strip().lower()
            if create == 'y':
                for col in missing:
                    db.create_collection(col)
                    print(f"   Created: {col}")
                print("   ✅ Collections created!")
        else:
            print("\n   ✅ All required collections exist!")

        client.close()
        return len(missing) == 0

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_api_health():
    """Test API health endpoint."""
    print("\n" + "=" * 50)
    print("3. Testing API Health")
    print("=" * 50)

    try:
        import requests

        api_url = os.getenv('API_URL', 'http://localhost:8000')

        print(f"   API URL: {api_url}")

        # Test health endpoint
        response = requests.get(f"{api_url}/health", timeout=5)

        if response.status_code == 200:
            print("   ✅ API is healthy!")
            print(f"   Response: {response.json()}")
            return True
        else:
            print(f"   ❌ API returned status: {response.status_code}")
            return False

    except requests.exceptions.ConnectionError:
        print("   ⚠️  API not running (this is OK if you're testing MongoDB only)")
        return None
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_quick_add_api():
    """Test Quick Add API functionality."""
    print("\n" + "=" * 50)
    print("4. Testing Quick Add API")
    print("=" * 50)

    try:
        import requests

        api_url = os.getenv('API_URL', 'http://localhost:8000')

        # Test URL analysis (without actually creating)
        test_urls = [
            "https://finance.naver.com/marketindex/",
            "https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=101"
        ]

        for url in test_urls:
            print(f"\n   Analyzing: {url}")

            response = requests.post(
                f"{api_url}/api/quick-add/analyze",
                json={"url": url},
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Analysis successful!")
                print(f"      - Page Type: {result.get('page_type')}")
                print(f"      - Fields: {len(result.get('fields', []))}")
                print(f"      - Schedule: {result.get('schedule')}")
            else:
                print(f"   ❌ Analysis failed: {response.status_code}")
                print(f"      {response.text[:200]}")

        return True

    except requests.exceptions.ConnectionError:
        print("   ⚠️  API not running")
        return None
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_monitoring_api():
    """Test Monitoring API."""
    print("\n" + "=" * 50)
    print("5. Testing Monitoring API")
    print("=" * 50)

    try:
        import requests

        api_url = os.getenv('API_URL', 'http://localhost:8000')

        # Test health check
        response = requests.get(f"{api_url}/api/monitoring/health", timeout=5)

        if response.status_code == 200:
            result = response.json()
            print("   ✅ Monitoring health check successful!")
            print(f"      - Overall Score: {result.get('overall_score')}")
            print(f"      - Status: {result.get('status')}")
            print(f"      - Active Sources: {result.get('active_sources')}")
            print(f"      - Failed Sources: {result.get('failed_sources')}")
            return True
        else:
            print(f"   ❌ Health check failed: {response.status_code}")
            return False

    except requests.exceptions.ConnectionError:
        print("   ⚠️  API not running")
        return None
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def insert_test_data():
    """Insert test data for development."""
    print("\n" + "=" * 50)
    print("6. Inserting Test Data")
    print("=" * 50)

    try:
        from pymongo import MongoClient
        from bson import ObjectId

        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        database = os.getenv('MONGODB_DATABASE', 'crawler_system_dev')

        client = MongoClient(uri)
        db = client[database]

        # Check if test data already exists
        if db.sources.find_one({'name': 'TEST_NAVER_FINANCE'}):
            print("   ⚠️  Test data already exists")
            return True

        # Insert test source
        now = datetime.utcnow()
        test_source = {
            'name': 'TEST_NAVER_FINANCE',
            'url': 'https://finance.naver.com/marketindex/',
            'type': 'html',
            'fields': [
                {'name': 'exchange_rate', 'selector': '.data', 'data_type': 'float'},
                {'name': 'change', 'selector': '.change', 'data_type': 'float'}
            ],
            'schedule': '0 9 * * 1-5',  # 평일 오전 9시
            'status': 'inactive',
            'error_count': 0,
            'created_at': now,
            'updated_at': now,
            'metadata': {
                'page_type': 'financial_data',
                'crawl_strategy': 'static'
            }
        }

        result = db.sources.insert_one(test_source)
        print(f"   ✅ Test source created: {result.inserted_id}")

        # Insert test crawler
        test_crawler = {
            'source_id': result.inserted_id,
            'code': '''
async def crawl(url, fields):
    import httpx
    from bs4 import BeautifulSoup

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract exchange rates
        items = soup.select('.data_lst li')
        results = []

        for item in items:
            name = item.select_one('.h_lst').text.strip()
            value = item.select_one('.value').text.strip()
            results.append({'name': name, 'value': value})

        return results
''',
            'version': 1,
            'status': 'testing',
            'dag_id': None,
            'created_at': now,
            'created_by': 'manual',
            'gpt_prompt': 'Test crawler for development'
        }

        crawler_result = db.crawlers.insert_one(test_crawler)
        print(f"   ✅ Test crawler created: {crawler_result.inserted_id}")

        # Insert a wellknown case for testing
        test_case = {
            'error_pattern': 'ConnectionTimeout',
            'error_category': 'network_error',
            'solution_type': 'retry',
            'solution_code': 'retry_with_backoff',
            'solution_description': '네트워크 타임아웃 시 지수 백오프로 재시도',
            'success_count': 10,
            'failure_count': 1,
            'last_used': now,
            'created_by': 'manual',
            'created_at': now
        }

        case_result = db.wellknown_cases.insert_one(test_case)
        print(f"   ✅ Test wellknown case created: {case_result.inserted_id}")

        client.close()
        return True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Test Development Environment Setup')
    parser.add_argument('--full', action='store_true', help='Run all tests including API tests')
    parser.add_argument('--init-data', action='store_true', help='Insert test data')
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("   ETL Pipeline Development Environment Test")
    print("   " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 60)

    results = {}

    # Always test MongoDB
    results['mongodb_connection'] = test_mongodb_connection()
    results['mongodb_init'] = test_mongodb_init()

    # Insert test data if requested
    if args.init_data:
        results['test_data'] = insert_test_data()

    # API tests only with --full flag
    if args.full:
        results['api_health'] = test_api_health()
        results['quick_add'] = test_quick_add_api()
        results['monitoring'] = test_monitoring_api()

    # Summary
    print("\n" + "=" * 50)
    print("Test Summary")
    print("=" * 50)

    for test, result in results.items():
        if result is True:
            status = "✅ PASS"
        elif result is False:
            status = "❌ FAIL"
        else:
            status = "⚠️  SKIP"
        print(f"   {test}: {status}")

    # Overall result
    failures = [r for r in results.values() if r is False]
    if failures:
        print(f"\n   ❌ {len(failures)} test(s) failed")
        return 1
    else:
        print("\n   ✅ All tests passed!")
        return 0


if __name__ == '__main__':
    sys.exit(main())
