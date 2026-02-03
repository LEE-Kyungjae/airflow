// MongoDB Initialization Script for Crawler System

// Switch to the crawler_system database
db = db.getSiblingDB('crawler_system');

// Create collections with validation schemas
db.createCollection('sources', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['name', 'url', 'type', 'fields', 'schedule', 'status'],
      properties: {
        name: {
          bsonType: 'string',
          description: 'Source name - required'
        },
        url: {
          bsonType: 'string',
          description: 'Source URL - required'
        },
        type: {
          enum: ['html', 'pdf', 'excel', 'csv'],
          description: 'Data type - must be html, pdf, excel, or csv'
        },
        fields: {
          bsonType: 'array',
          items: {
            bsonType: 'object',
            required: ['name'],
            properties: {
              name: { bsonType: 'string' },
              selector: { bsonType: 'string' },
              data_type: { bsonType: 'string' }
            }
          }
        },
        schedule: {
          bsonType: 'string',
          description: 'Cron expression for scheduling'
        },
        status: {
          enum: ['active', 'inactive', 'error'],
          description: 'Source status'
        },
        last_run: { bsonType: 'date' },
        last_success: { bsonType: 'date' },
        error_count: { bsonType: 'int' },
        created_at: { bsonType: 'date' },
        updated_at: { bsonType: 'date' }
      }
    }
  }
});

db.createCollection('crawlers', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['source_id', 'code', 'version', 'status'],
      properties: {
        source_id: { bsonType: 'objectId' },
        code: { bsonType: 'string' },
        version: { bsonType: 'int' },
        status: {
          enum: ['active', 'testing', 'deprecated']
        },
        dag_id: { bsonType: 'string' },
        created_at: { bsonType: 'date' },
        created_by: {
          enum: ['gpt', 'manual']
        },
        gpt_prompt: { bsonType: 'string' }
      }
    }
  }
});

db.createCollection('crawl_results', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['source_id', 'crawler_id', 'status', 'executed_at'],
      properties: {
        source_id: { bsonType: 'objectId' },
        crawler_id: { bsonType: 'objectId' },
        run_id: { bsonType: 'string' },
        data: { bsonType: 'object' },
        record_count: { bsonType: 'int' },
        status: {
          enum: ['success', 'partial', 'failed']
        },
        error_code: { bsonType: 'string' },
        error_message: { bsonType: 'string' },
        execution_time_ms: { bsonType: 'int' },
        executed_at: { bsonType: 'date' }
      }
    }
  }
});

db.createCollection('crawler_history', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['crawler_id', 'version', 'code', 'change_reason', 'changed_at'],
      properties: {
        crawler_id: { bsonType: 'objectId' },
        version: { bsonType: 'int' },
        code: { bsonType: 'string' },
        change_reason: {
          enum: ['auto_fix', 'manual_edit', 'regenerate']
        },
        change_detail: { bsonType: 'string' },
        changed_at: { bsonType: 'date' },
        changed_by: {
          enum: ['gpt', 'user']
        }
      }
    }
  }
});

db.createCollection('error_logs', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['source_id', 'error_code', 'error_type', 'message', 'created_at'],
      properties: {
        source_id: { bsonType: 'objectId' },
        crawler_id: { bsonType: 'objectId' },
        run_id: { bsonType: 'string' },
        error_code: { bsonType: 'string' },
        error_type: { bsonType: 'string' },
        message: { bsonType: 'string' },
        stack_trace: { bsonType: 'string' },
        html_snapshot: { bsonType: 'string' },
        auto_recoverable: { bsonType: 'bool' },
        resolved: { bsonType: 'bool' },
        resolved_at: { bsonType: 'date' },
        resolution_method: {
          enum: ['auto', 'manual']
        },
        resolution_detail: { bsonType: 'string' },
        created_at: { bsonType: 'date' }
      }
    }
  }
});

// Create indexes for better query performance
// Sources collection indexes
db.sources.createIndex({ name: 1 }, { unique: true });
db.sources.createIndex({ status: 1 });
db.sources.createIndex({ type: 1 });
db.sources.createIndex({ created_at: -1 });

// Crawlers collection indexes
db.crawlers.createIndex({ source_id: 1 });
db.crawlers.createIndex({ dag_id: 1 }, { unique: true, sparse: true });
db.crawlers.createIndex({ status: 1 });
db.crawlers.createIndex({ created_at: -1 });

// Crawl results collection indexes
db.crawl_results.createIndex({ source_id: 1, executed_at: -1 });
db.crawl_results.createIndex({ crawler_id: 1 });
db.crawl_results.createIndex({ run_id: 1 });
db.crawl_results.createIndex({ status: 1 });
db.crawl_results.createIndex({ executed_at: -1 });

// Crawler history collection indexes
db.crawler_history.createIndex({ crawler_id: 1, version: -1 });
db.crawler_history.createIndex({ changed_at: -1 });

// Error logs collection indexes
db.error_logs.createIndex({ source_id: 1, created_at: -1 });
db.error_logs.createIndex({ crawler_id: 1 });
db.error_logs.createIndex({ error_code: 1 });
db.error_logs.createIndex({ resolved: 1 });
db.error_logs.createIndex({ created_at: -1 });

// Create TTL index for old crawl results (keep for 90 days)
db.crawl_results.createIndex(
  { executed_at: 1 },
  { expireAfterSeconds: 7776000 }  // 90 days
);

// Create TTL index for old error logs (keep for 30 days if resolved)
// Note: This will only delete documents that have resolved: true and are older than 30 days

// ============== Self-Healing Collections ==============

// Healing Sessions - 자가 치유 세션
db.createCollection('healing_sessions');
db.healing_sessions.createIndex({ session_id: 1 }, { unique: true });
db.healing_sessions.createIndex({ source_id: 1, created_at: -1 });
db.healing_sessions.createIndex({ status: 1 });
db.healing_sessions.createIndex({ created_at: -1 });

// Wellknown Cases - 알려진 문제 케이스
db.createCollection('wellknown_cases');
db.wellknown_cases.createIndex({ error_pattern: 1 });
db.wellknown_cases.createIndex({ error_category: 1 });
db.wellknown_cases.createIndex({ success_count: -1 });

// Healing Schedules - 재시도 스케줄
db.createCollection('healing_schedules');
db.healing_schedules.createIndex({ session_id: 1 });
db.healing_schedules.createIndex({ scheduled_at: 1 });

// ============== ETL Data Collections ==============

// News Articles
db.createCollection('news_articles');
db.news_articles.createIndex({ content_hash: 1 }, { unique: true, sparse: true });
db.news_articles.createIndex({ published_at: -1 });
db.news_articles.createIndex({ _source_id: 1, _data_date: -1 });
db.news_articles.createIndex({ _crawled_at: 1 }, { expireAfterSeconds: 7776000 }); // 90 days

// Financial Data
db.createCollection('financial_data');
db.financial_data.createIndex({ stock_code: 1, _data_date: -1 });
db.financial_data.createIndex({ _source_id: 1 });
db.financial_data.createIndex({ trade_date: -1 });

// Stock Prices
db.createCollection('stock_prices');
db.stock_prices.createIndex({ stock_code: 1, _data_date: 1 }, { unique: true, sparse: true });
db.stock_prices.createIndex({ trade_date: -1 });

// Exchange Rates
db.createCollection('exchange_rates');
db.exchange_rates.createIndex({ currency_code: 1, _data_date: 1 }, { unique: true, sparse: true });

// Market Indices
db.createCollection('market_indices');
db.market_indices.createIndex({ index_code: 1, _data_date: 1 }, { unique: true, sparse: true });

// Announcements (공시)
db.createCollection('announcements');
db.announcements.createIndex({ content_hash: 1 }, { unique: true, sparse: true });
db.announcements.createIndex({ published_at: -1 });
db.announcements.createIndex({ announcement_type: 1 });
db.announcements.createIndex({ _crawled_at: 1 }, { expireAfterSeconds: 31536000 }); // 365 days

// Generic crawl data
db.createCollection('crawl_data');
db.crawl_data.createIndex({ _source_id: 1, _data_date: -1 });
db.crawl_data.createIndex({ _crawled_at: -1 });

// ============== Data Review/Verification Collections ==============

// Data Reviews - 데이터 검토 상태
db.createCollection('data_reviews', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['crawl_result_id', 'source_id', 'review_status', 'created_at'],
      properties: {
        crawl_result_id: { bsonType: 'objectId' },
        source_id: { bsonType: 'objectId' },
        data_record_index: { bsonType: 'int' },  // Index within crawl result data array
        review_status: {
          enum: ['pending', 'approved', 'on_hold', 'needs_correction', 'corrected']
        },
        reviewer_id: { bsonType: 'string' },
        reviewed_at: { bsonType: 'date' },
        original_data: { bsonType: 'object' },    // Original extracted data
        corrected_data: { bsonType: 'object' },   // Corrected data (if any)
        corrections: {
          bsonType: 'array',
          items: {
            bsonType: 'object',
            properties: {
              field: { bsonType: 'string' },
              original_value: { bsonType: ['string', 'number', 'null'] },
              corrected_value: { bsonType: ['string', 'number', 'null'] },
              reason: { bsonType: 'string' }
            }
          }
        },
        source_highlights: {
          bsonType: 'array',
          items: {
            bsonType: 'object',
            properties: {
              field: { bsonType: 'string' },
              bbox: {
                bsonType: 'object',
                properties: {
                  x: { bsonType: 'double' },
                  y: { bsonType: 'double' },
                  width: { bsonType: 'double' },
                  height: { bsonType: 'double' }
                }
              },
              page: { bsonType: 'int' },
              selector: { bsonType: 'string' }
            }
          }
        },
        confidence_score: { bsonType: 'double' },
        ocr_confidence: { bsonType: 'double' },
        ai_confidence: { bsonType: 'double' },
        needs_number_review: { bsonType: 'bool' },
        uncertain_numbers: { bsonType: 'array' },
        notes: { bsonType: 'string' },
        review_duration_ms: { bsonType: 'int' },
        created_at: { bsonType: 'date' },
        updated_at: { bsonType: 'date' }
      }
    }
  }
});

// Data Reviews indexes
db.data_reviews.createIndex({ crawl_result_id: 1, data_record_index: 1 });
db.data_reviews.createIndex({ source_id: 1, review_status: 1 });
db.data_reviews.createIndex({ review_status: 1, created_at: -1 });
db.data_reviews.createIndex({ reviewer_id: 1, reviewed_at: -1 });
db.data_reviews.createIndex({ needs_number_review: 1, review_status: 1 });
db.data_reviews.createIndex({ created_at: -1 });

// Review Queue - 검토 대기열 (빠른 조회용)
db.createCollection('review_queue');
db.review_queue.createIndex({ source_id: 1, priority: -1, created_at: 1 });
db.review_queue.createIndex({ assigned_to: 1, status: 1 });
db.review_queue.createIndex({ status: 1, created_at: 1 });

// Review Sessions - 검토 세션 (통계용)
db.createCollection('review_sessions');
db.review_sessions.createIndex({ reviewer_id: 1, started_at: -1 });
db.review_sessions.createIndex({ source_id: 1 });

print('MongoDB initialization completed successfully!');
print('Collections created:');
print('  - Core: sources, crawlers, crawl_results, crawler_history, error_logs');
print('  - Self-Healing: healing_sessions, wellknown_cases, healing_schedules');
print('  - ETL Data: news_articles, financial_data, stock_prices, exchange_rates, market_indices, announcements, crawl_data');
print('Indexes created for optimal query performance');
