import { APIRequestContext, expect } from '@playwright/test';

/**
 * API Helper Fixture
 *
 * Provides utility functions for API interactions in E2E tests.
 * Includes methods for all major API endpoints.
 */

const API_URL = process.env.API_URL || 'http://localhost:8000';

// ============================================
// Type Definitions
// ============================================

export interface Source {
  _id: string;
  name: string;
  url: string;
  type: 'html' | 'pdf' | 'excel' | 'csv';
  fields: FieldDefinition[];
  schedule?: string;
  status: 'active' | 'inactive' | 'error' | 'pending';
  last_run?: string;
  last_success?: string;
  error_count: number;
  created_at: string;
  updated_at: string;
}

export interface FieldDefinition {
  name: string;
  selector: string;
  data_type: 'string' | 'number' | 'date' | 'url' | 'list';
  is_list?: boolean;
  attribute?: string;
  pattern?: string;
}

export interface Crawler {
  _id: string;
  source_id: string;
  version: number;
  status: 'active' | 'testing' | 'deprecated';
  dag_id: string;
  created_at: string;
  created_by: string;
  code?: string;
}

export interface DashboardStats {
  sources: { total: number; active: number; error: number };
  crawlers: { total: number; active: number };
  recent_executions: {
    total: number;
    success: number;
    failed: number;
    success_rate: number;
  };
  unresolved_errors: number;
  timestamp: string;
}

export interface Review {
  _id: string;
  crawl_result_id: string;
  source_id: string;
  review_status: 'pending' | 'approved' | 'on_hold' | 'needs_correction' | 'corrected';
  original_data: Record<string, any>;
  corrected_data?: Record<string, any>;
  confidence_score?: number;
  created_at: string;
}

// ============================================
// API Helper Class
// ============================================

export class ApiHelper {
  constructor(
    private request: APIRequestContext,
    private authToken?: string
  ) {}

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.authToken) {
      headers['Authorization'] = `Bearer ${this.authToken}`;
    }
    return headers;
  }

  // ========== Authentication ==========

  async login(username: string, password: string): Promise<{ access_token: string }> {
    const response = await this.request.post(`${API_URL}/api/auth/login`, {
      data: { username, password },
    });
    expect(response.ok()).toBeTruthy();
    const data = await response.json();
    this.authToken = data.access_token;
    return data;
  }

  async verifyAuth(): Promise<any> {
    const response = await this.request.get(`${API_URL}/api/auth/verify`, {
      headers: this.getHeaders(),
    });
    return response.json();
  }

  // ========== Sources ==========

  async createSource(sourceData: Partial<Source>): Promise<any> {
    const response = await this.request.post(`${API_URL}/api/sources`, {
      headers: this.getHeaders(),
      data: sourceData,
    });
    return { response, data: response.ok() ? await response.json() : null };
  }

  async listSources(params?: { status?: string; skip?: number; limit?: number }): Promise<Source[]> {
    const queryParams = new URLSearchParams();
    if (params?.status) queryParams.append('status', params.status);
    if (params?.skip) queryParams.append('skip', params.skip.toString());
    if (params?.limit) queryParams.append('limit', params.limit.toString());

    const url = `${API_URL}/api/sources${queryParams.toString() ? '?' + queryParams : ''}`;
    const response = await this.request.get(url, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getSource(sourceId: string): Promise<Source | null> {
    const response = await this.request.get(`${API_URL}/api/sources/${sourceId}`, {
      headers: this.getHeaders(),
    });
    if (!response.ok()) return null;
    return response.json();
  }

  async updateSource(sourceId: string, updateData: Partial<Source>): Promise<Source> {
    const response = await this.request.put(`${API_URL}/api/sources/${sourceId}`, {
      headers: this.getHeaders(),
      data: updateData,
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async deleteSource(sourceId: string): Promise<boolean> {
    const response = await this.request.delete(`${API_URL}/api/sources/${sourceId}`, {
      headers: this.getHeaders(),
    });
    return response.status() === 204;
  }

  async triggerCrawl(sourceId: string): Promise<any> {
    const response = await this.request.post(`${API_URL}/api/sources/${sourceId}/trigger`, {
      headers: this.getHeaders(),
    });
    return { response, data: response.ok() ? await response.json() : null };
  }

  // ========== Quick Add ==========

  async analyzeUrl(url: string, hint?: string): Promise<any> {
    const response = await this.request.post(`${API_URL}/api/quick-add/analyze`, {
      headers: this.getHeaders(),
      data: { url, hint, deep_analysis: true },
    });
    return { response, data: response.ok() ? await response.json() : null };
  }

  async quickAddSource(url: string, name?: string, autoStart = true): Promise<any> {
    const response = await this.request.post(`${API_URL}/api/quick-add`, {
      headers: this.getHeaders(),
      data: { url, name, auto_start: autoStart },
    });
    return { response, data: response.ok() ? await response.json() : null };
  }

  async testCrawl(url: string, fields: FieldDefinition[], maxRecords = 10): Promise<any> {
    const response = await this.request.post(`${API_URL}/api/quick-add/test`, {
      headers: this.getHeaders(),
      data: { url, fields, max_records: maxRecords },
    });
    return { response, data: response.ok() ? await response.json() : null };
  }

  // ========== Crawlers ==========

  async listCrawlers(params?: { source_id?: string; status?: string }): Promise<Crawler[]> {
    const queryParams = new URLSearchParams();
    if (params?.source_id) queryParams.append('source_id', params.source_id);
    if (params?.status) queryParams.append('status', params.status);

    const url = `${API_URL}/api/crawlers${queryParams.toString() ? '?' + queryParams : ''}`;
    const response = await this.request.get(url, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getCrawler(crawlerId: string): Promise<Crawler | null> {
    const response = await this.request.get(`${API_URL}/api/crawlers/${crawlerId}`, {
      headers: this.getHeaders(),
    });
    if (!response.ok()) return null;
    return response.json();
  }

  async getCrawlerCode(crawlerId: string): Promise<{ code: string; version: number } | null> {
    const response = await this.request.get(`${API_URL}/api/crawlers/${crawlerId}/code`, {
      headers: this.getHeaders(),
    });
    if (!response.ok()) return null;
    return response.json();
  }

  async getCrawlerHistory(crawlerId: string): Promise<any[]> {
    const response = await this.request.get(`${API_URL}/api/crawlers/${crawlerId}/history`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async rollbackCrawler(crawlerId: string, version: number): Promise<any> {
    const response = await this.request.post(`${API_URL}/api/crawlers/${crawlerId}/rollback/${version}`, {
      headers: this.getHeaders(),
    });
    return { response, data: response.ok() ? await response.json() : null };
  }

  // ========== Reviews ==========

  async getReviewDashboard(): Promise<any> {
    const response = await this.request.get(`${API_URL}/api/reviews/dashboard`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getReviewQueue(params?: {
    source_id?: string;
    status?: string;
    limit?: number;
  }): Promise<any[]> {
    const queryParams = new URLSearchParams();
    if (params?.source_id) queryParams.append('source_id', params.source_id);
    if (params?.status) queryParams.append('status', params.status);
    if (params?.limit) queryParams.append('limit', params.limit.toString());

    const url = `${API_URL}/api/reviews/queue${queryParams.toString() ? '?' + queryParams : ''}`;
    const response = await this.request.get(url, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getNextReview(currentId?: string, direction = 'forward'): Promise<any> {
    const queryParams = new URLSearchParams();
    if (currentId) queryParams.append('current_id', currentId);
    queryParams.append('direction', direction);

    const url = `${API_URL}/api/reviews/next${queryParams.toString() ? '?' + queryParams : ''}`;
    const response = await this.request.get(url, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async updateReview(
    reviewId: string,
    status: string,
    reviewerId: string,
    corrections?: any[],
    notes?: string
  ): Promise<any> {
    const response = await this.request.put(
      `${API_URL}/api/reviews/${reviewId}?reviewer_id=${reviewerId}`,
      {
        headers: this.getHeaders(),
        data: { status, corrections, notes },
      }
    );
    return { response, data: response.ok() ? await response.json() : null };
  }

  async batchApprove(reviewIds: string[], reviewerId: string): Promise<any> {
    const response = await this.request.post(
      `${API_URL}/api/reviews/batch-approve?reviewer_id=${reviewerId}`,
      {
        headers: this.getHeaders(),
        data: reviewIds,
      }
    );
    return { response, data: response.ok() ? await response.json() : null };
  }

  // ========== Dashboard ==========

  async getDashboardStats(): Promise<DashboardStats> {
    const response = await this.request.get(`${API_URL}/api/dashboard`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getExecutionTrends(days = 7): Promise<any> {
    const response = await this.request.get(`${API_URL}/api/dashboard/execution-trends?days=${days}`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getSystemHealth(): Promise<any> {
    const response = await this.request.get(`${API_URL}/api/dashboard/system-health`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getRecentActivity(hours = 24): Promise<any> {
    const response = await this.request.get(`${API_URL}/api/dashboard/recent-activity?hours=${hours}`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  // ========== Monitoring ==========

  async getPipelineStatus(): Promise<any[]> {
    const response = await this.request.get(`${API_URL}/api/monitoring/pipelines`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }

  async getHealingSessions(): Promise<any[]> {
    const response = await this.request.get(`${API_URL}/api/monitoring/healing-sessions`, {
      headers: this.getHeaders(),
    });
    expect(response.ok()).toBeTruthy();
    return response.json();
  }
}

/**
 * Create API helper with authentication
 */
export async function createAuthenticatedApiHelper(
  request: APIRequestContext,
  username = 'admin',
  password = process.env.ADMIN_PASSWORD || 'admin123'
): Promise<ApiHelper> {
  const helper = new ApiHelper(request);
  await helper.login(username, password);
  return helper;
}

/**
 * Create API helper without authentication
 */
export function createApiHelper(request: APIRequestContext): ApiHelper {
  return new ApiHelper(request);
}
