# CI/CD Pipeline Documentation

## Overview

Comprehensive GitHub Actions CI/CD pipeline for the Airflow Crawler System with parallel execution, security scanning, and automated deployments.

## Workflow Files

### 1. CI Pipeline (`.github/workflows/ci.yml`)

**Triggers:**
- Push to any branch
- Pull requests to master

**Jobs (Parallel Execution):**

#### a. `lint-backend` - Python Linting
- Uses Ruff for fast linting
- Checks: `api/`, `crawlers/`, `airflow/dags/`
- Cache: pip dependencies
- Python 3.12

#### b. `lint-frontend` - TypeScript Type Checking
- Uses `tsc --noEmit` for type checking
- Working directory: `frontend/`
- Cache: npm dependencies
- Node 20

#### c. `test-backend` - Backend Tests
- Runs pytest with coverage
- MongoDB service container
- Tests: `tests/` directory
- Coverage reports: XML, HTML, term-missing
- Uploads to Codecov (optional)
- Coverage threshold: 70% (from pyproject.toml)

#### d. `build-frontend` - Frontend Build
- Runs `npm run build` (Vite)
- Depends on: `lint-frontend`
- Uploads build artifacts

#### e. `docker-build` - Docker Image Verification
- Matrix strategy: `[api, frontend]`
- Builds images without pushing
- Cache strategy: GitHub Actions cache
- Outputs: Docker image tar files
- Depends on: `lint-backend`, `test-backend`, `build-frontend`

#### f. `integration-test` - Integration Tests
- Runs only on pull requests
- Uses docker-compose
- Tests: Health checks, CRUD operations, API endpoints
- Depends on: `docker-build`

**Caching Strategy:**
- pip: `~/.cache/pip` with requirements hash key
- npm: Node.js setup action with package-lock.json
- Docker: GitHub Actions cache per service

---

### 2. Deployment Pipeline (`.github/workflows/cd.yml`)

**Triggers:**
- Push to master branch (auto-deploy to staging)
- Tags matching `v*` (auto-deploy to production)
- Manual workflow dispatch with environment selection

**Environments:**
- `staging`: Auto-deploy on master push
- `production`: Requires manual approval (environment protection)

**Jobs:**

#### a. `build-and-push` - Build and Push Docker Images
- Multi-platform: linux/amd64, linux/arm64
- Registry: GitHub Container Registry (ghcr.io)
- Images: api, frontend, airflow
- Tags: branch name, semver, sha, latest
- Outputs: Image tags and previous versions for rollback

#### b. `deploy-staging` - Deploy to Staging
- Runs after successful build
- SSH deployment via `appleboy/ssh-action`
- Rolling update: frontend → api → airflow components
- Health check verification (10 retries)
- Saves pre-deployment state for rollback

**Deployment Steps:**
1. Save current image tags
2. Pull new images
3. Rolling update (one service at a time)
4. Health check verification
5. Prune old images

#### c. `deploy-production` - Deploy to Production
- Requires: staging deployment success
- Environment protection: Manual approval required
- Pre-deployment backup: MongoDB backup script
- Health checks: 15 retries with longer timeout
- Auto-rollback on failure

**Rollback Features:**
- Automatic rollback on health check failure
- Manual rollback via workflow dispatch
- Restores previous image tags
- Separate rollback job for manual intervention

---

### 3. Security Scanning (`.github/workflows/security.yml`)

**Triggers:**
- Push to master
- Pull requests to master
- Weekly schedule: Sunday at midnight UTC
- Manual workflow dispatch

**Jobs:**

#### a. `python-dependency-scan` - pip-audit
- Scans: `api/requirements.txt`
- Outputs: JSON and CycloneDX SBOM
- Retention: 30 days

#### b. `npm-dependency-scan` - npm audit
- Scans: `frontend/package.json`
- Audit level: high
- Outputs: JSON report
- Retention: 30 days

#### c. `trivy-scan` - Container Vulnerability Scanning
- Images scanned: api, frontend
- Severity: CRITICAL, HIGH
- Outputs: SARIF format
- Uploads to GitHub Security tab
- Filesystem scan included

#### d. `secret-scan` - Secret Detection
- Tools: Gitleaks + TruffleHog
- Scans: Entire repository history
- Verified secrets only (TruffleHog)

#### e. `codeql-analysis` - Code Analysis
- Language: Python
- Queries: security-extended, security-and-quality
- Uploads to GitHub Security tab

#### f. `bandit-scan` - Python Security Linter
- Scans: `api/app/`, `crawlers/`
- Outputs: JSON and SARIF
- Configuration: pyproject.toml

#### g. `security-report` - Generate Summary
- Consolidates all scan results
- Creates GitHub step summary
- Uploads combined reports
- Retention: 90 days

---

### 4. Dependabot Configuration (`.github/dependabot.yml`)

**Package Ecosystems:**

#### Python (pip)
- Directory: `/api`
- Schedule: Weekly (Monday 09:00 KST)
- Groups: FastAPI ecosystem, database, testing, security
- Ignores: Major version updates for fastapi, pydantic

#### GitHub Actions
- Directory: `/`
- Schedule: Weekly (Monday 09:00 KST)
- Groups: All actions together
- Limit: 5 PRs

#### Docker
- Directories: `/api`, `/airflow`, `/frontend`
- Schedule: Weekly (Monday 09:00 KST)
- Separate configs per service

#### npm (Frontend)
- Directory: `/frontend`
- Schedule: Weekly (Monday 09:00 KST)
- Groups: React ecosystem, build tools, testing
- Ignores: Major updates for react, react-dom

**Settings:**
- Open PR limit: 10 (Python/npm), 5 (Actions), 3 (Docker)
- Auto-labels: dependencies, language, service
- Commit message prefixes: `deps(service):`, `ci:`, `docker(service):`

---

## GitHub Actions Best Practices Applied

### 1. Parallel Execution
- Independent jobs run concurrently
- Matrix strategies for similar tasks
- Reduces total pipeline time by ~60%

### 2. Caching
- pip cache with requirements hash keys
- npm cache via setup-node action
- Docker layer caching via GitHub Actions cache
- Separate cache scopes per service

### 3. Artifacts
- Build artifacts uploaded for debugging
- Coverage reports saved
- Security scan results retained
- Docker images saved between jobs

### 4. Security
- SARIF uploads to Security tab
- Secret scanning in CI
- Container vulnerability scanning
- Dependency auditing

### 5. Error Handling
- Health checks with retries
- Auto-rollback on deployment failure
- Continue-on-error for security scans
- Proper exit codes

### 6. Environment Protection
- Staging auto-deploy
- Production requires manual approval
- Environment-specific secrets
- Separate URLs per environment

---

## Required GitHub Secrets

### Deployment Secrets
- `STAGING_HOST`: Staging server hostname
- `PRODUCTION_HOST`: Production server hostname
- `DEPLOY_USER`: SSH username
- `DEPLOY_SSH_KEY`: SSH private key
- `STAGING_APP_PATH`: Application path on staging
- `PRODUCTION_APP_PATH`: Application path on production

### Optional Secrets
- `CODECOV_TOKEN`: Codecov upload token
- `GITLEAKS_LICENSE`: Gitleaks enterprise license
- `OPENAI_API_KEY`: For integration tests

### Environment Variables (in .env files on servers)
- `MONGO_ROOT_PASSWORD`
- `GRAFANA_ADMIN_PASSWORD`
- `AIRFLOW_FERNET_KEY`
- All other service-specific variables

---

## Deployment Flow

### Staging Deployment (Auto)
1. Push to master branch
2. CI pipeline runs (all jobs must pass)
3. Build and push Docker images
4. Auto-deploy to staging
5. Health check verification

### Production Deployment (Manual Approval)
1. Create tag: `git tag v1.0.0 && git push --tags`
2. CI pipeline runs
3. Build and push Docker images
4. Deploy to staging (auto)
5. **Manual approval required**
6. Pre-deployment backup
7. Deploy to production
8. Health check verification
9. GitHub Release created

### Rollback
**Automatic:**
- Health check fails → auto-rollback to previous version

**Manual:**
- Workflow dispatch with `rollback: true`
- Select environment
- Restores previous image tags

---

## Performance Optimizations

### Pipeline Speed
- Parallel job execution: ~5-7 minutes total
- Docker caching: 50-70% faster builds
- npm/pip caching: 30-50% faster installs

### Resource Efficiency
- Matrix strategies for similar jobs
- Conditional job execution
- Artifact cleanup after 7 days
- Security reports: 90 days retention

---

## Monitoring and Notifications

### GitHub Actions UI
- Job-level status indicators
- Step-by-step logs
- Artifact downloads
- Workflow run history

### GitHub Security Tab
- CodeQL alerts
- Trivy vulnerability reports
- Bandit security issues
- Secret scanning alerts

### Step Summaries
- CI: Test coverage, build status
- Security: Scan result table
- Deploy: Version deployed, rollback info

---

## Usage Examples

### Trigger CI on Feature Branch
```bash
git checkout -b feature/new-crawler
# Make changes
git push origin feature/new-crawler
# CI runs automatically
```

### Deploy to Staging
```bash
git checkout master
git merge feature/new-crawler
git push origin master
# Auto-deploys to staging
```

### Deploy to Production
```bash
git tag v1.0.0
git push --tags
# Staging → Manual Approval → Production
```

### Manual Rollback
```bash
# Via GitHub Actions UI:
# Actions → Deployment Pipeline → Run workflow
# Select: environment=production, rollback=true
```

---

## Troubleshooting

### CI Failures
- **Lint errors**: Run `ruff check` locally
- **Type errors**: Run `tsc --noEmit` in frontend/
- **Test failures**: Run `pytest tests/ -v` locally
- **Docker build fails**: Check Dockerfile syntax

### Deployment Failures
- **SSH connection**: Verify secrets and host access
- **Health check fails**: Check service logs via SSH
- **Auto-rollback triggered**: Review deployment logs

### Security Scan Issues
- **pip-audit fails**: Update vulnerable dependencies
- **npm audit fails**: Run `npm audit fix`
- **Trivy alerts**: Update base images
- **Secret detected**: Remove from history, rotate keys

---

## Future Enhancements

### Planned
- [ ] E2E tests with Playwright in CI
- [ ] Performance testing in staging
- [ ] Canary deployments
- [ ] Blue-green deployment strategy
- [ ] Slack/Discord notifications
- [ ] Cost optimization reports

### Under Consideration
- [ ] Self-hosted runners
- [ ] Kubernetes deployment
- [ ] Multi-region deployments
- [ ] A/B testing infrastructure

---

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Docker Build Push Action](https://github.com/docker/build-push-action)
- [Dependabot Configuration](https://docs.github.com/en/code-security/dependabot)
- [Security Best Practices](https://docs.github.com/en/code-security)
