# CI/CD Pipeline Verification Checklist

## Pre-Deployment Verification

### 1. GitHub Secrets Configuration

**Required Secrets:**
- [ ] `STAGING_HOST` - Staging server hostname
- [ ] `PRODUCTION_HOST` - Production server hostname
- [ ] `DEPLOY_USER` - SSH deployment user
- [ ] `DEPLOY_SSH_KEY` - SSH private key (with proper permissions)
- [ ] `STAGING_APP_PATH` - Application path on staging server
- [ ] `PRODUCTION_APP_PATH` - Application path on production server

**Optional Secrets:**
- [ ] `CODECOV_TOKEN` - For code coverage uploads
- [ ] `GITLEAKS_LICENSE` - For Gitleaks enterprise features
- [ ] `OPENAI_API_KEY` - For integration tests

**Location:** Settings → Secrets and variables → Actions

---

### 2. Environment Protection Rules

**Staging Environment:**
- [ ] Name: `staging`
- [ ] URL: https://staging.example.com (update in cd.yml)
- [ ] Protection: None (auto-deploy)

**Production Environment:**
- [ ] Name: `production`
- [ ] URL: https://example.com (update in cd.yml)
- [ ] Protection: Required reviewers (at least 1)
- [ ] Deployment branches: master and tags matching v*

**Location:** Settings → Environments

---

### 3. Repository Settings

**GitHub Actions:**
- [ ] Actions enabled: Settings → Actions → General
- [ ] Workflow permissions: Read and write
- [ ] Allow GitHub Actions to create PRs: Enabled (for Dependabot)

**Security:**
- [ ] Code scanning: Enabled (Settings → Security → Code scanning)
- [ ] Secret scanning: Enabled
- [ ] Dependabot alerts: Enabled
- [ ] Dependabot security updates: Enabled

**Packages:**
- [ ] GitHub Packages enabled (for ghcr.io)
- [ ] Package permissions: Public or private based on needs

---

### 4. Server Configuration

**Staging Server:**
- [ ] Docker & Docker Compose installed
- [ ] SSH access with deploy key
- [ ] Application directory exists: `/opt/crawler-system` (or custom path)
- [ ] `.env` file configured
- [ ] Docker network created: `crawler-network`
- [ ] Firewall allows SSH (port 22)
- [ ] Service ports accessible: 8000 (API), 3000 (Frontend), 8080 (Airflow)

**Production Server:**
- [ ] Docker & Docker Compose installed
- [ ] SSH access with deploy key
- [ ] Application directory exists
- [ ] `.env` file configured (production values)
- [ ] Backup scripts in place: `scripts/backup.sh`
- [ ] Monitoring configured (Prometheus, Grafana)
- [ ] SSL certificates configured (if using HTTPS)

**Test SSH Connection:**
```bash
ssh -i /path/to/deploy_key DEPLOY_USER@STAGING_HOST
ssh -i /path/to/deploy_key DEPLOY_USER@PRODUCTION_HOST
```

---

### 5. YAML Syntax Validation

**Validate locally:**
```bash
# Install yamllint
pip install yamllint

# Validate workflow files
yamllint .github/workflows/ci.yml
yamllint .github/workflows/cd.yml
yamllint .github/workflows/security.yml
yamllint .github/dependabot.yml
```

**Or use online validator:**
- https://www.yamllint.com/

---

### 6. Docker Registry Authentication

**GitHub Container Registry:**
- [ ] Personal Access Token (PAT) created with `write:packages` scope
- [ ] Test login: `echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin`
- [ ] Organization permissions configured (if using org repo)

**Image naming convention:**
```
ghcr.io/OWNER/REPO/api:latest
ghcr.io/OWNER/REPO/frontend:latest
ghcr.io/OWNER/REPO/airflow:latest
```

---

## Post-Deployment Verification

### 7. CI Pipeline Testing

**Test Scenario 1: Feature Branch**
```bash
git checkout -b test/ci-verification
echo "test" > test.txt
git add test.txt
git commit -m "test: CI pipeline verification"
git push origin test/ci-verification
```

**Expected Results:**
- [ ] `lint-backend` job passes
- [ ] `lint-frontend` job passes
- [ ] `test-backend` job passes
- [ ] `build-frontend` job passes
- [ ] `docker-build` job passes (matrix: api, frontend)
- [ ] All jobs complete in ~5-7 minutes

**Test Scenario 2: Pull Request**
```bash
# Create PR from test branch to master
```

**Expected Results:**
- [ ] All CI jobs run
- [ ] `integration-test` job runs (only on PRs)
- [ ] Status checks appear on PR
- [ ] Coverage report uploaded

---

### 8. Security Scanning Verification

**Trigger security scan:**
```bash
# Manual trigger
Actions → Security Scan → Run workflow

# Or wait for weekly schedule (Sunday)
```

**Expected Results:**
- [ ] `python-dependency-scan` completes
- [ ] `npm-dependency-scan` completes
- [ ] `trivy-scan` completes (3 scans: api, frontend, filesystem)
- [ ] `secret-scan` completes (Gitleaks + TruffleHog)
- [ ] `codeql-analysis` completes
- [ ] `bandit-scan` completes
- [ ] SARIF reports uploaded to Security tab
- [ ] Summary report generated

**Check Security Tab:**
- [ ] CodeQL alerts visible
- [ ] Trivy alerts visible
- [ ] Bandit alerts visible
- [ ] No secrets detected

---

### 9. Staging Deployment Verification

**Deploy to staging:**
```bash
git checkout master
git merge test/ci-verification
git push origin master
```

**Expected Results:**
- [ ] CI pipeline passes
- [ ] `build-and-push` job builds images
- [ ] Images pushed to ghcr.io
- [ ] `deploy-staging` job runs
- [ ] SSH connection successful
- [ ] Images pulled on server
- [ ] Services restarted in order
- [ ] Health check passes

**Manual Verification:**
```bash
# SSH to staging
ssh DEPLOY_USER@STAGING_HOST

# Check running services
docker compose ps

# Check logs
docker compose logs api --tail 50
docker compose logs frontend --tail 50

# Test API
curl http://localhost:8000/health
curl http://localhost:8000/

# Test Frontend
curl http://localhost:3000/
```

---

### 10. Production Deployment Verification

**Deploy to production:**
```bash
git tag v1.0.0
git push --tags
```

**Expected Results:**
- [ ] CI pipeline passes
- [ ] Images built and pushed
- [ ] Staging deployment completes
- [ ] Approval request appears in GitHub Actions
- [ ] After approval: Pre-deployment backup runs
- [ ] Production deployment executes
- [ ] Health checks pass (15 retries)
- [ ] GitHub Release created

**Manual Verification:**
```bash
# SSH to production
ssh DEPLOY_USER@PRODUCTION_HOST

# Verify deployment
docker compose ps
docker compose logs api --tail 50

# Test services
curl http://localhost:8000/health
```

---

### 11. Rollback Testing

**Test auto-rollback:**
```bash
# Temporarily break health endpoint on server
# Deploy should fail and auto-rollback
```

**Test manual rollback:**
```bash
# GitHub Actions → Deployment Pipeline → Run workflow
# Select: environment=production, rollback=true
```

**Expected Results:**
- [ ] Previous image tags restored
- [ ] Services restarted with old images
- [ ] Health check passes
- [ ] No data loss

---

### 12. Dependabot Verification

**Check Dependabot configuration:**
- [ ] Settings → Security → Dependabot
- [ ] Dependency updates enabled
- [ ] PRs created weekly (Monday 09:00 KST)

**Expected Behavior:**
- [ ] Separate PRs for each ecosystem
- [ ] Grouped updates (FastAPI ecosystem, React ecosystem, etc.)
- [ ] Proper labels applied
- [ ] Commit message prefixes correct

---

## Performance Benchmarks

**Record baseline metrics:**

| Workflow | Expected | Actual | Status |
|----------|----------|--------|--------|
| CI (full) | 5-7 min | ___ min | ⬜ |
| CI (cached) | 3-5 min | ___ min | ⬜ |
| Deploy (staging) | 3-5 min | ___ min | ⬜ |
| Deploy (production) | 5-8 min | ___ min | ⬜ |
| Security scan | 8-12 min | ___ min | ⬜ |

---

## Common Issues & Solutions

### Issue: SSH Connection Failed
**Symptoms:** Deploy job fails at SSH connection
**Solutions:**
1. Verify SSH key format (PEM, no passphrase)
2. Check server firewall allows SSH
3. Verify `DEPLOY_USER` has correct permissions
4. Test manual SSH: `ssh -i key DEPLOY_USER@HOST`

### Issue: Health Check Timeout
**Symptoms:** Deploy completes but health check fails
**Solutions:**
1. Check service logs: `docker compose logs api`
2. Verify MongoDB is running: `docker compose ps mongodb`
3. Check environment variables in `.env`
4. Increase health check retries in workflow

### Issue: Docker Build Fails
**Symptoms:** `docker-build` job fails
**Solutions:**
1. Check Dockerfile syntax
2. Verify base images exist
3. Check build context paths
4. Review build logs for specific errors

### Issue: Coverage Below Threshold
**Symptoms:** `test-backend` fails with coverage error
**Solutions:**
1. Add tests for uncovered code
2. Temporarily lower threshold in `pyproject.toml`
3. Check coverage report: Download artifact

### Issue: Security Scan Alerts
**Symptoms:** Security tab shows vulnerabilities
**Solutions:**
1. Review severity (CRITICAL > HIGH > MEDIUM)
2. Update dependencies: `pip install --upgrade <package>`
3. Check for patches or workarounds
4. Add exceptions if false positive

---

## Sign-Off

**Verified by:** _______________
**Date:** _______________
**Version:** v1.0.0

**Checklist Summary:**
- [ ] All secrets configured
- [ ] Environments configured
- [ ] Servers prepared
- [ ] CI pipeline tested
- [ ] Security scanning verified
- [ ] Staging deployment successful
- [ ] Production deployment successful
- [ ] Rollback tested
- [ ] Dependabot active
- [ ] Documentation reviewed

**Notes:**
_Add any environment-specific notes or exceptions here_

---

## Next Steps

After verification:
1. [ ] Train team on deployment process
2. [ ] Document rollback procedures
3. [ ] Set up monitoring alerts
4. [ ] Configure backup schedules
5. [ ] Plan for disaster recovery testing
