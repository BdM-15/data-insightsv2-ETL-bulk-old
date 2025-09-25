# USASpending ETL Pipeline - Operations Runbook

## Table of Contents

1. [System Overview](#system-overview)
2. [Daily Operations](#daily-operations)
3. [Monitoring and Alerting](#monitoring-and-alerting)
4. [Troubleshooting Guide](#troubleshooting-guide)
5. [Maintenance Procedures](#maintenance-procedures)
6. [Disaster Recovery](#disaster-recovery)
7. [Performance Optimization](#performance-optimization)
8. [Configuration Management](#configuration-management)

## System Overview

### Architecture Components

- **Data Sources**: USASpending.gov API
- **Storage Layers**:
  - `s1_raw`: Raw API responses
  - `s2_interim`: Processed/cleaned data
  - `s3_processed`: Final analytical data with semantic descriptions
- **Orchestration**: Python scripts for historical, incremental, and diagnostic operations
- **Monitoring**: Metrics collection and validation suite

### Key File Locations

```
├── etl/                    # Core ETL modules
├── scripts/                # Orchestration scripts
├── sql/                    # Database schema and migrations
├── config/                 # Environment configurations
├── work/                   # Runtime data directory
└── logs/                   # Application logs
```

## Daily Operations

### Morning Checklist (9:00 AM)

1. **Check Pipeline Status**

   ```bash
   python scripts/monitor.py --status
   ```

2. **Review Overnight Runs**

   ```bash
   python scripts/monitor.py --recent-failures --hours 12
   ```

3. **Validate Data Quality**

   ```bash
   python scripts/run_diagnostics.py --quick
   ```

4. **Check Storage Usage**
   ```bash
   python scripts/monitor.py --storage
   ```

### Evening Operations (6:00 PM)

1. **Run Incremental Pipeline**

   ```bash
   python scripts/run_incremental.py
   ```

2. **Execute Data Retention**

   ```bash
   python scripts/maintenance.py --retention
   ```

3. **Generate Daily Report**
   ```bash
   python scripts/monitor.py --daily-report
   ```

### Weekly Operations (Sundays)

1. **Full Validation Suite**

   ```bash
   python scripts/run_diagnostics.py --full
   ```

2. **Database Maintenance**

   ```bash
   python scripts/maintenance.py --vacuum --reindex
   ```

3. **Performance Analysis**
   ```bash
   python scripts/monitor.py --performance-report --days 7
   ```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Pipeline Health**

   - Success/failure rates
   - Execution duration trends
   - Data volume changes

2. **Data Quality**

   - Duplicate record rates
   - NULL value percentages
   - Schema compliance

3. **System Resources**
   - Database storage usage
   - Work directory disk space
   - API rate limit consumption

### Alert Thresholds

| Metric                | Warning   | Critical  |
| --------------------- | --------- | --------- |
| Pipeline failure rate | >10%      | >25%      |
| Data freshness        | >48 hours | >72 hours |
| Storage usage         | >80%      | >95%      |
| Duplicate rate        | >5%       | >15%      |

### Monitoring Commands

```bash
# Real-time pipeline status
python scripts/monitor.py --live

# Check for critical issues
python scripts/run_diagnostics.py --critical-only

# Generate metrics dashboard
python scripts/monitor.py --dashboard

# Export metrics for external systems
python scripts/monitor.py --export --format json
```

## Troubleshooting Guide

### Common Issues

#### 1. Pipeline Failure: "Database Connection Error"

**Symptoms:**

- Pipeline fails to start
- Error: "Failed to connect to database"

**Diagnosis:**

```bash
# Test database connectivity
python -c "from etl.config import Config; from etl.utils.config_manager import EnhancedConfigurationManager; cm = EnhancedConfigurationManager(); print(cm.validate_current_configuration())"
```

**Resolution:**

1. Check database service status
2. Verify connection parameters in config
3. Test network connectivity
4. Check database user permissions

#### 2. Pipeline Failure: "API Rate Limit Exceeded"

**Symptoms:**

- HTTP 429 errors in logs
- Pipeline execution stops during acquisition

**Diagnosis:**

```bash
# Check recent API call patterns
grep -i "rate limit\|429" logs/*.log | tail -20
```

**Resolution:**

1. Reduce `chunk_size` in configuration
2. Increase retry delays
3. Implement exponential backoff
4. Contact USASpending.gov for rate limit increase

#### 3. Data Quality Issues: High Duplicate Rate

**Symptoms:**

- Validation reports high duplicate percentage
- Data volume larger than expected

**Diagnosis:**

```bash
# Run duplicate analysis
python scripts/run_diagnostics.py --duplicates-only
```

**Resolution:**

1. Check incremental merge logic
2. Verify watermark management
3. Review deduplication rules
4. Consider manual cleanup if severe

#### 4. Storage Issues: Disk Space Full

**Symptoms:**

- Pipeline fails with "No space left on device"
- Storage monitoring shows >95% usage

**Diagnosis:**

```bash
# Check disk usage by component
python scripts/monitor.py --storage --detailed
```

**Resolution:**

1. Run emergency retention cleanup
   ```bash
   python scripts/maintenance.py --emergency-cleanup
   ```
2. Archive old data manually
3. Increase retention cleanup frequency
4. Provision additional storage

### Emergency Procedures

#### Pipeline Complete Failure

1. **Stop all running processes**

   ```bash
   pkill -f "python scripts/"
   ```

2. **Assess system state**

   ```bash
   python scripts/monitor.py --health-check
   ```

3. **Review recent logs**

   ```bash
   tail -100 logs/pipeline.log
   ```

4. **Attempt recovery**
   ```bash
   python scripts/run_historical.py --recovery-mode
   ```

#### Data Corruption

1. **Isolate affected data**

   - Identify corrupted date range
   - Mark data as quarantined in database

2. **Stop incremental processing**

   ```bash
   # Disable incremental pipeline
   touch work/.pipeline_disabled
   ```

3. **Restore from backup**

   - Use database backup
   - Re-run historical pipeline for affected period

4. **Validate restoration**
   ```bash
   python scripts/run_diagnostics.py --date-range YYYY-MM-DD YYYY-MM-DD
   ```

## Maintenance Procedures

### Database Maintenance

#### Weekly Database Health Check

```bash
# Run comprehensive database analysis
python scripts/maintenance.py --db-health-check

# Check for index usage
python scripts/maintenance.py --analyze-indexes

# Update table statistics
python scripts/maintenance.py --update-stats
```

#### Monthly Deep Clean

```bash
# Full vacuum and reindex
python scripts/maintenance.py --full-vacuum --reindex

# Cleanup old logs
python scripts/maintenance.py --cleanup-logs --days 30

# Archive old metrics
python scripts/maintenance.py --archive-metrics --days 90
```

### Configuration Updates

#### Updating Pipeline Configuration

```bash
# Validate new configuration
python -c "from etl.utils.config_manager import EnhancedConfigurationManager; cm = EnhancedConfigurationManager(); print(cm.validate_current_configuration())"

# Apply configuration changes
python scripts/setup.py --update-config

# Test configuration
python scripts/run_diagnostics.py --config-test
```

#### Environment Migration

```bash
# Export current configuration
python scripts/setup.py --export-config --file config/backup.json

# Deploy to new environment
python scripts/setup.py --import-config --file config/backup.json --env production

# Validate deployment
python scripts/run_diagnostics.py --full --env production
```

## Disaster Recovery

### Backup Strategy

1. **Database Backups**

   - Daily automated backups via pg_dump
   - Weekly full database backups
   - Monthly archived backups

2. **Configuration Backups**

   - Version controlled in Git
   - Environment-specific configs backed up daily

3. **Code Backups**
   - Source code in Git repository
   - Tagged releases for rollback capability

### Recovery Procedures

#### Complete System Recovery

1. **Infrastructure Setup**

   ```bash
   # Install dependencies
   python scripts/setup.py --install-all

   # Setup database
   python scripts/setup.py --init-database
   ```

2. **Restore Configuration**

   ```bash
   # Restore configuration from backup
   python scripts/setup.py --import-config --file backup/config.json
   ```

3. **Restore Data**

   ```bash
   # Restore database from backup
   psql usaspending_etl < backup/database_backup.sql

   # Verify data integrity
   python scripts/run_diagnostics.py --full
   ```

4. **Resume Operations**

   ```bash
   # Run incremental catch-up
   python scripts/run_incremental.py --catch-up

   # Verify pipeline health
   python scripts/monitor.py --health-check
   ```

## Performance Optimization

### Monitoring Performance Trends

```bash
# Generate performance report
python scripts/monitor.py --performance-report --days 30

# Identify slow queries
python scripts/maintenance.py --slow-query-analysis

# Check resource utilization
python scripts/monitor.py --resource-utilization
```

### Optimization Techniques

1. **Database Optimization**

   - Regular VACUUM ANALYZE
   - Index optimization
   - Query plan analysis

2. **Pipeline Optimization**

   - Parallel processing tuning
   - Batch size optimization
   - Memory usage optimization

3. **Storage Optimization**
   - Data retention tuning
   - Compression strategies
   - Partitioning for large tables

### Performance Tuning Commands

```bash
# Optimize database performance
python scripts/maintenance.py --optimize-db

# Tune pipeline parameters
python scripts/setup.py --tune-performance

# Analyze bottlenecks
python scripts/monitor.py --bottleneck-analysis
```

## Configuration Management

### Configuration Hierarchy

1. **Environment Variables** (highest priority)

   - Sensitive values (passwords, API keys)
   - Deployment-specific settings

2. **Environment Files** (medium priority)

   - `config/production.json`
   - `config/development.json`

3. **Default Configuration** (lowest priority)
   - `config/default.json`

### Configuration Commands

```bash
# View current configuration
python scripts/setup.py --show-config

# Validate configuration
python scripts/setup.py --validate-config

# Update specific setting
python scripts/setup.py --set database_host localhost

# Export configuration
python scripts/setup.py --export-config --file backup.json
```

### Security Best Practices

1. **Credential Management**

   - Store sensitive values in environment variables
   - Use encrypted configuration files
   - Regular credential rotation

2. **Access Controls**

   - Database user permissions
   - File system permissions
   - Network security

3. **Audit Trail**
   - Configuration change logging
   - Access logging
   - Regular security reviews

## Contact Information

### Support Escalation

1. **Level 1: Automated Recovery**

   - Self-healing pipelines
   - Automatic retry mechanisms

2. **Level 2: Operations Team**

   - Daily monitoring and maintenance
   - Standard troubleshooting procedures

3. **Level 3: Engineering Team**
   - Complex issue resolution
   - System modifications and updates

### Emergency Contacts

- Operations Team: [operations@company.com]
- Engineering Team: [engineering@company.com]
- Database Admin: [dba@company.com]
- Infrastructure Team: [infrastructure@company.com]

---

_Last Updated: [Current Date]_
_Document Version: 1.0_
