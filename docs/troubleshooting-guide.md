# USASpending ETL Pipeline - Troubleshooting Guide

## Quick Reference

### Common Commands
```bash
# Check pipeline status
python scripts/monitor.py --status

# Run diagnostics
python scripts/run_diagnostics.py

# View recent logs
tail -f logs/pipeline.log

# Test configuration
python scripts/setup.py --validate-config
```

### Emergency Procedures
```bash
# Stop all pipelines
pkill -f "python scripts/"

# Emergency cleanup
python scripts/maintenance.py --emergency-cleanup

# Recovery mode
python scripts/run_historical.py --recovery-mode
```

## Issue Categories

### 1. Connection Issues

#### Database Connection Failures

**Error Patterns:**
- "Failed to connect to database"
- "Connection refused"
- "Authentication failed"

**Diagnostic Steps:**
1. Test database connectivity:
   ```bash
   psql -h $ETL_DATABASE_HOST -p $ETL_DATABASE_PORT -U $ETL_DATABASE_USER -d $ETL_DATABASE_NAME
   ```

2. Check database service:
   ```bash
   systemctl status postgresql
   ```

3. Verify network connectivity:
   ```bash
   telnet $ETL_DATABASE_HOST $ETL_DATABASE_PORT
   ```

**Solutions:**
- Restart database service
- Check firewall rules
- Verify credentials
- Check connection string format

#### API Connection Issues

**Error Patterns:**
- "HTTP 429: Rate limit exceeded"
- "Connection timeout"
- "SSL certificate verification failed"

**Diagnostic Steps:**
1. Test API endpoint:
   ```bash
   curl -I "https://api.usaspending.gov/api/v2/awards/"
   ```

2. Check API rate limits:
   ```bash
   grep -i "rate limit\|429" logs/*.log | tail -10
   ```

**Solutions:**
- Implement exponential backoff
- Reduce request frequency
- Contact API provider for limits
- Check SSL certificate validity

### 2. Data Processing Issues

#### Data Quality Problems

**Error Patterns:**
- High duplicate rates
- Unexpected NULL values
- Schema validation failures

**Diagnostic Commands:**
```bash
# Check data quality metrics
python scripts/run_diagnostics.py --data-quality

# Analyze duplicates
python scripts/run_diagnostics.py --duplicates-only

# Schema validation
python scripts/run_diagnostics.py --schema-check
```

**Solutions:**
- Review deduplication logic
- Check data transformation rules
- Validate source data quality
- Update schema if needed

#### Performance Degradation

**Symptoms:**
- Slow pipeline execution
- High memory usage
- Database locks

**Diagnostic Commands:**
```bash
# Performance analysis
python scripts/monitor.py --performance-report

# Database performance
python scripts/maintenance.py --db-performance

# Resource utilization
python scripts/monitor.py --resource-usage
```

**Solutions:**
- Optimize database queries
- Add missing indexes
- Increase batch sizes
- Scale infrastructure

### 3. Storage Issues

#### Disk Space Problems

**Error Patterns:**
- "No space left on device"
- "Disk quota exceeded"

**Diagnostic Commands:**
```bash
# Check disk usage
df -h
du -sh work/* | sort -hr

# Storage analysis
python scripts/monitor.py --storage --detailed
```

**Solutions:**
```bash
# Emergency cleanup
python scripts/maintenance.py --emergency-cleanup

# Run retention policies
python scripts/maintenance.py --retention --aggressive

# Archive old data
python scripts/maintenance.py --archive --days 30
```

#### Database Growth Issues

**Symptoms:**
- Rapidly growing database size
- Slow query performance
- Table bloat

**Diagnostic Commands:**
```bash
# Database size analysis
python scripts/maintenance.py --db-size-analysis

# Table bloat check
python scripts/maintenance.py --bloat-check
```

**Solutions:**
```bash
# Vacuum and analyze
python scripts/maintenance.py --vacuum --analyze

# Reindex tables
python scripts/maintenance.py --reindex

# Implement partitioning
python scripts/maintenance.py --partition-tables
```

### 4. Configuration Issues

#### Environment Configuration

**Error Patterns:**
- "Configuration validation failed"
- "Missing required setting"
- "Invalid configuration value"

**Diagnostic Commands:**
```bash
# Validate configuration
python scripts/setup.py --validate-config

# Show configuration sources
python scripts/setup.py --config-sources

# Test configuration
python scripts/setup.py --test-config
```

**Solutions:**
- Check environment variables
- Validate config file syntax
- Review configuration inheritance
- Update configuration schema

### 5. Pipeline Execution Issues

#### Orchestration Failures

**Error Patterns:**
- Pipeline hangs indefinitely
- Processes fail to start
- Inconsistent execution results

**Diagnostic Commands:**
```bash
# Check running processes
ps aux | grep python

# Pipeline status
python scripts/monitor.py --pipeline-status

# Execution history
python scripts/monitor.py --execution-history
```

**Solutions:**
- Kill hanging processes
- Clear work directory locks
- Reset pipeline state
- Check resource availability

#### Data Consistency Issues

**Symptoms:**
- Missing data for some dates
- Inconsistent record counts
- Failed data validation

**Diagnostic Commands:**
```bash
# Data consistency check
python scripts/run_diagnostics.py --consistency

# Date range analysis
python scripts/run_diagnostics.py --date-gaps

# Record count validation
python scripts/run_diagnostics.py --record-counts
```

**Solutions:**
- Re-run failed date ranges
- Check watermark logic
- Validate merge operations
- Review incremental logic

## Advanced Troubleshooting

### Log Analysis

#### Centralized Logging
```bash
# View all logs
tail -f logs/*.log

# Filter by severity
grep -i error logs/*.log | tail -20
grep -i warning logs/*.log | tail -20

# Search by timestamp
grep "2024-01-01" logs/*.log

# JSON log analysis
cat logs/pipeline.log | jq '.level="ERROR"'
```

#### Log Rotation
```bash
# Archive old logs
python scripts/maintenance.py --archive-logs

# Compress logs
python scripts/maintenance.py --compress-logs

# Clean up logs
python scripts/maintenance.py --cleanup-logs --days 7
```

### Database Debugging

#### Query Analysis
```sql
-- Check long-running queries
SELECT query, query_start, state, wait_event_type
FROM pg_stat_activity
WHERE state = 'active' AND query_start < now() - interval '5 minutes';

-- Check table sizes
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname IN ('s1_raw', 's2_interim', 's3_processed')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

#### Lock Analysis
```sql
-- Check for locks
SELECT blocked_locks.pid AS blocked_pid,
       blocked_activity.usename AS blocked_user,
       blocking_locks.pid AS blocking_pid,
       blocking_activity.usename AS blocking_user,
       blocked_activity.query AS blocked_statement,
       blocking_activity.query AS current_statement_in_blocking_process
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
```

### Performance Profiling

#### Pipeline Profiling
```bash
# Profile pipeline execution
python -m cProfile -o profile.stats scripts/run_incremental.py

# Analyze profile results
python -c "import pstats; p = pstats.Stats('profile.stats'); p.sort_stats('cumulative'); p.print_stats(20)"

# Memory profiling
python -m memory_profiler scripts/run_incremental.py
```

#### Database Profiling
```bash
# Enable query logging
echo "log_statement = 'all'" >> postgresql.conf
echo "log_duration = on" >> postgresql.conf

# Analyze slow queries
python scripts/maintenance.py --slow-query-report
```

## Recovery Procedures

### Partial Recovery

#### Single Table Recovery
```bash
# Identify affected table
python scripts/run_diagnostics.py --table s3_processed.awards

# Backup current data
pg_dump -t s3_processed.awards usaspending_etl > backup_awards.sql

# Re-process specific date range
python scripts/run_historical.py --table awards --start-date 2024-01-01 --end-date 2024-01-07

# Validate recovery
python scripts/run_diagnostics.py --table s3_processed.awards --date-range 2024-01-01 2024-01-07
```

#### Date Range Recovery
```bash
# Clear affected date range
python scripts/maintenance.py --clear-date-range 2024-01-01 2024-01-07

# Re-process data
python scripts/run_historical.py --start-date 2024-01-01 --end-date 2024-01-07

# Validate results
python scripts/run_diagnostics.py --date-range 2024-01-01 2024-01-07
```

### Full System Recovery

#### Complete Database Recovery
```bash
# Stop all pipelines
pkill -f "python scripts/"

# Backup current state
pg_dumpall > full_backup.sql

# Restore from backup
dropdb usaspending_etl
createdb usaspending_etl
psql usaspending_etl < database_backup.sql

# Initialize system
python scripts/setup.py --init-database

# Validate restoration
python scripts/run_diagnostics.py --full
```

#### Configuration Recovery
```bash
# Reset to default configuration
cp config/default.json config/current.json

# Restore from backup
python scripts/setup.py --import-config --file config/backup.json

# Validate configuration
python scripts/setup.py --validate-config
```

## Prevention Strategies

### Monitoring Setup

#### Automated Alerts
```bash
# Setup email alerts for critical issues
python scripts/setup.py --configure-alerts --email admin@company.com

# Setup Slack notifications
python scripts/setup.py --configure-slack --webhook-url "https://hooks.slack.com/..."

# Configure monitoring thresholds
python scripts/setup.py --set-thresholds --error-rate 10 --storage-usage 80
```

#### Health Checks
```bash
# Setup automated health checks
crontab -e
# Add: */15 * * * * /path/to/python scripts/monitor.py --health-check --alert-on-failure

# Setup daily reports
# Add: 0 9 * * * /path/to/python scripts/monitor.py --daily-report --email
```

### Backup Automation

#### Database Backups
```bash
# Setup automated backups
crontab -e
# Daily backup: 0 2 * * * /path/to/scripts/backup_database.sh
# Weekly full backup: 0 1 * * 0 /path/to/scripts/full_backup.sh
```

#### Configuration Backups
```bash
# Setup config backups
crontab -e
# Daily config backup: 0 3 * * * /path/to/python scripts/setup.py --backup-config
```

### Capacity Planning

#### Storage Monitoring
```bash
# Setup storage alerts
crontab -e
# Hourly storage check: 0 * * * * /path/to/python scripts/monitor.py --storage --alert-threshold 90
```

#### Performance Baseline
```bash
# Establish performance baselines
python scripts/monitor.py --establish-baseline

# Regular performance checks
crontab -e
# Weekly performance report: 0 8 * * 1 /path/to/python scripts/monitor.py --performance-report --baseline-compare
```

## Support Contacts

### Internal Support
- **Operations Team**: operations@company.com
- **Engineering Team**: engineering@company.com
- **Database Admin**: dba@company.com

### External Support
- **USASpending.gov API Support**: [API Documentation](https://api.usaspending.gov/)
- **PostgreSQL Community**: [PostgreSQL Support](https://www.postgresql.org/support/)

---

*Last Updated: [Current Date]*
*Document Version: 1.0*