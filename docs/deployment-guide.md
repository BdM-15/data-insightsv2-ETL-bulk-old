# USASpending ETL Pipeline - Deployment Guide

## Prerequisites

### System Requirements

- **OS**: Linux (Ubuntu 20.04+ recommended) or Windows 10+
- **Python**: 3.13+
- **Database**: PostgreSQL 14+ with pgvector extension
- **Memory**: 8GB RAM minimum, 16GB recommended
- **Storage**: 100GB+ available disk space
- **Network**: Stable internet connection for API access

### Required Software

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y python3.13 python3.13-venv postgresql postgresql-contrib git curl

# CentOS/RHEL
sudo yum update
sudo yum install -y python313 python313-venv postgresql postgresql-contrib git curl

# Windows (via chocolatey)
choco install python postgresql git curl
```

## Installation Steps

### 1. Environment Setup

#### Clone Repository

```bash
git clone <repository-url> usaspending-etl
cd usaspending-etl
```

#### Create Virtual Environment

```bash
# Using uv (recommended)
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv --python 3.13
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows

# Using standard venv
python3.13 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows
```

#### Install Dependencies

```bash
# Using uv
uv pip install -e .

# Using pip
pip install -e .
```

### 2. Database Setup

#### PostgreSQL Configuration

```bash
# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql
```

```sql
-- In PostgreSQL prompt
CREATE USER etl_user WITH PASSWORD 'secure_password';
CREATE DATABASE usaspending_etl OWNER etl_user;
GRANT ALL PRIVILEGES ON DATABASE usaspending_etl TO etl_user;

-- Enable extensions
\c usaspending_etl
CREATE EXTENSION IF NOT EXISTS pgvector;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\q
```

#### Database Initialization

```bash
# Initialize database schema
python scripts/setup.py --init-database
```

### 3. Configuration

#### Environment Variables

```bash
# Create environment file
cat > .env << EOF
ETL_DATABASE_HOST=localhost
ETL_DATABASE_PORT=5432
ETL_DATABASE_NAME=usaspending_etl
ETL_DATABASE_USER=etl_user
ETL_DATABASE_PASSWORD=secure_password
ETL_API_KEY=your_api_key_here
ETL_WORK_DIR=./work
ETL_LOG_LEVEL=INFO
EOF

# Load environment variables
source .env
export $(cat .env | xargs)
```

#### Configuration Files

```bash
# Copy default configurations
cp config/default.json config/production.json

# Edit production configuration
nano config/production.json
```

#### Validate Configuration

```bash
python scripts/setup.py --validate-config
```

### 4. Initial Data Load

#### Test Connection

```bash
# Test database and API connectivity
python scripts/setup.py --test-connections
```

#### Historical Data Load

```bash
# Load initial dataset (last 30 days)
python scripts/run_historical.py --days 30

# Monitor progress
tail -f logs/pipeline.log
```

### 5. Service Configuration

#### Systemd Service (Linux)

```bash
# Create service file
sudo tee /etc/systemd/system/usaspending-etl.service > /dev/null << EOF
[Unit]
Description=USASpending ETL Pipeline
After=network.target postgresql.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$PWD
Environment=PATH=$PWD/.venv/bin
EnvironmentFile=$PWD/.env
ExecStart=$PWD/.venv/bin/python scripts/monitor.py --daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable usaspending-etl
sudo systemctl start usaspending-etl
```

#### Cron Jobs

```bash
# Setup automated tasks
crontab -e

# Add these entries:
# Daily incremental pipeline at 6 AM
0 6 * * * cd /path/to/usaspending-etl && .venv/bin/python scripts/run_incremental.py

# Weekly full diagnostics on Sundays at 2 AM
0 2 * * 0 cd /path/to/usaspending-etl && .venv/bin/python scripts/run_diagnostics.py --full

# Daily maintenance at 1 AM
0 1 * * * cd /path/to/usaspending-etl && .venv/bin/python scripts/maintenance.py --routine

# Hourly health check
0 * * * * cd /path/to/usaspending-etl && .venv/bin/python scripts/monitor.py --health-check
```

## Production Deployment

### 1. Security Configuration

#### Database Security

```sql
-- Connect as superuser
sudo -u postgres psql

-- Create read-only user for monitoring
CREATE USER etl_monitor WITH PASSWORD 'monitor_password';
GRANT CONNECT ON DATABASE usaspending_etl TO etl_monitor;
GRANT USAGE ON SCHEMA s1_raw, s2_interim, s3_processed TO etl_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA s1_raw, s2_interim, s3_processed TO etl_monitor;

-- Revoke unnecessary privileges
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
```

#### File Permissions

```bash
# Set secure file permissions
chmod 600 .env
chmod 600 config/production.json
chmod -R 750 scripts/
chmod -R 750 etl/
```

#### Firewall Configuration

```bash
# Allow only necessary ports
sudo ufw allow 22    # SSH
sudo ufw allow 5432  # PostgreSQL (if remote access needed)
sudo ufw enable
```

### 2. Performance Optimization

#### PostgreSQL Tuning

```bash
# Edit PostgreSQL configuration
sudo nano /etc/postgresql/14/main/postgresql.conf
```

```ini
# Memory settings (adjust based on available RAM)
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB

# Connection settings
max_connections = 100

# Logging
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_statement = 'mod'
log_duration = on
log_min_duration_statement = 1000

# Performance
random_page_cost = 1.1
effective_io_concurrency = 200
```

#### Application Tuning

```json
// config/production.json
{
  "chunk_size": 5000,
  "max_retries": 5,
  "parallel_workers": 4,
  "disk_limit_gb": 200,
  "pipeline_settings": {
    "batch_size": 2000,
    "timeout_seconds": 600
  }
}
```

### 3. Monitoring Setup

#### Logging Configuration

```bash
# Setup log rotation
sudo tee /etc/logrotate.d/usaspending-etl > /dev/null << EOF
/path/to/usaspending-etl/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0644 $USER $USER
    postrotate
        systemctl reload usaspending-etl
    endscript
}
EOF
```

#### Alerting Setup

```bash
# Configure email alerts
python scripts/setup.py --configure-alerts \
  --email admin@company.com \
  --smtp-host smtp.company.com \
  --smtp-port 587 \
  --smtp-user alerts@company.com \
  --smtp-password alert_password
```

### 4. Backup Configuration

#### Database Backups

```bash
# Create backup script
tee scripts/backup_database.sh > /dev/null << EOF
#!/bin/bash
BACKUP_DIR="/backup/usaspending-etl"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# Full database backup
pg_dump -h localhost -U etl_user usaspending_etl | gzip > $BACKUP_DIR/full_backup_$DATE.sql.gz

# Schema-only backup
pg_dump -h localhost -U etl_user --schema-only usaspending_etl > $BACKUP_DIR/schema_backup_$DATE.sql

# Cleanup old backups (keep 7 days)
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete
find $BACKUP_DIR -name "*.sql" -mtime +7 -delete

echo "Backup completed: $DATE"
EOF

chmod +x scripts/backup_database.sh

# Schedule backups
echo "0 2 * * * /path/to/usaspending-etl/scripts/backup_database.sh" | crontab -
```

#### Configuration Backups

```bash
# Create config backup script
tee scripts/backup_config.sh > /dev/null << EOF
#!/bin/bash
BACKUP_DIR="/backup/usaspending-etl/config"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# Backup configuration files
tar -czf $BACKUP_DIR/config_backup_$DATE.tar.gz config/ .env

# Cleanup old backups (keep 30 days)
find $BACKUP_DIR -name "*.tar.gz" -mtime +30 -delete

echo "Config backup completed: $DATE"
EOF

chmod +x scripts/backup_config.sh
```

## Environment-Specific Configurations

### Development Environment

```json
// config/development.json
{
  "log_level": "DEBUG",
  "disk_limit_gb": 20,
  "chunk_size": 1000,
  "enable_metrics": true,
  "validation_rules": {
    "max_error_rate_percent": 20
  }
}
```

### Staging Environment

```json
// config/staging.json
{
  "log_level": "INFO",
  "disk_limit_gb": 50,
  "chunk_size": 3000,
  "enable_metrics": true,
  "validation_rules": {
    "max_error_rate_percent": 10
  }
}
```

### Production Environment

```json
// config/production.json
{
  "log_level": "WARNING",
  "disk_limit_gb": 200,
  "chunk_size": 5000,
  "enable_metrics": true,
  "validation_rules": {
    "max_error_rate_percent": 5,
    "data_freshness_hours": 24
  },
  "retention_days": {
    "raw_data": 14,
    "interim_data": 60,
    "processed_data": 1095
  }
}
```

## Health Checks

### Post-Deployment Validation

```bash
# Run comprehensive health check
python scripts/setup.py --health-check

# Test all components
python scripts/run_diagnostics.py --full

# Verify monitoring
python scripts/monitor.py --test-alerts

# Check service status
systemctl status usaspending-etl
```

### Performance Baseline

```bash
# Establish performance baseline
python scripts/monitor.py --establish-baseline

# Run performance test
python scripts/run_incremental.py --dry-run --profile
```

## Rollback Procedures

### Application Rollback

```bash
# Stop current version
sudo systemctl stop usaspending-etl

# Rollback to previous version
git checkout <previous-tag>
pip install -e .

# Restart service
sudo systemctl start usaspending-etl

# Verify rollback
python scripts/monitor.py --health-check
```

### Database Rollback

```bash
# Stop application
sudo systemctl stop usaspending-etl

# Restore database from backup
dropdb usaspending_etl
createdb usaspending_etl -O etl_user
gunzip -c /backup/full_backup_YYYYMMDD_HHMMSS.sql.gz | psql usaspending_etl

# Restart application
sudo systemctl start usaspending-etl
```

## Scaling Considerations

### Horizontal Scaling

- Database replication for read queries
- Load balancing for API requests
- Distributed task processing

### Vertical Scaling

- Increase server resources (CPU, RAM, Storage)
- Optimize PostgreSQL configuration
- Tune application parameters

### Storage Scaling

- Implement table partitioning
- Use compressed storage formats
- Automated data archival

## Security Checklist

- [ ] Database credentials are secure and rotated regularly
- [ ] API keys are stored in environment variables
- [ ] File permissions are restrictive
- [ ] Firewall is configured properly
- [ ] SSL/TLS is enabled for database connections
- [ ] Logging doesn't expose sensitive information
- [ ] Backup files are encrypted
- [ ] Access controls are implemented
- [ ] Security updates are applied regularly
- [ ] Audit trails are maintained

---

_Last Updated: [Current Date]_
_Document Version: 1.0_
