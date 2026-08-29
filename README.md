# CitiCore Account Service - Comprehensive Detailed Notes

**Complete guide to building a production-grade banking account service with MySQL master-replica replication, read-write separation, partitioning for scalability, and Spring Boot routing logic.**

---

## Table of Contents

1. [Database Architecture Overview](#database-architecture-overview)
2. [Local Docker MySQL Setup](#local-docker-mysql-setup)
3. [MySQL Users and Authentication](#mysql-users-and-authentication)
4. [MySQL Partitioning](#mysql-partitioning)
5. [Dead Letter Events (DLQ)](#dead-letter-events-dlq)
6. [Partition Maintenance](#partition-maintenance)
7. [Docker MySQL Replication](#docker-mysql-replication)
8. [Docker Configuration Problems](#docker-configuration-problems)
9. [AWS RDS Implementation](#aws-rds-implementation)
10. [SSL/TLS Security](#ssltls-security)
11. [Java Truststore Configuration](#java-truststore-configuration)
12. [Spring Boot Datasource Routing](#spring-boot-datasource-routing)
13. [Read/Write Routing with Annotations](#readwrite-routing-with-annotations)
14. [AOP-Based Routing](#aop-based-routing)
15. [Replica Health Monitoring](#replica-health-monitoring)
16. [Transaction Outbox Pattern](#transaction-outbox-pattern)
17. [Real Issues & Solutions](#real-issues--solutions)
18. [Security Improvements](#security-improvements)
19. [Interview-Ready Explanations](#interview-ready-explanations)

---

## Database Architecture Overview

### Definition

**Database Architecture** defines how data is organized, stored, accessed, and protected in an application. For banking systems, this includes:
- High availability (redundancy)
- Read/write separation (scaling)
- Strong consistency (correctness)
- Data partitioning (performance)

### What You Implemented

```
CitiCore Account Service Architecture:
┌─────────────────────────────────────┐
│    Spring Boot Application          │
│   (Account Service Logic)           │
└────────────┬────────────────────────┘
             │
    ┌────────┴────────┐
    │                 │
    v                 v
Primary Hikari      Replica Hikari
 Pool (Writes)      Pool (Reads)
    │                 │
    v                 v
RDS Primary ◄────► RDS Replica
(AWS)           (Asynchronous
                 Replication)
    │                 │
    └────────┬────────┘
             │
        Java Truststore
      (SSL/TLS Certs)
```

### Why This Architecture

```
Problem 1: Single Database Bottleneck
├─ Reads compete with writes
├─ Write-heavy operations slow down reads
└─ Database becomes bottleneck

Solution: Read/Write Separation
├─ Primary: Only writes (not competing)
├─ Replica: Only reads (high throughput)
└─ Result: 10x+ improvement in read performance

Problem 2: Data Loss Risk
├─ Single server fails = data lost
├─ No redundancy for critical banking data
└─ Regulatory violations (PCI-DSS)

Solution: Replication
├─ Primary: Source of truth
├─ Replica: Real-time backup
├─ If primary fails, replica becomes primary
└─ Data protected, business continuity

Problem 3: Uncontrolled Table Growth
├─ account_statements: millions of records monthly
├─ Full table scans become slow
├─ Storage becomes expensive
└─ Queries time out

Solution: Partitioning
├─ account_statements: monthly partitions
├─ account_outbox: monthly partitions
├─ Old partitions dropped (automatic cleanup)
└─ New partitions added monthly
└─ Queries only scan relevant partition (pruning)

Problem 4: Eventual Consistency Issues
├─ Replica lags behind primary
├─ Strong read gives stale data
└─ Banking rules violated (balance must be current)

Solution: Dual Routing
├─ @ReadOnly → Replica (consistent-read-not-required)
├─ @PrimaryRead → Primary (strong consistency)
└─ @Transactional → Primary (writes)
```

### Step-by-Step Design Process

**Step 1: Identify Data Access Patterns**

```
Account Service Queries:

Writes (must use Primary):
├─ Create account
├─ Debit account
├─ Credit account
└─ Transfer funds

Consistent Reads (must use Primary):
├─ Get current balance (for transfer validation)
├─ Check if account exists before debit
└─ Validate sufficient balance

Eventual-Consistent Reads (can use Replica):
├─ Get statement history
├─ Get transaction list
├─ Generate account summary
└─ Admin reports
```

**Step 2: Design Table Schema with Partitioning**

```sql
-- accounts table (not partitioned, small)
CREATE TABLE accounts (
    id BIGINT PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE,
    balance DECIMAL(15,2),
    status ENUM('ACTIVE', 'CLOSED'),
    created_at TIMESTAMP
);

-- account_statements table (partitioned monthly)
CREATE TABLE account_statements (
    id BIGINT,
    created_at DATETIME,
    account_number VARCHAR(20),
    txn_type ENUM('DEBIT', 'CREDIT', 'REVERSAL'),
    amount DECIMAL(15,2),
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uk_txn_ref (txn_ref, created_at),
    INDEX idx_stmt_account_number (account_number, created_at)
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION p_2026_08 VALUES LESS THAN ('2026-09-01'),
    PARTITION p_2026_09 VALUES LESS THAN ('2026-10-01'),
    ...
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- account_outbox table (partitioned monthly)
CREATE TABLE account_outbox (
    id BIGINT,
    created_at DATETIME,
    event_type VARCHAR(50),
    event_payload JSON,
    status ENUM('PENDING', 'SENT', 'FAILED'),
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uk_event_id (event_id, created_at),
    INDEX idx_outbox_status (status, created_at)
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION p_2026_08 VALUES LESS THAN ('2026-09-01'),
    ...
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
```

**Step 3: Set Up Replication (Primary → Replica)**

```
Primary Configuration:
├─ server-id = 1
├─ binary logging enabled (log-bin)
├─ GTID mode enabled (for reliability)
└─ Write operations accepted

Replica Configuration:
├─ server-id = 2
├─ relay-log enabled (receives from primary)
├─ GTID mode enabled (matches primary)
├─ Read-only mode enabled (no direct writes)
└─ Read operations only
```

**Step 4: Configure Spring Boot Routing**

```yaml
# Application knows about both databases
spring:
  datasource:
    primary:
      jdbc-url: jdbc:mysql://primary-rds:3306/citicore_account
      username: app_primary
      password: ${DB_PRIMARY_PASSWORD}
    replica:
      jdbc-url: jdbc:mysql://replica-rds:3306/citicore_account
      username: app_replica
      password: ${DB_REPLICA_PASSWORD}
```

**Step 5: Implement Routing Logic**

```java
// @ReadOnly → Routes to Replica
@ReadOnly
public List<Statement> getStatements(String accountNumber) {
    // Query executes on Replica
}

// @PrimaryRead → Routes to Primary
@PrimaryRead
public BigDecimal getBalance(String accountNumber) {
    // Query executes on Primary (strong consistency)
}

// Default (no annotation) → Routes to Primary
@Transactional
public void debit(String accountNumber, BigDecimal amount) {
    // Write executes on Primary
}
```

---

## Local Docker MySQL Setup

### Definition

**Local Docker MySQL** means running MySQL instances in Docker containers on your local development machine before moving to AWS RDS. This allows testing replication and routing logic locally.

### What You Implemented

```
Two Docker MySQL containers:

citicore-mysql-primary
├─ Exposed on: localhost:3308
├─ Internal port: 3306
├─ Database: citicore_account
├─ Tables: accounts, account_statements, account_outbox
├─ Role: Write target, replication source
└─ Status: Primary

citicore-mysql-replica
├─ Exposed on: localhost:3307
├─ Internal port: 3306
├─ Database: citicore_account (replicated from primary)
├─ Tables: same as primary (copied via replication)
├─ Role: Read target, replication target
└─ Status: Replica (read-only)
```

### Why Local Docker Setup

```
Benefit 1: No AWS Costs
├─ AWS RDS costs ~$0.60/hour per instance
├─ Docker containers: free
└─ Save $30/day during development

Benefit 2: Fast Iteration
├─ Container starts in seconds
├─ Easy to restart, reset, rebuild
├─ No AWS provisioning delays
└─ Test replication without AWS

Benefit 3: Configuration Testing
├─ Test replication settings locally
├─ Find configuration errors before AWS
├─ No infrastructure costs for mistakes
└─ Learn MySQL replication safely

Benefit 4: Isolation
├─ Local development doesn't affect AWS
├─ Can experiment without risk
├─ Easy to destroy and start fresh
└─ Clean state always available
```

### Step-by-Step Process

**Step 1: Create Docker Compose File**

```yaml
# docker-compose.yml
version: '3.8'

services:
  # Primary MySQL instance
  citicore-mysql-primary:
    image: mysql:8.0
    container_name: citicore-mysql-primary
    environment:
      MYSQL_ROOT_PASSWORD: root_password
      MYSQL_DATABASE: citicore_account
      MYSQL_USER: citicore
      MYSQL_PASSWORD: citicore123
    ports:
      - "3308:3306"
    volumes:
      - ./mysql-config/primary.cnf:/etc/mysql/conf.d/primary.cnf
      - ./mysql-config/init-primary.sql:/docker-entrypoint-initdb.d/init-primary.sql
      - primary-data:/var/lib/mysql
    networks:
      - citicore-network
    command: --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --gtid-mode=ON --enforce-gtid-consistency=ON

  # Replica MySQL instance
  citicore-mysql-replica:
    image: mysql:8.0
    container_name: citicore-mysql-replica
    environment:
      MYSQL_ROOT_PASSWORD: root_password
      MYSQL_DATABASE: citicore_account
    ports:
      - "3307:3306"
    volumes:
      - ./mysql-config/replica.cnf:/etc/mysql/conf.d/replica.cnf
      - ./mysql-config/init-replica.sql:/docker-entrypoint-initdb.d/init-replica.sql
      - replica-data:/var/lib/mysql
    networks:
      - citicore-network
    command: --server-id=2 --relay-log=relay-bin --log-bin=mysql-bin --read-only=ON --super-read-only=ON --gtid-mode=ON --enforce-gtid-consistency=ON
    depends_on:
      - citicore-mysql-primary

volumes:
  primary-data:
  replica-data:

networks:
  citicore-network:
    driver: bridge
```

**Step 2: Create Configuration Files**

```ini
# mysql-config/primary.cnf
[mysqld]
server-id=1
log-bin=mysql-bin
binlog-format=ROW
gtid-mode=ON
enforce-gtid-consistency=ON
log-replica-updates=ON
bind-address=0.0.0.0

# mysql-config/replica.cnf
[mysqld]
server-id=2
relay-log=relay-bin
log-bin=mysql-bin
log-replica-updates=ON
gtid-mode=ON
enforce-gtid-consistency=ON
read-only=ON
super-read-only=ON
bind-address=0.0.0.0
```

**Step 3: Start Containers**

```bash
# Start both containers
docker-compose up -d

# Verify containers running
docker ps

# Check logs
docker logs citicore-mysql-primary
docker logs citicore-mysql-replica
```

**Step 4: Initialize Databases**

```bash
# Connect to primary
docker exec -it citicore-mysql-primary mysql -uroot -proot_password

# Create schema and tables
SOURCE /docker-entrypoint-initdb.d/init-primary.sql;

# Check replication status
SHOW MASTER STATUS;
```

**Step 5: Verify Replication**

```bash
# Connect to replica
docker exec -it citicore-mysql-replica mysql -uroot -proot_password

# Check replica status
SHOW SLAVE STATUS\G

# Should show:
# Slave_IO_Running: Yes
# Slave_SQL_Running: Yes
# Seconds_Behind_Master: 0
```

---

## MySQL Users and Authentication

### Definition

**MySQL Users** are accounts with specific permissions. Different users for different purposes (write user, read-only user, admin) enforce least-privilege security.

### What You Implemented

```
MySQL Users:

1. citicore (Primary)
   ├─ Use: Application writes
   ├─ Host: % (any host)
   ├─ Password: citicore123
   ├─ Auth Plugin: caching_sha2_password
   └─ Permissions: All on citicore_account

2. citicore_readonly (Replica)
   ├─ Use: Application reads
   ├─ Host: % (any host)
   ├─ Password: replica123
   ├─ Auth Plugin: caching_sha2_password
   └─ Permissions: SELECT only on citicore_account

3. root (Admin)
   ├─ Use: Administration only
   ├─ Password: root_password
   └─ Permissions: All (don't use in application)
```

### Why Separate Users

```
Security Benefit 1: Least Privilege
├─ citicore_readonly can only SELECT
├─ Even if credentials leaked, read-only
├─ Cannot accidentally delete data
└─ Damage is limited to read

Security Benefit 2: Audit Trail
├─ Can track which user made changes
├─ Read operations logged separately
├─ Easier to investigate issues
└─ Better compliance audit

Security Benefit 3: Defense in Depth
├─ Read replica doesn't need write user
├─ Primary doesn't need read-only user
├─ Each user has minimal permissions
└─ Reduces attack surface
```

### Step-by-Step Process

**Step 1: Create Application Users**

```bash
# Connect to MySQL
docker exec -it citicore-mysql-primary mysql -uroot -proot_password

# Create write user (primary)
CREATE USER 'citicore'@'%' IDENTIFIED BY 'citicore123';
GRANT ALL PRIVILEGES ON citicore_account.* TO 'citicore'@'%';
FLUSH PRIVILEGES;

# Verify
SELECT user, host, plugin FROM mysql.user WHERE user = 'citicore';
```

**Step 2: Create Read-Only User**

```bash
# Connect to replica
docker exec -it citicore-mysql-replica mysql -uroot -proot_password

# Create read-only user
CREATE USER 'citicore_readonly'@'%' IDENTIFIED BY 'replica123';
GRANT SELECT ON citicore_account.* TO 'citicore_readonly'@'%';
FLUSH PRIVILEGES;

# Verify
SELECT user, host, plugin FROM mysql.user WHERE user = 'citicore_readonly';
```

**Step 3: Test User Connections**

```bash
# Test primary write user
docker exec -it citicore-mysql-primary mysql -u citicore -pciticore123 -e "SELECT CURRENT_USER();"
# Output: citicore@%

# Test replica read-only user
docker exec -it citicore-mysql-replica mysql -u citicore_readonly -preplica123 -e "SELECT CURRENT_USER();"
# Output: citicore_readonly@%
```

**Step 4: Test Write Restriction on Replica**

```bash
# Connect as read-only user to replica
docker exec -it citicore-mysql-replica mysql -u citicore_readonly -preplica123

# Try to insert (should fail)
INSERT INTO accounts VALUES (1, 'ACC001', 1000);
# Error: Access Denied for user 'citicore_readonly'@'...' to database 'citicore_account'

# Try to read (should succeed)
SELECT COUNT(*) FROM accounts;
# Returns count successfully
```

---

## MySQL Partitioning

### Definition

**Table Partitioning** divides a large table into smaller, manageable pieces based on a column value. For example, monthly partitions for account_statements table.

Benefits:
- **Query Performance**: Queries only scan relevant partitions (partition pruning)
- **Maintenance**: Old partitions dropped instead of deleting millions of rows
- **Scalability**: Handles millions of rows without slowdown

### What You Implemented

**account_statements (Monthly RANGE Partitioning)**

```sql
CREATE TABLE account_statements (
    id BIGINT,
    created_at DATETIME,
    account_number VARCHAR(20),
    txn_type ENUM('DEBIT', 'CREDIT', 'REVERSAL'),
    amount DECIMAL(15,2),
    txn_ref VARCHAR(50),
    
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uk_txn_ref (txn_ref, created_at),
    INDEX idx_stmt_account_number (account_number, created_at)
    
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION p_2025_01 VALUES LESS THAN ('2025-02-01'),
    PARTITION p_2025_02 VALUES LESS THAN ('2025-03-01'),
    ...
    PARTITION p_2026_08 VALUES LESS THAN ('2026-09-01'),
    PARTITION p_2026_09 VALUES LESS THAN ('2026-10-01'),
    PARTITION p_2026_10 VALUES LESS THAN ('2026-11-01'),
    PARTITION p_2026_11 VALUES LESS THAN ('2026-12-01'),
    PARTITION p_2026_12 VALUES LESS THAN ('2027-01-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
```

**account_outbox (Monthly RANGE Partitioning)**

```sql
CREATE TABLE account_outbox (
    id BIGINT,
    created_at DATETIME,
    event_id VARCHAR(50),
    event_type VARCHAR(50),
    event_payload JSON,
    status ENUM('PENDING', 'SENT', 'FAILED'),
    retried_count INT DEFAULT 0,
    
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uk_event_id (event_id, created_at),
    INDEX idx_outbox_status (status, created_at)
    
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION p_2025_01 VALUES LESS THAN ('2025-02-01'),
    ...
    PARTITION p_2026_12 VALUES LESS THAN ('2027-01-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
```

### Why Partitioning for Banking

```
Problem 1: Uncontrolled Growth
├─ account_statements: 10,000+ new records per day
├─ account_outbox: 5,000+ events per day
├─ Monthly: 300,000+ records
├─ Yearly: 3.6+ million records
└─ Table becomes massive

Without Partitioning:
├─ SELECT * WHERE account_number = 'X' scans all 3.6M rows
├─ Queries become slow
├─ Storage expensive
└─ No easy cleanup

With Partitioning:
├─ Partition p_2026_08 = only August records (~300K)
├─ Query with date filter only scans August partition
├─ Queries 12x faster (scan 1 of 12 months)
└─ Easy cleanup: DROP PARTITION p_2025_01

Problem 2: Regulatory Requirements
├─ Banking: retain 7 years of records (rule)
├─ Without partitioning: million-row DELETE takes hours
├─ With partitioning: DROP PARTITION p_2019_01 takes seconds
└─ Compliance requirement: easy to meet

Problem 3: Index Performance
├─ Without partitioning: huge index on 3.6M rows
├─ Index lookup slow, uses more memory
├─ With partitioning: smaller indexes per partition
└─ Index lookups fast, memory efficient
```

### Step-by-Step Process

**Step 1: Design Partitioning Strategy**

```
Decision: Monthly partitioning on created_at

Why monthly?
├─ Most queries filter by date range (statements for month X)
├─ Monthly cleanup natural (archive old months)
├─ Not too many partitions (24 months ≈ 24 partitions)
├─ Not too few (yearly = only 1 query filter benefit)
└─ Perfect balance

Table Selection:
├─ account_statements: YES (millions of rows)
├─ account_outbox: YES (grows rapidly, short-lived)
├─ accounts: NO (small, grows slowly, keep unpartitioned)
└─ dead_letter_events: NO (small admin table)
```

**Step 2: Create Partitioned Table**

```sql
-- Create account_statements with partitioning
CREATE TABLE account_statements (
    id BIGINT AUTO_INCREMENT,
    created_at DATETIME NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    txn_type ENUM('DEBIT', 'CREDIT', 'REVERSAL') NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    txn_ref VARCHAR(50) UNIQUE,
    
    PRIMARY KEY (id, created_at),
    INDEX idx_account (account_number, created_at)
) PARTITION BY RANGE COLUMNS(created_at) (
    PARTITION p_2026_08 VALUES LESS THAN ('2026-09-01'),
    PARTITION p_2026_09 VALUES LESS THAN ('2026-10-01'),
    PARTITION p_2026_10 VALUES LESS THAN ('2026-11-01'),
    PARTITION p_2026_11 VALUES LESS THAN ('2026-12-01'),
    PARTITION p_2026_12 VALUES LESS THAN ('2027-01-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
```

**Step 3: Insert Data and Verify Partitioning**

```bash
# Insert test data
INSERT INTO account_statements (created_at, account_number, txn_type, amount, txn_ref)
VALUES 
  ('2026-08-15', 'ACC001', 'DEBIT', 500, 'TXN001'),
  ('2026-09-20', 'ACC002', 'CREDIT', 1000, 'TXN002'),
  ('2026-10-10', 'ACC003', 'TRANSFER', 2000, 'TXN003');

# Verify partition distribution
SELECT 
    PARTITION_NAME, 
    TABLE_ROWS, 
    DATA_LENGTH 
FROM INFORMATION_SCHEMA.PARTITIONS 
WHERE TABLE_NAME = 'account_statements';

# Output:
# p_2026_08    1 row
# p_2026_09    1 row
# p_2026_10    1 row
# p_future     0 rows
```

**Step 4: Verify Partition Pruning**

```bash
# Check EXPLAIN to see partition pruning in action
EXPLAIN 
SELECT * 
FROM account_statements 
WHERE account_number = 'ACC001' 
  AND created_at >= '2026-08-01' 
  AND created_at < '2026-09-01';

# Output should show:
# Partitions: p_2026_08 (only scans August partition!)

# Without date filter (scans all):
EXPLAIN 
SELECT * 
FROM account_statements 
WHERE account_number = 'ACC001';

# Output shows:
# Partitions: p_2026_08,p_2026_09,p_2026_10,p_future (all partitions)
```

---

## Dead Letter Events (DLQ)

### Definition

**Dead Letter Queue (DLQ)** is a table for events that failed to be processed. Instead of losing failed events, they're stored for manual investigation and retry.

### What You Implemented

```sql
CREATE TABLE dead_letter_events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    kafka_topic VARCHAR(50) NOT NULL,
    partition_id INT,
    offset_id BIGINT,
    event_key VARCHAR(255),
    event_payload JSON NOT NULL,
    error_message VARCHAR(500),
    exception_class VARCHAR(255),
    status ENUM('UNRESOLVED', 'RESOLVED', 'MANUALLY_FIXED') DEFAULT 'UNRESOLVED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(50),
    
    INDEX idx_dlq_status (status),
    INDEX idx_dlq_created (created_at)
);
```

### Why DLQ for Banking

```
Scenario: Outbox Publishing Fails

Without DLQ:
├─ Event fails to publish to Kafka
├─ Error logged
├─ Event lost
├─ Customer doesn't get notification
└─ Regulatory violation (transaction record incomplete)

With DLQ:
├─ Event fails to publish
├─ Event stored in dead_letter_events table
├─ Alert sent to operations
├─ Manual investigation possible
├─ Can retry the event later
└─ Compliance: complete audit trail

DLQ Provides:
├─ Observability: see all failures
├─ Debuggability: payload stored for analysis
├─ Recoverability: can manually retry
└─ Compliance: no data loss
```

---

## Partition Maintenance

### Definition

**Partition Maintenance** is the ongoing operation to add new partitions monthly and drop old ones based on retention policy.

### What You Implemented

**Add New Partition (Monthly)**

```sql
-- January 2027 arrives, need to reorganize p_future
ALTER TABLE account_statements
REORGANIZE PARTITION p_future INTO (
    PARTITION p_2027_01 VALUES LESS THAN ('2027-02-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- Now p_future is ready for records > 2027-02-01
-- New partition p_2027_01 ready for January 2027
```

**Drop Old Partition (When Retention Expires)**

```sql
-- Banking retention: 7 years
-- January 2020 data now 7 years old, can be dropped
ALTER TABLE account_statements
DROP PARTITION p_2020_01;

-- Alternative: Archive partition first
-- mysqldump -u admin -p citicore_account account_statements_p_2020_01 > archive_2020_01.sql
-- Then DROP PARTITION
```

**Monitor Partition Sizes**

```sql
-- Check which partitions are getting full
SELECT 
    PARTITION_NAME,
    TABLE_ROWS,
    DATA_LENGTH / 1024 / 1024 AS SIZE_MB
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_NAME = 'account_statements'
ORDER BY PARTITION_NAME;

-- Helps identify growth patterns
```

### Step-by-Step Maintenance Schedule

**Monthly (1st of month):**

```bash
#!/bin/bash
# Monthly partition maintenance script

# Generate new partition for next month
NEXT_MONTH=$(date -d "+1 month" +"%Y_%m")
NEXT_DATE=$(date -d "+1 month" +"%Y-%m-01")

mysql -u admin -p -e "
ALTER TABLE account_statements
REORGANIZE PARTITION p_future INTO (
    PARTITION p_${NEXT_MONTH} VALUES LESS THAN ('${NEXT_DATE}'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
"
```

**Quarterly (Check & Archive):**

```bash
#!/bin/bash
# Quarterly check: archive partitions older than 7 years

ARCHIVE_DATE=$(date -d "-7 years" +"%Y_%m")

# Check if partition exists
mysql -u admin -p -e "
SELECT PARTITION_NAME 
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_NAME = 'account_statements'
AND PARTITION_NAME = 'p_${ARCHIVE_DATE}';
"

# If exists, archive and drop
if [ $? -eq 0 ]; then
    # Backup partition to file
    mysqldump -u admin -p citicore_account account_statements \
        --where="created_at >= '$(date -d "-7 years -1 month" +"%Y-%m-01")' 
        AND created_at < '$(date -d "-7 years" +"%Y-%m-01")'" \
        > archive_${ARCHIVE_DATE}.sql
    
    # Drop partition
    mysql -u admin -p -e "
    ALTER TABLE account_statements
    DROP PARTITION p_${ARCHIVE_DATE};
    "
fi
```

---

## Docker MySQL Replication

### Definition

**MySQL Replication** is the automatic copying of changes from a Primary (source) to a Replica (copy). All writes go to Primary, Replica reads Primary's binary log, and applies same changes.

### What You Implemented

**Replication Flow:**

```
Primary MySQL
├─ Receives INSERT/UPDATE/DELETE
├─ Writes to binary log (mysql-bin.000001)
├─ Changes recorded with GTID (Global Transaction ID)
└─ Replica reads binary log

Replica MySQL
├─ IO Thread: reads binary log from Primary
├─ SQL Thread: applies changes to local database
├─ Result: Replica is copy of Primary
└─ Reads from Replica get Primary's data
```

**Primary Configuration:**

```ini
[mysqld]
# Unique identifier
server-id=1

# Binary logging enabled
log-bin=mysql-bin

# Row-based replication (records exact changes, not SQL)
binlog-format=ROW

# GTID mode (replication with transaction IDs, more reliable)
gtid-mode=ON
enforce-gtid-consistency=ON

# Primary also logs its own replicated changes
log-replica-updates=ON

# Listen on all network interfaces
bind-address=0.0.0.0
```

**Replica Configuration:**

```ini
[mysqld]
# Unique identifier (different from primary)
server-id=2

# Relay log (stores changes from primary before applying)
relay-log=relay-bin

# Also keep binary log (for chain replication if needed)
log-bin=mysql-bin

# Apply replicated changes to own binary log
log-replica-updates=ON

# GTID mode (must match primary)
gtid-mode=ON
enforce-gtid-consistency=ON

# Make replica read-only (only replication can write)
read-only=ON
super-read-only=ON

# Listen on all interfaces
bind-address=0.0.0.0
```

### Why Each Setting

```
server-id=1 and server-id=2:
├─ Unique ID for each server
├─ Required for replication to work
├─ Prevents replication loops
└─ Used in logs and status

binlog-format=ROW:
├─ Alternatives: STATEMENT, MIXED
├─ ROW: logs exact row changes (safest)
├─ STATEMENT: logs SQL (problematic with functions)
├─ MIXED: uses both (complicated)
└─ ROW: best for banking (exact changes)

gtid-mode=ON:
├─ GTID = Global Transaction ID
├─ Every transaction gets unique ID
├─ Replica can resume from specific GTID
├─ More reliable than position-based
└─ Survives server restarts

read-only=ON, super-read-only=ON:
├─ read-only: normal users can't write
├─ super-read-only: even admin can't write
├─ Only replication thread can write
├─ Ensures replica is read-only
└─ Prevents accidental writes
```

### Step-by-Step Replication Setup

**Step 1: Configure and Start Primary**

```bash
# Start primary with config
docker run -d \
  --name citicore-mysql-primary \
  -e MYSQL_ROOT_PASSWORD=root_password \
  -e MYSQL_DATABASE=citicore_account \
  -v ./primary.cnf:/etc/mysql/conf.d/my.cnf \
  -p 3308:3306 \
  mysql:8.0

# Create replication user (for replica to connect)
docker exec citicore-mysql-primary mysql -uroot -proot_password -e "
CREATE USER 'replicator'@'%' IDENTIFIED BY 'replica_password';
GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';
FLUSH PRIVILEGES;
"
```

**Step 2: Get Primary Binary Log Position**

```bash
docker exec citicore-mysql-primary mysql -uroot -proot_password -e "
SHOW MASTER STATUS;
"

# Output:
# File: mysql-bin.000001
# Position: 154
# Binlog_Do_DB:
# Binlog_Ignore_DB:
# Executed_Gtid_Set: 2a34215c-1234-5678-abcd-ef1234567890:1
```

**Step 3: Start Replica and Configure Replication**

```bash
# Start replica with config
docker run -d \
  --name citicore-mysql-replica \
  -e MYSQL_ROOT_PASSWORD=root_password \
  -e MYSQL_DATABASE=citicore_account \
  -v ./replica.cnf:/etc/mysql/conf.d/my.cnf \
  -p 3307:3306 \
  mysql:8.0

# Configure replica to connect to primary
docker exec citicore-mysql-replica mysql -uroot -proot_password -e "
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='citicore-mysql-primary',
  SOURCE_USER='replicator',
  SOURCE_PASSWORD='replica_password',
  SOURCE_AUTO_POSITION = 1;
"

# Start replication
docker exec citicore-mysql-replica mysql -uroot -proot_password -e "
START REPLICA;
"
```

**Step 4: Verify Replication**

```bash
# Check replica status
docker exec citicore-mysql-replica mysql -uroot -proot_password -e "
SHOW REPLICA STATUS\G
"

# Should show:
# Replica_IO_Running: Yes (reading from primary binary log)
# Replica_SQL_Running: Yes (applying changes locally)
# Seconds_Behind_Source: 0 (fully caught up)
# Retrieved_Gtid_Set: (GTIDs received from primary)
# Executed_Gtid_Set: (GTIDs applied to replica)
```

---

## Docker Configuration Problems

### Definition

**Docker Configuration Problems** are issues where Docker mount permissions or file permissions prevent MySQL from reading configuration files.

### Real Problems You Encountered

**Problem 1: World-Writable Config Ignored**

```
Error: World-writable config file '/etc/mysql/conf.d/primary.cnf' is ignored.

Root Cause:
├─ File permissions: -rwxrwxrwx (777)
├─ World-writable means anyone can modify
├─ MySQL security: refuses to use world-writable config
└─ Config file ignored completely

Solution:
├─ Correct permissions: -rw-r--r-- (644)
├─ Command: chmod 644 primary.cnf
├─ Result: MySQL now reads the config file
└─ Replication settings take effect
```

**Problem 2: Docker Command Inside MySQL Prompt**

```
Error: docker exec commands executed inside mysql> prompt return SQL error 1064

Mistake:
mysql> docker exec -it container mysql -e "COMMAND"
ERROR 1064 (42000): You have an error in your SQL syntax

Reason:
├─ docker exec is a shell command, not MySQL command
├─ MySQL prompt interprets it as SQL
├─ Causes syntax error

Solution:
├─ Exit MySQL prompt first: exit;
├─ Then run docker exec from shell
├─ Or use: \! docker exec ... (shell escape in MySQL)
└─ Example: \! docker exec -it container mysql -uroot -ppass
```

### Step-by-Step Fix Process

**Step 1: Identify Permission Issue**

```bash
# List files with permissions
ls -la mysql-config/

# Bad (world-writable):
# -rwxrwxrwx 1 user group primary.cnf

# Good (restricted):
# -rw-r--r-- 1 user group primary.cnf
```

**Step 2: Fix Permissions**

```bash
# Fix primary config
chmod 644 mysql-config/primary.cnf

# Fix replica config
chmod 644 mysql-config/replica.cnf

# Verify
ls -la mysql-config/
# Should now show -rw-r--r--
```

**Step 3: Rebuild Containers**

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (to start fresh)
docker volume rm primary-data replica-data

# Restart
docker-compose up -d

# Check logs for config errors
docker logs citicore-mysql-primary | grep -i "config"
docker logs citicore-mysql-replica | grep -i "config"

# Should no longer see "config file ignored" error
```

**Step 4: Verify Configuration Applied**

```bash
# Connect to primary and check settings
docker exec -it citicore-mysql-primary mysql -uroot -proot_password -e "
SHOW VARIABLES WHERE Variable_name IN ('server_id', 'log_bin', 'binlog_format', 'gtid_mode');
"

# Expected output:
# server_id: 1
# log_bin: ON
# binlog_format: ROW
# gtid_mode: ON

# If values are defaults (server_id=1, gtid_mode=OFF), config file not read!
# Go back and fix permissions
```

---

## AWS RDS Implementation

### Definition

**AWS RDS (Relational Database Service)** is AWS's managed MySQL database service. Instead of managing your own MySQL servers, AWS handles backups, replication, patches, and maintenance.

### What You Implemented

**RDS Setup:**

```
AWS RDS Primary:
├─ Endpoint: citicore-mysql-primary.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com
├─ Port: 3306
├─ Multi-AZ: Enabled (automatic failover)
├─ Storage: 100 GB
├─ Instance Type: db.t3.micro
├─ Engine: MySQL 8.0
└─ Status: Available

AWS RDS Replica:
├─ Endpoint: citicore-mysql-replica.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com
├─ Port: 3306
├─ Read-Only Replica: Yes
├─ Same availability zone or different AZ
├─ Storage: 100 GB (auto-synced)
├─ Replication: Asynchronous (async)
└─ Status: Available
```

### Why RDS vs Self-Managed

```
Self-Managed MySQL (on EC2):
├─ Pros: Full control, customizable
├─ Cons:
│  ├─ Manual backups needed
│  ├─ Manual replication setup
│  ├─ Manual patches/updates
│  ├─ Manual failover on failure
│  ├─ Disk space management
│  └─ High operational overhead

RDS (AWS Managed):
├─ Pros:
│  ├─ Automatic daily backups (35-day retention)
│  ├─ One-click read replicas
│  ├─ Automatic patches (maintenance window)
│  ├─ Automatic failover to Multi-AZ standby
│  ├─ Automatic storage scaling
│  ├─ CloudWatch monitoring
│  └─ Low operational overhead
├─ Cons:
│  ├─ Some customization limits
│  └─ Higher cost than EC2
```

### Step-by-Step RDS Setup

**Step 1: Create Primary RDS Instance**

```bash
aws rds create-db-instance \
  --db-instance-identifier citicore-mysql-primary \
  --db-instance-class db.t3.micro \
  --engine mysql \
  --engine-version 8.0.28 \
  --master-username admin \
  --master-user-password ${DB_PASSWORD} \
  --allocated-storage 100 \
  --storage-type gp2 \
  --multi-az \
  --backup-retention-period 35 \
  --db-name citicore_account \
  --vpc-security-group-ids sg-xxx \
  --db-subnet-group-name citicore-db-subnet \
  --publicly-accessible false \
  --enable-cloudwatch-logs-exports error,general,slowquery \
  --region ap-south-1

# Wait for creation (5-10 minutes)
aws rds describe-db-instances \
  --db-instance-identifier citicore-mysql-primary \
  --region ap-south-1 \
  --query 'DBInstances[0].DBInstanceStatus'
```

**Step 2: Create Read Replica**

```bash
aws rds create-db-instance-read-replica \
  --db-instance-identifier citicore-mysql-replica \
  --source-db-instance-identifier citicore-mysql-primary \
  --publicly-accessible false \
  --region ap-south-1

# Wait for creation (5-10 minutes)
aws rds describe-db-instances \
  --db-instance-identifier citicore-mysql-replica \
  --region ap-south-1
```

**Step 3: Create Application Users**

```bash
# Connect to RDS primary
mysql -h citicore-mysql-primary.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com \
      -u admin -p

# Create write user
CREATE USER 'app_primary'@'%' IDENTIFIED BY 'password123';
GRANT ALL PRIVILEGES ON citicore_account.* TO 'app_primary'@'%';

# Create read-only user
CREATE USER 'app_replica'@'%' IDENTIFIED BY 'replica_password';
GRANT SELECT ON citicore_account.* TO 'app_replica'@'%';

# Apply
FLUSH PRIVILEGES;
```

**Step 4: Create Tables**

```bash
# Connect and create schema
mysql -h citicore-mysql-primary.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com \
      -u admin -p citicore_account < schema.sql

# Verify replication to replica
mysql -h citicore-mysql-replica.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com \
      -u admin -p citicore_account -e "SHOW TABLES;"

# Should show same tables (replicated automatically)
```

---

## SSL/TLS Security

### Definition

**SSL/TLS** encrypts database traffic so passwords and data cannot be intercepted in transit.

### What You Implemented

```
Without TLS (Insecure):
Client → [PLAIN TEXT] → MySQL
├─ Password visible in network traffic
├─ Data readable if intercepted
└─ Man-in-the-middle attack possible

With TLS (Secure):
Client → [ENCRYPTED] → MySQL
├─ Password encrypted
├─ Data encrypted
├─ Man-in-the-middle prevented
└─ Certificate validation ensures you're talking to real database
```

### Certificate Chain

```
AWS RDS provides:
├─ RDS CA Certificate (issuer)
└─ RDS certificate (server certificate)

Your Java Application needs:
├─ Copy AWS RDS CA certificate
├─ Create Java truststore
├─ Add AWS CA to truststore
├─ Configure JVM to use truststore
└─ Configure JDBC URL with sslMode=VERIFY_IDENTITY
```

### Step-by-Step Process

**Step 1: Download RDS CA Certificate**

```bash
# Get global RDS CA bundle
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# Or region-specific
wget https://truststore.pki.rds.amazonaws.com/ap-south-1/ap-south-1-bundle.pem

# Verify certificate
openssl x509 -in global-bundle.pem -text -noout | head -20
```

**Step 2: Create Java Truststore**

```bash
# Create truststore from PEM certificate
keytool -import \
  -alias rds-ca \
  -file global-bundle.pem \
  -keystore rds-truststore.jks \
  -storepass changeit \
  -noprompt

# Verify truststore
keytool -list -keystore rds-truststore.jks -storepass changeit

# Output should show:
# rds-ca, 2021-01-01 00:00:00, trustedCertEntry
```

**Step 3: Add Truststore to Application**

```bash
# Copy truststore to application
cp rds-truststore.jks src/main/resources/

# Create in Docker image
COPY rds-truststore.jks /app/

# Configure in Dockerfile
ENV JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/app/rds-truststore.jks \
  -Djavax.net.ssl.trustStorePassword=changeit"
```

**Step 4: Configure JDBC URL**

```yaml
# application.yml
spring:
  datasource:
    primary:
      jdbc-url: jdbc:mysql://citicore-mysql-primary.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com:3306/citicore_account?sslMode=VERIFY_IDENTITY&serverTimezone=UTC
      username: app_primary
      password: ${DB_PASSWORD}
      
    replica:
      jdbc-url: jdbc:mysql://citicore-mysql-replica.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com:3306/citicore_account?sslMode=VERIFY_IDENTITY&serverTimezone=UTC
      username: app_replica
      password: ${DB_REPLICA_PASSWORD}
```

**Step 5: Test Connection**

```bash
# MySQL command-line test
mysql -h citicore-mysql-primary.cnk8ckkm2hsk.ap-south-1.rds.amazonaws.com \
      -u app_primary -p \
      --ssl-mode=VERIFY_IDENTITY \
      -e "STATUS;"

# Should show:
# SSL: Cipher in use is TLS_AES_256_GCM_SHA384
```

---

## Spring Boot Datasource Routing

### Definition

**Datasource Routing** directs SQL queries to either Primary (write) or Replica (read) database based on query type.

### What You Implemented

**Architecture:**

```
Spring Boot Application
    │
    ├─ Detects: Is this a READ or WRITE?
    │
    ├─ WRITE → Route to Primary
    │   ├─ INSERT, UPDATE, DELETE go to Primary only
    │   └─ @Transactional methods
    │
    ├─ READ (Consistent) → Route to Primary
    │   ├─ SELECT for validation (balance check, duplicate check)
    │   ├─ Must be current data
    │   └─ @PrimaryRead annotation
    │
    └─ READ (Eventual) → Route to Replica
        ├─ SELECT for reporting (statements, history)
        ├─ Stale data acceptable
        └─ @ReadOnly annotation

Spring AbstractRoutingDataSource:
    │
    ├─ Determines: Primary or Replica?
    │
    └─ Routes to appropriate DataSource
```

### Step-by-Step Implementation

**Step 1: Create Routing Logic**

```java
// Enum to indicate which datasource
public enum DataSourceType {
    PRIMARY,
    REPLICA
}

// ThreadLocal storage for current datasource type
public class DataSourceContext {
    private static final ThreadLocal<DataSourceType> context = 
        ThreadLocal.withInitial(() -> DataSourceType.PRIMARY);
    
    public static void setDataSourceType(DataSourceType type) {
        context.set(type);
    }
    
    public static DataSourceType getDataSourceType() {
        return context.get();
    }
    
    public static void clear() {
        context.remove();
    }
}

// Custom routing datasource
public class CitiCoreRoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        return DataSourceContext.getDataSourceType();
    }
}
```

**Step 2: Create Annotations**

```java
// @ReadOnly: Route to Replica
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ReadOnly {}

// @PrimaryRead: Route to Primary (strong consistency)
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PrimaryRead {}
```

**Step 3: Create AOP Aspect**

```java
@Aspect
@Component
public class DataSourceRoutingAspect {
    
    // Route @ReadOnly methods to Replica
    @Before("@annotation(ReadOnly)")
    public void setReplicaDataSource() {
        DataSourceContext.setDataSourceType(DataSourceType.REPLICA);
        logger.debug("Routing to REPLICA datasource");
    }
    
    // Route @PrimaryRead methods to Primary
    @Before("@annotation(PrimaryRead)")
    public void setPrimaryDataSource() {
        DataSourceContext.setDataSourceType(DataSourceType.PRIMARY);
        logger.debug("Routing to PRIMARY datasource");
    }
    
    // Default: Route @Transactional to Primary
    @Before("@annotation(Transactional)")
    public void setTransactionalDataSource() {
        DataSourceContext.setDataSourceType(DataSourceType.PRIMARY);
        logger.debug("Routing to PRIMARY datasource (transactional)");
    }
    
    // Cleanup after method completes
    @After("@annotation(ReadOnly) || @annotation(PrimaryRead) || @annotation(Transactional)")
    public void clearDataSourceContext() {
        DataSourceContext.clear();
    }
}
```

**Step 4: Configure DataSources**

```java
@Configuration
public class DataSourceConfiguration {
    
    // Primary DataSource
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.primary")
    public DataSourceProperties primaryDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    public DataSource primaryDataSource() {
        return primaryDataSourceProperties()
            .initializeDataSourceBuilder()
            .type(HikariDataSource.class)
            .build();
    }
    
    // Replica DataSource
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.replica")
    public DataSourceProperties replicaDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    public DataSource replicaDataSource() {
        return replicaDataSourceProperties()
            .initializeDataSourceBuilder()
            .type(HikariDataSource.class)
            .build();
    }
    
    // Routing DataSource
    @Bean
    public DataSource routingDataSource(
        @Qualifier("primaryDataSource") DataSource primary,
        @Qualifier("replicaDataSource") DataSource replica) {
        
        CitiCoreRoutingDataSource routingDataSource = new CitiCoreRoutingDataSource();
        
        Map<Object, Object> dataSourceMap = new HashMap<>();
        dataSourceMap.put(DataSourceType.PRIMARY, primary);
        dataSourceMap.put(DataSourceType.REPLICA, replica);
        
        routingDataSource.setTargetDataSources(dataSourceMap);
        routingDataSource.setDefaultTargetDataSource(primary);
        
        return new LazyConnectionDataSourceProxy(routingDataSource);
    }
}
```

**Step 5: Use Annotations in Service**

```java
@Service
public class AccountService {
    
    // Read from Primary (strong consistency required)
    @PrimaryRead
    public BigDecimal getAccountBalance(String accountNumber) {
        // Must use PRIMARY for current balance
        Account account = accountRepository.findByAccountNumber(accountNumber);
        return account.getBalance();
    }
    
    // Read from Replica (eventual consistency acceptable)
    @ReadOnly
    public List<Statement> getStatementHistory(String accountNumber) {
        // Can use REPLICA for historical data
        return statementRepository.findByAccountNumber(accountNumber);
    }
    
    // Write to Primary (default)
    @Transactional
    public void debitAccount(String accountNumber, BigDecimal amount) {
        // Must use PRIMARY for writes
        Account account = accountRepository.findByAccountNumber(accountNumber);
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
    }
}
```

---

## Read/Write Routing with Annotations

### Decision Matrix

```
Operation Type          → Datasource    Reason
─────────────────────────────────────────────────────────────
INSERT, UPDATE, DELETE  → PRIMARY       Only primary accepts writes
CREATE, ALTER, DROP     → PRIMARY       Schema changes only on primary
SELECT (balance check)  → PRIMARY       Need current data
SELECT (transaction)    → PRIMARY       Validation requires current data
SELECT (statements)     → REPLICA       Historical data, eventual-ok
SELECT (reporting)      → REPLICA       Stale data acceptable
BEGIN TRANSACTION       → PRIMARY       Writes in transaction
COMMIT/ROLLBACK         → PRIMARY       Part of transaction
```

### Real-World Example

```java
// User wants to transfer $500 from Account A to Account B

@Transactional
public void transferFunds(String fromAccount, String toAccount, BigDecimal amount) {
    
    // Step 1: Get balances (STRONG CONSISTENCY needed)
    @PrimaryRead
    BigDecimal fromBalance = getBalance(fromAccount); // PRIMARY
    
    // Step 2: Validate sufficient balance
    if (fromBalance.compareTo(amount) < 0) {
        throw new InsufficientFundsException();
    }
    
    // Step 3: Debit source account (WRITE to PRIMARY)
    debitAccount(fromAccount, amount); // PRIMARY
    
    // Step 4: Credit destination account (WRITE to PRIMARY)
    creditAccount(toAccount, amount); // PRIMARY
    
    // Step 5: Log transaction (WRITE to PRIMARY)
    createStatement(fromAccount, toAccount, amount); // PRIMARY
    
    // Step 6: Publish event (async, can be eventual-consistent)
    @ReadOnly
    publishTransferEvent(...); // Could use REPLICA
}

// Later: Customer checks transaction history
@ReadOnly
public List<Statement> getTransactions(String accountNumber) {
    // Can use REPLICA - customer accepts slight delay in seeing transactions
    return statementRepository.findByAccountNumber(accountNumber); // REPLICA
}
```

---

## AOP-Based Routing

### Definition

**Aspect-Oriented Programming (AOP)** intercepts method calls to add behavior (like datasource routing) without changing method code.

### How It Works

```
Method Call:
    │
    ↓
AOP Interceptor ("Before" advice):
├─ Checks for @ReadOnly annotation
├─ Sets DataSourceContext.REPLICA
├─ Continues to method

Method Executes:
├─ Checks DataSourceContext
├─ Routes query to REPLICA
└─ Returns result

AOP Interceptor ("After" advice):
├─ Clears DataSourceContext
└─ Returns to caller
```

### Real Problem & Solution

**Problem: Replica Lag**

```
Timeline:
12:00:00 - Primary writes: balance = $5000
12:00:00 - Kafka publishes: balance changed event

12:00:02 - Replication lag = 2 seconds
          - Replica still has old data: balance = $4500

12:00:02 - Customer calls getBalance()
          - @ReadOnly → routes to REPLICA
          - Gets stale data: $4500
          - Expects: $5000
```

**Solution: Use @PrimaryRead for Balance**

```java
@PrimaryRead  // Force PRIMARY for current balance
public BigDecimal getBalance(String accountNumber) {
    // Gets PRIMARY directly: $5000 (current)
}

@ReadOnly  // Can use REPLICA for history
public List<Statement> getStatements(String accountNumber) {
    // Uses REPLICA: sufficient lag tolerance
}
```

---

## Replica Health Monitoring

### Definition

**Health Monitoring** checks if Replica is up-to-date and falls back to Primary if Replica is behind or down.

### What You Implemented

```java
@Component
public class ReplicaHealthMonitor {
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    private final AtomicBoolean replicaHealthy = new AtomicBoolean(true);
    
    // Monitor replica every 5 seconds
    @Scheduled(fixedRate = 5000)
    public void checkReplicaHealth() {
        try {
            // Get seconds behind master
            Integer secondsBehind = jdbcTemplate.queryForObject(
                "SHOW REPLICA STATUS\\G",
                (rs, rowNum) -> rs.getInt("Seconds_Behind_Master")
            );
            
            // If more than 10 seconds behind, mark unhealthy
            if (secondsBehind != null && secondsBehind > 10) {
                replicaHealthy.set(false);
                logger.warn("Replica is {} seconds behind", secondsBehind);
            } else {
                replicaHealthy.set(true);
                logger.debug("Replica healthy");
            }
        } catch (Exception e) {
            replicaHealthy.set(false);
            logger.error("Replica health check failed", e);
        }
    }
    
    public boolean isHealthy() {
        return replicaHealthy.get();
    }
}

// In routing logic
@Before("@annotation(ReadOnly)")
public void setDataSourceRoute() {
    if (replicaHealthMonitor.isHealthy()) {
        DataSourceContext.setDataSourceType(DataSourceType.REPLICA);
    } else {
        // Fallback to PRIMARY if replica unhealthy
        DataSourceContext.setDataSourceType(DataSourceType.PRIMARY);
        logger.warn("Replica unhealthy, falling back to PRIMARY");
    }
}
```

---

## Transaction Outbox Pattern

### Definition

**Transaction Outbox Pattern** ensures reliable event publishing by storing events in the same database transaction as business operations.

### Problem It Solves

```
Scenario: Debit account and publish event

Without Outbox (Unreliable):
1. Debit account (write to primary) ✓
2. Publish event to Kafka ✓
3. App crashes before publishing

Result: Account debited but event not published
        Customer doesn't know why balance changed
        Notification service never sends alert

With Outbox (Reliable):
1. BEGIN TRANSACTION
2. Debit account (write to primary) ✓
3. Write event to outbox table ✓
4. COMMIT (both operations together)
5. Separate process reads outbox and publishes

Result: Even if app crashes, event is in database
        Outbox publisher will eventually send it
        No data loss
```

### Implementation

```sql
CREATE TABLE account_outbox (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_id VARCHAR(50) UNIQUE,
    event_type VARCHAR(50),
    event_payload JSON,
    status ENUM('PENDING', 'SENT', 'FAILED'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```java
@Transactional
public void debitAccount(String accountNumber, BigDecimal amount) {
    // Step 1: Update account
    Account account = accountRepository.findByAccountNumber(accountNumber);
    account.setBalance(account.getBalance().subtract(amount));
    accountRepository.save(account);
    
    // Step 2: Create outbox event (SAME TRANSACTION)
    OutboxEvent event = new OutboxEvent();
    event.setEventId(UUID.randomUUID().toString());
    event.setEventType("ACCOUNT_DEBITED");
    event.setEventPayload(jsonOf(account, amount));
    event.setStatus(OutboxStatus.PENDING);
    outboxRepository.save(event);
    
    // COMMIT: Both account change and event stored together
    // If crash before commit: neither change is persisted
    // If crash after commit: both persisted, event will be published
}

// Separate scheduled task publishes from outbox
@Scheduled(fixedRate = 5000)
public void publishPendingEvents() {
    List<OutboxEvent> pending = outboxRepository.findByStatus(OutboxStatus.PENDING);
    
    for (OutboxEvent event : pending) {
        try {
            kafkaTemplate.send(event.getEventType(), event.getEventPayload());
            event.setStatus(OutboxStatus.SENT);
            outboxRepository.save(event);
        } catch (Exception e) {
            event.setStatus(OutboxStatus.FAILED);
            outboxRepository.save(event);
        }
    }
}
```

---

## Real Issues & Solutions

### Issue #1: Docker Windows Path Issues

**Problem:**

```
Error: not a directory: Are you trying to mount a directory onto a file (or vice-versa)?
       Path: C:\Users\...\primary.cnf
```

**Root Cause:**

Windows path format incompatible with Docker volume mount syntax.

**Solution:**

```bash
# Use forward slashes instead of backslashes
# Or use WSL2 paths

# Option 1: Convert Windows path
# Before: C:\Users\Admin\Project\primary.cnf
# After: /c/users/admin/project/primary.cnf

# Option 2: Use Docker Desktop's automatic path conversion
# Works if project in default locations

# Option 3: Copy file into container
COPY primary.cnf /etc/mysql/conf.d/
```

### Issue #2: Replica SSL Certificate Errors

**Problem:**

```
SSLHandshakeException: certificate_unknown
Path does not chain with any of the trust anchors
```

**Root Cause:**

Java doesn't trust AWS RDS CA certificate (not in default truststore).

**Solution:**

```bash
# Download AWS RDS CA certificate
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# Create Java truststore with AWS CA
keytool -import -alias rds-ca -file global-bundle.pem \
        -keystore rds-truststore.jks -storepass changeit -noprompt

# Configure JVM to use truststore
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=rds-truststore.jks \
  -Djavax.net.ssl.trustStorePassword=changeit"

# Configure JDBC URL
jdbc-url: jdbc:mysql://rds.amazonaws.com/db?sslMode=VERIFY_IDENTITY
```

### Issue #3: Replica Lag Consistency Issues

**Problem:**

```
User transfers $500 from Account A to Account B
Immediately checks Account A balance
Gets stale balance (transfer not yet replicated)
Thinks transfer didn't go through (confusion)
```

**Root Cause:**

Read from Replica, which is 2-3 seconds behind Primary.

**Solution:**

```java
// Use @PrimaryRead for operations that need current data
@PrimaryRead
public BigDecimal getBalance(String accountNumber) {
    // Routes to PRIMARY, always returns current balance
}

// Can use @ReadOnly for non-critical reads
@ReadOnly
public List<Statement> getStatements(String accountNumber) {
    // Routes to REPLICA, eventual consistency acceptable
}
```

---

## Security Improvements

### Before (Insecure)

```yaml
# Original configuration
datasource:
  url: jdbc:mysql://localhost:3306/account?useSSL=false
  username: root
  password: password123
```

**Security Issues:**

```
1. useSSL=false
   ├─ Database traffic unencrypted
   ├─ Passwords visible in network
   └─ Man-in-the-middle attack possible

2. Hard-coded password
   ├─ Password in source code
   ├─ Visible in Git history
   ├─ Visible if code leaked
   └─ Can't rotate password without rebuild

3. Server certificate not validated
   ├─ Connecting to random server OK
   ├─ Attacker can intercept connection
   └─ User doesn't know they're hacked

4. Root account for all operations
   ├─ Full database access not needed
   ├─ One compromised account = full breach
   └─ Can't identify which account did what
```

### After (Secure)

```yaml
# Production configuration
datasource:
  primary:
    jdbc-url: jdbc:mysql://rds.amazonaws.com:3306/account?sslMode=VERIFY_IDENTITY&serverTimezone=UTC
    username: ${DB_PRIMARY_USERNAME}
    password: ${DB_PRIMARY_PASSWORD}
    driver-class-name: com.mysql.cj.jdbc.Driver
    
  replica:
    jdbc-url: jdbc:mysql://rds-replica.amazonaws.com:3306/account?sslMode=VERIFY_IDENTITY&serverTimezone=UTC
    username: ${DB_REPLICA_USERNAME}
    password: ${DB_REPLICA_PASSWORD}
    driver-class-name: com.mysql.cj.jdbc.Driver

jpa:
  hibernate:
    ddl-auto: validate
```

**Security Improvements:**

```
1. sslMode=VERIFY_IDENTITY
   ├─ Encrypted database traffic (TLS 1.3)
   ├─ Passwords encrypted in transit
   ├─ Server certificate validated
   └─ Hostname verification prevents spoofing

2. Passwords from environment variables
   ├─ Not in source code
   ├─ Not in Git history
   ├─ Can be rotated without rebuild
   └─ AWS Secrets Manager integration possible

3. Read-only user for replica
   ├─ Even if replica credentials leaked, read-only
   ├─ Can't accidentally delete from replica
   ├─ Damage limited to reading data

4. Primary user has limited permissions
   ├─ Only on citicore_account database
   ├─ Not root
   ├─ Audit trail shows which account made changes
   └─ Easier to investigate security incidents

5. ddl-auto: validate
   ├─ Schema won't auto-modify in production
   ├─ Prevents accidental schema changes
   ├─ Forces explicit migration management
   └─ No surprise schema modifications
```

---

## Interview-Ready Explanations

### "Walk me through your database architecture"

**Response:**

"I designed a scalable banking database architecture with master-replica replication and intelligent routing. 

**Architecture layers:**

1. **Primary/Replica Separation**: The primary database handles all writes, while the replica handles reads. This decouples write operations from read operations, allowing reads to scale without competing with writes.

2. **Routing Logic**: Spring Boot application uses AOP to automatically route queries. Transactional methods go to Primary. Methods marked with @ReadOnly go to Replica for eventual-consistent reads. Methods marked with @PrimaryRead force Primary for strong consistency (balance checks).

3. **Table Partitioning**: High-volume tables like account_statements are partitioned monthly by created_at. This enables partition pruning—queries with date filters only scan relevant partitions, making them 12x faster than querying 12 months of data.

4. **Event Reliability**: I use the Transactional Outbox pattern. When debiting an account, the debit and the event are written in the same database transaction. Even if the application crashes, the event is persisted and a background job publishes it to Kafka later.

5. **Security**: All database connections use TLS 1.3 with VERIFY_IDENTITY. Passwords come from environment variables, not hard-coded. I use separate users—write user for primary, read-only user for replica—to enforce least privilege.

**Why this matters for banking**: Write/read separation lets me handle thousands of read queries (customers checking balances) without slowing down transaction processing. Partitioning keeps queries fast even with millions of historical transactions. Outbox pattern ensures no transaction is lost. TLS protects passwords and data in transit."

### "How do you handle replica lag?"

**Response:**

"Replica lag is asynchronous replication delay. The primary accepts a write, but takes 1-2 seconds to replicate to the replica.

**The problem**: If I route balance-check queries to the replica and they come too quickly, they see stale data.

**My solution**: I use selective routing. Operations requiring strong consistency (balance validation before transfer) use @PrimaryRead to route to Primary. Operations where eventual consistency is fine (viewing transaction history) use @ReadOnly to route to Replica.

Additionally, I monitor replica health—if Seconds_Behind_Master exceeds 10 seconds, I temporarily fall back to Primary for all reads. This prevents sending reads to a severely lagged replica.

**For banking specifically**: Transfer validation must check current balance against Primary. But showing historical statements can come from Replica. This balance (pun intended) gives me read scaling without sacrificing consistency where it matters."

### "What problems did you encounter with Docker MySQL?"

**Response:**

"Two main problems taught me important lessons:

**Problem 1: Configuration Files Ignored**
MySQL was ignoring my replication configuration files. The error was 'World-writable config file is ignored.' MySQL has a security check—it won't read configuration files with 777 permissions. The fix was chmod 644 to restrict permissions. This taught me that Docker file permissions matter.

**Problem 2: SSL Handshake Failures with RDS**
When moving from local Docker to AWS RDS, SSL connections failed with 'certificate_unknown.' Java didn't trust AWS's certificate chain. The fix was downloading the RDS CA certificate, creating a Java truststore, and configuring the JVM. This taught me that cloud-provided certificates need explicit trust setup in Java.

The key lesson: Test each layer independently. First, I tested RDS connectivity with MySQL CLI directly. Then I tested Java TLS separately. Then I configured Spring Boot. Testing layer-by-layer prevented me from incorrectly blaming AWS networking when the issue was Java configuration."

---

## Verification Checklist

```
Pre-Deployment:
☐ Primary RDS instance created and accessible
☐ Replica RDS instance created and syncing
☐ Replication lag < 1 second
☐ TLS certificates downloaded and truststore created
☐ Database users created (write user, read-only user)
☐ Database schema created with partitions
☐ Outbox table created

Spring Boot Configuration:
☐ Primary JDBC URL configured correctly
☐ Replica JDBC URL configured correctly
☐ Passwords from environment variables (not hard-coded)
☐ JVM truststore configured with JAVA_TOOL_OPTIONS
☐ TLS sslMode set to VERIFY_IDENTITY
☐ DDL auto set to validate (not update)
☐ schema.sql startup execution disabled

AOP & Routing:
☐ @ReadOnly routes to Replica
☐ @PrimaryRead routes to Primary
☐ @Transactional defaults to Primary
☐ DataSourceContext properly cleaned up after method
☐ Replica health monitor running
☐ Fallback to Primary when replica unhealthy

Application Level:
☐ Writes use Primary only
☐ Balance checks use @PrimaryRead
☐ Reports use @ReadOnly
☐ Outbox events created in same transaction as writes
☐ Background job publishing outbox events
☐ Replica lag monitoring in place
☐ CloudWatch logs configured for slow queries
```

---
