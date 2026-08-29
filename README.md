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
# CitiCore AWS Infrastructure & Deployment Guide

**Complete infrastructure architecture for a Spring Cloud microservices banking platform on AWS, covering VPC setup, RDS migration, ECS containerization, Spring Cloud Config Server, event-driven architecture with Kafka, and distributed configuration management.**

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [AWS Account Setup](#aws-account-setup)
3. [VPC Architecture](#vpc-architecture)
4. [RDS Migration](#rds-migration)
5. [Security Groups](#security-groups)
6. [Kafka + Redis on EC2](#kafka--redis-on-ec2)
7. [ECR Setup](#ecr-setup)
8. [ECS Deployment](#ecs-deployment)
9. [Spring Cloud Config Server](#spring-cloud-config-server)
10. [GitHub Webhook Integration](#github-webhook-integration)
11. [Spring Cloud Bus Configuration](#spring-cloud-bus-configuration)
12. [Real Issues & Solutions](#real-issues--solutions)
13. [Production Hardening](#production-hardening)
14. [Cost Optimization](#cost-optimization)
15. [Troubleshooting](#troubleshooting)
16. [Interview-Ready Decisions](#interview-ready-decisions)

---

## Architecture Overview

```
                         INTERNET
                            |
                            v
                    +───────────────+
                    |      ALB      |
                    |   Public SG   |
                    +───────┬───────+
                            |
          ┌─────────────────┼─────────────────┐
          |                 |                 |
          v                 v                 v
      API Gateway    Config Server     Eureka Server
      (ECS/Fargate) (ECS/Fargate)    (ECS/Fargate)
          |
    ┌─────┴─────────────────┐
    |                       |
    v                       v
Auth Service           User Service
(ECS/Fargate)         (ECS/Fargate)
    |                       |
    └─────────┬─────────────┘
              |
              v
       Account Service
       Transaction Service
       (ECS/Fargate)
              |
              v
   Notification Service


       ┌──────────────────────────────────────┐
       │      PRIVATE CITICORE VPC            │
       │      10.0.0.0/16                     │
       │                                       │
       │  ┌──────────────┐  ┌──────────────┐ │
       │  │  Kafka EC2   │  │  Redis EC2   │ │
       │  │ :9092        │  │ :6379        │ │
       │  │ Docker       │  │ Docker       │ │
       │  │              │  │              │ │
       │  └──────────────┘  └──────────────┘ │
       │                                       │
       │  ┌──────────────────────────────────┐ │
       │  │   RDS MySQL Primary              │ │
       │  │   :3306                          │ │
       │  │                                  │ │
       │  │   ├─ Replica (Read)              │ │
       │  │   │  :3306                       │ │
       │  └──────────────────────────────────┘ │
       │                                       │
       └──────────────────────────────────────┘
```

**Key Principles:**
- Internet-facing: Only ALB and managed HTTPS endpoints
- Private services: Kafka, Redis, RDS, ECS (except via ALB)
- Security groups isolate traffic at each layer
- Cost-conscious: Self-managed Kafka/Redis (no MSK/ElastiCache)
- Spring Cloud integration: Config Server, Bus, Eureka

---

## AWS Account Setup

### Region Selection

```
Region: ap-south-1 (Mumbai)
```

**Why Mumbai?**
- Lowest latency for India-based users
- Free tier availability
- RDS and ECS services available

### VPC Creation

```bash
# Create VPC
Name: citicore-vpc
CIDR: 10.0.0.0/16
```

**CIDR Design:**

```
10.0.0.0/16 (Main VPC)
├── 10.0.1.0/24 - Public A (ALB, NAT Gateway future)
├── 10.0.2.0/24 - Public B
├── 10.0.11.0/24 - Private A (ECS, App)
├── 10.0.12.0/24 - Private B (ECS, App)
├── 10.0.21.0/24 - Database A (RDS)
└── 10.0.22.0/24 - Database B (RDS)
```

**Why separate database subnets?**
- RDS requires Multi-AZ placement across subnets
- Isolates database traffic from application traffic
- Simplifies security group rules
- Improves database subnet group management

### Internet Gateway

```bash
# Create IGW
Name: citicore-igw

# Attach to VPC
Attach to: citicore-vpc
```

**Traffic flow:**
```
Internet → IGW → Public subnets → ALB → Private subnets (ECS)
                                     ↓
                                 (ALB routes to ECS)
```

---

## VPC Architecture

### Subnet Layout

| Subnet | CIDR | AZ | Purpose | Route Table |
|--------|------|-----|---------|------------|
| citicore-public-a | 10.0.1.0/24 | ap-south-1a | ALB, Public resources | Public RT |
| citicore-public-b | 10.0.2.0/24 | ap-south-1b | ALB, Public resources | Public RT |
| citicore-private-a | 10.0.11.0/24 | ap-south-1a | ECS services | Private RT |
| citicore-private-b | 10.0.12.0/24 | ap-south-1b | ECS services | Private RT |
| citicore-db-a | 10.0.21.0/24 | ap-south-1a | RDS | DB RT |
| citicore-db-b | 10.0.22.0/24 | ap-south-1b | RDS | DB RT |

### Route Tables

**Public Route Table:**

```
Destination | Target
────────────|──────────
10.0.0.0/16 | local
0.0.0.0/0   | citicore-igw
```

Associated: citicore-public-a, citicore-public-b

**Private Route Table:**

```
Destination | Target
────────────|──────────
10.0.0.0/16 | local
```

Associated: All private and database subnets

**Important:** No NAT Gateway is configured to minimize costs. For production, consider NAT Gateway or VPN for outbound internet access from private subnets.

---

## Security Groups

### Architecture

```
Internet
   |
   v
citicore-alb-sg (ALB)
   |
   +─── HTTP:80, HTTPS:443
        |
        v
citicore-ecs-sg (ECS Services)
   |
   +─── TCP:3306 ──→ citicore-rds-sg (RDS)
   |
   +─── TCP:9092 ──→ citicore-kafka-sg (Kafka EC2)
   |
   +─── TCP:6379 ──→ citicore-redis-sg (Redis EC2)
```

### Individual Security Groups

#### ALB Security Group

```yaml
Name: citicore-alb-sg
VPC: citicore-vpc

Inbound Rules:
  - Protocol: TCP
    Port: 80
    Source: 0.0.0.0/0
  - Protocol: TCP
    Port: 443
    Source: 0.0.0.0/0

Outbound Rules:
  - All traffic to 0.0.0.0/0
```

#### ECS Security Group

```yaml
Name: citicore-ecs-sg
VPC: citicore-vpc

Inbound Rules:
  - Protocol: TCP
    Port: 8080-8090
    Source: citicore-alb-sg
  - Protocol: TCP
    Port: 8888  (Config Server)
    Source: 0.0.0.0/0  (For webhook)

Outbound Rules:
  - All traffic to 0.0.0.0/0
```

#### RDS Security Group

```yaml
Name: citicore-rds-sg
VPC: citicore-vpc

Inbound Rules:
  - Protocol: TCP
    Port: 3306
    Source: citicore-ecs-sg

Outbound Rules:
  - All traffic to 0.0.0.0/0 (allows replication between Primary/Replica)
```

#### Kafka Security Group

```yaml
Name: citicore-kafka-sg
VPC: citicore-vpc

Inbound Rules:
  - Protocol: TCP
    Port: 9092
    Source: citicore-ecs-sg

Outbound Rules:
  - All traffic to 0.0.0.0/0
```

#### Redis Security Group

```yaml
Name: citicore-redis-sg
VPC: citicore-vpc

Inbound Rules:
  - Protocol: TCP
    Port: 6379
    Source: citicore-ecs-sg

Outbound Rules:
  - All traffic to 0.0.0.0/0
```

---

## RDS Migration

### Challenge: Switching from VPC A to VPC B

**Scenario:**

Original RDS was in `vpc-0f3de40cadfd9316c`. New VPC is `vpc-0d064f45265cbcdad`.

Account hit RDS instance limit, preventing direct instance-to-instance migration.

### Solution: Snapshot → Restore

**Step 1: Create Snapshot**

```sql
-- AWS Console
DB Instances → citicore-mysql-primary → Snapshots → Take Snapshot

Name: citicore-mysql-primary-before-vpc-migration
Status: Available (wait for completion)
```

**Why snapshot?**
- Preserves all data and schema
- Allows recovery if restore fails
- No downtime for snapshot creation
- Point-in-time recovery capability

**Step 2: Delete Old Instances**

```bash
# After snapshot is available
AWS Console → DB Instances → Delete

Instance: citicore-mysql-replica → Delete
Instance: citicore-mysql-primary → Delete
```

**Step 3: Restore from Snapshot**

```bash
AWS Console → Snapshots → citicore-mysql-primary-before-vpc-migration

Restore to DB Instance

Name: citicore-mysql-primary
DB Instance Class: db.t3.micro
VPC: citicore-vpc (NEW)
DB Subnet Group: citicore-db-subnet-group
Security Group: citicore-rds-sg
Publicly Accessible: No
```

**Step 4: Create Read Replica**

```bash
AWS Console → DB Instances → citicore-mysql-primary

Create Read Replica

Name: citicore-mysql-replica
Instance Class: db.t3.micro
VPC: citicore-vpc
Availability Zone: (same or different)
```

### Verification

```bash
# Check replication
AWS Console → DB Instances → citicore-mysql-replica

Status: Available
Role: Replica
Replication State: Replicating
Replication Lag: 0 seconds ✅
```

---

## Kafka + Redis on EC2

### EC2 Instance Setup

```yaml
Name: citicore-infra
Instance Type: t3.small
VPC: citicore-vpc
Subnet: citicore-public-a
Private IP: 10.0.1.87
Public IP: 3.6.94.35
OS: Ubuntu 26.04 LTS
Security Groups:
  - citicore-kafka-sg
  - citicore-redis-sg
```

**Why t3.small?**
- Free tier eligible (up to 750 hours/month)
- Sufficient for Kafka + Redis in development
- 2 vCPU, 2 GB RAM

**Why public IP?**
- Allows SSH administration
- For production: use Systems Manager Session Manager (no public IP needed)

### Docker Installation

```bash
ssh -i citicore-ec2-key.pem ubuntu@3.6.94.35

# Update
sudo apt update && sudo apt upgrade -y

# Install Docker
sudo apt install -y docker.io docker-compose-v2

# Add user to docker group
sudo usermod -aG docker ubuntu
# Refresh environment
newgrp docker

# Verify
docker --version
docker compose version
docker run hello-world
```

### Docker Compose Configuration

```yaml
# ~/citicore-infra/docker-compose.yml

version: '3.8'

services:
  kafka:
    image: apache/kafka:4.0.1
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://kafka:9093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9093,PLAINTEXT_HOST://10.0.1.87:9092
      KAFKA_CONTROLLER_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_HEAP_OPTS: "-Xms256M -Xmx512M"
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - citicore-network

  redis:
    image: redis:8-alpine
    container_name: redis
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    networks:
      - citicore-network

volumes:
  kafka_data:
  redis_data:

networks:
  citicore-network:
    driver: bridge
```

### Deployment

```bash
cd ~/citicore-infra

# Validate
docker compose config

# Start
docker compose up -d

# Verify
docker compose ps
docker logs kafka
docker logs redis

# Test Kafka
docker exec kafka kafka-topics.sh --create --topic test --bootstrap-server kafka:9092

# Test Redis
docker exec redis redis-cli ping
# Output: PONG
```

### Private IP Configuration

**Critical for ECS connectivity:**

Kafka advertised address must be the EC2 private IP:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://10.0.1.87:9092
```

This ensures ECS services in private subnets can reach Kafka using the private network.

---

## ECR Setup

### Create Repository

```bash
aws ecr create-repository \
  --repository-name citicore/config-server \
  --region ap-south-1

# Output
{
  "repository": {
    "repositoryUri": "580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server"
  }
}
```

### Build and Push Docker Image

```bash
# From application root

# Build
docker build -f Dockerfile.config-server -t citicore/config-server:1.0 .

# Tag for ECR
docker tag citicore/config-server:1.0 \
  580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0

# Login to ECR
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin \
  580655778303.dkr.ecr.ap-south-1.amazonaws.com

# Push
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0
```

---

## ECS Deployment

### ECS Cluster

```bash
# Create cluster
aws ecs create-cluster --cluster-name citicore-cluster --region ap-south-1
```

### IAM Roles

**ECS Task Execution Role:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Attach policies:
- `AmazonECSTaskExecutionRolePolicy`
- `SecretsManagerReadWrite` (for GitHub token)

**ECS Task Role:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Attach policies as needed for application (e.g., S3, DynamoDB).

### ECS Task Definition

```json
{
  "family": "citicore-config-server",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "arn:aws:iam::580655778303:role/citicore-ecs-task-execution-role",
  "taskRoleArn": "arn:aws:iam::580655778303:role/citicore-ecs-task-role",
  "containerDefinitions": [
    {
      "name": "config-server",
      "image": "580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0",
      "portMappings": [
        {
          "containerPort": 8888,
          "hostPort": 8888,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "CONFIG_GIT_URI",
          "value": "https://github.com/Rabbaniinamdar/citicore-config-repo.git"
        },
        {
          "name": "CONFIG_GIT_USERNAME",
          "value": "Rabbaniinamdar"
        },
        {
          "name": "KAFKA_BOOTSTRAP_SERVERS",
          "value": "10.0.1.87:9092"
        }
      ],
      "secrets": [
        {
          "name": "CONFIG_GIT_TOKEN",
          "valueFrom": "arn:aws:secretsmanager:ap-south-1:580655778303:secret:citicore/config-server/github-token-CS1bD2"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/citicore-config-server",
          "awslogs-region": "ap-south-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

### ECS Service

```bash
aws ecs create-service \
  --cluster citicore-cluster \
  --service-name config-server \
  --task-definition citicore-config-server:2 \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-12345,subnet-67890],securityGroups=[sg-ecs-id],assignPublicIp=DISABLED}" \
  --load-balancers targetGroupArn=arn:aws:elasticloadbalancing:ap-south-1:580655778303:targetgroup/citicore-config-tg/abc123,containerName=config-server,containerPort=8888
```

---

## Spring Cloud Config Server

### Configuration

```yaml
# application.yml
spring:
  application:
    name: config-server
  cloud:
    config:
      server:
        git:
          uri: ${CONFIG_GIT_URI}
          username: ${CONFIG_GIT_USERNAME}
          password: ${CONFIG_GIT_TOKEN}
          default-label: main
          clone-on-start: true
      bus:
        enabled: true
        destination: springCloudBus
    bus:
      kafka:
        broker-addresses: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
        auto-startup: true
        enabled: true

server:
  port: 8888

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,bus-refresh,monitor
  endpoint:
    health:
      show-details: always
    bus-refresh:
      enabled: true
```

### Dependencies

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-config-server</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-bus</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-bus-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-config-monitor</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
</dependencies>
```

### Main Class

```java
@SpringBootApplication
@EnableConfigServer
public class ConfigServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConfigServerApplication.class, args);
    }
}
```

---

## GitHub Webhook Integration

### Secrets Manager Setup

**Create GitHub Token Secret:**

```bash
aws secretsmanager create-secret \
  --name citicore/config-server/github-token \
  --secret-string "ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
  --region ap-south-1
```

### ALB Configuration

**Create Target Group:**

```bash
aws elbv2 create-target-group \
  --name citicore-config-tg \
  --protocol HTTP \
  --port 8888 \
  --vpc-id vpc-0d064f45265cbcdad \
  --target-type ip
```

**Create ALB Listener:**

```bash
aws elbv2 create-load-balancer \
  --name citicore-config-alb \
  --subnets subnet-public-a subnet-public-b \
  --security-groups sg-alb-id \
  --scheme internet-facing

# Add listener
aws elbv2 create-listener \
  --load-balancer-arn arn:aws:elasticloadbalancing:ap-south-1:580655778303:loadbalancer/app/citicore-config-alb/abc123 \
  --protocol HTTP \
  --port 80 \
  --default-actions Type=forward,TargetGroupArn=arn:aws:elasticloadbalancing:ap-south-1:580655778303:targetgroup/citicore-config-tg/def456
```

**Get ALB DNS:**

```bash
ALB DNS: citicore-config-alb-1654378622.ap-south-1.elb.amazonaws.com
```

### GitHub Webhook Setup

1. Go to `Rabbaniinamdar/citicore-config-repo` → Settings → Webhooks
2. Click "Add webhook"
3. Payload URL: `http://citicore-config-alb-1654378622.ap-south-1.elb.amazonaws.com/monitor`
4. Content type: `application/json`
5. Events: `Push events`
6. Active: ✓

**Note:** For production, use HTTPS with a valid certificate.

### Test Webhook

```bash
# Push to config repository
git -C ~/citicore-config-repo add .
git -C ~/citicore-config-repo commit -m "Test webhook"
git -C ~/citicore-config-repo push

# Check CloudWatch logs
aws logs tail /ecs/citicore-config-server --follow

# Output should show:
# PropertyPathEndpoint : Refresh for: *
```

---

## Spring Cloud Bus Configuration

### Kafka Topic Provisioning

When Config Server starts, it automatically creates the Kafka topic:

```
springCloudBus
```

**Verification:**

```bash
# Inside Kafka EC2
docker exec kafka kafka-topics.sh \
  --list \
  --bootstrap-server kafka:9092

# Output includes: springCloudBus
```

### Refresh Flow

```
Developer
    |
    | git push
    v
GitHub Config Repository
    |
    | Webhook payload
    v
Config Server /monitor
    |
    | Process: Refresh for: *
    v
Spring Cloud Bus
    |
    | Publish event
    v
Kafka springCloudBus topic
    |
    v
Config Clients (subscribed)
    |
    | Refresh @RefreshScope beans
    v
New configuration applied
```

---

## Real Issues & Solutions

### Issue #1: RDS Replication Lag Shows Negative Values

**Scenario:**

```
Seconds_Behind_Master: -1
```

**Root Cause:**

Replica hasn't started replicating yet, or there's a timing issue reading the replica status.

**Solution:**

```sql
-- On replica
SHOW SLAVE STATUS\G

-- Check:
Slave_IO_Running: Yes
Slave_SQL_Running: Yes
Seconds_Behind_Master: 0
```

**Verification:**

```bash
# AWS Console
DB Instances → citicore-mysql-replica

Replication Lag: 0 seconds ✅
```

---

### Issue #2: Kafka Advertised Listeners Not Reachable from ECS

**Error:**

```
org.apache.kafka.common.errors.UnknownHostException:
Failed to create new KafkaAdminClient
```

**Root Cause:**

Kafka advertised address was incorrect or not reachable from private subnets.

**Solution:**

```yaml
# docker-compose.yml
environment:
  # WRONG:
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9093,PLAINTEXT_HOST://localhost:9092
  
  # CORRECT:
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9093,PLAINTEXT_HOST://10.0.1.87:9092
```

**Verification:**

```bash
# From ECS task
curl kafka:9092 → Connection refused (expected, not HTTP)
telnet kafka 9092 → Should connect

# Test with producer
docker exec kafka kafka-producer-perf-test.sh \
  --topic test \
  --num-records 100 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=10.0.1.87:9092
```

---

### Issue #3: Secrets Manager Access Denied in ECS

**Error:**

```
User: arn:aws:iam::580655778303:role/citicore-ecs-task-execution-role
is not authorized to perform: secretsmanager:GetSecretValue
```

**Root Cause:**

IAM execution role lacked `SecretsManagerReadWrite` policy.

**Solution:**

```bash
# Attach policy
aws iam attach-role-policy \
  --role-name citicore-ecs-task-execution-role \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
```

Or create inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:ap-south-1:580655778303:secret:citicore/config-server/github-token-CS1bD2"
      ]
    }
  ]
}
```

---

### Issue #4: GitHub Webhook Returns 400

**Error:**

```
HTTP 400 - MissingServletRequestParameterException
Required request parameter 'path' is not present
```

**Root Cause:**

Config Monitor endpoint was reached, but GitHub payload format might differ slightly.

**Solution:**

This warning is expected. The `/monitor` endpoint processes the event regardless:

```bash
# Check CloudWatch logs
aws logs tail /ecs/citicore-config-server --follow

# Look for:
PropertyPathEndpoint : Refresh for: *
```

The refresh flow completes successfully despite the warning.

---

## Production Hardening

### 1. Security

**Enable TLS at ALB:**

```bash
# Request certificate via ACM
aws acm request-certificate \
  --domain-name citicore-config.example.com \
  --validation-method DNS

# Update ALB listener to HTTPS
aws elbv2 modify-listener \
  --listener-arn arn:... \
  --protocol HTTPS \
  --certificates CertificateArn=arn:...
```

**Restrict webhook access:**

```json
{
  "Effect": "Allow",
  "Principal": "*",
  "Action": "elasticloadbalancing:*",
  "Resource": "*",
  "Condition": {
    "IpAddress": {
      "aws:SourceIp": ["GitHub IP ranges"]
    }
  }
}
```

### 2. High Availability

**Multiple Config Server replicas:**

```bash
aws ecs update-service \
  --cluster citicore-cluster \
  --service config-server \
  --desired-count 2 \
  --force-new-deployment
```

**Multi-AZ RDS:**

```bash
# Modify existing instance
aws rds modify-db-instance \
  --db-instance-identifier citicore-mysql-primary \
  --multi-az \
  --apply-immediately
```

### 3. Monitoring

**CloudWatch Alarms:**

```bash
# ECS CPU
aws cloudwatch put-metric-alarm \
  --alarm-name config-server-cpu \
  --alarm-description "Alert if Config Server CPU > 80%" \
  --metric-name CPUUtilization \
  --namespace AWS/ECS \
  --statistic Average \
  --period 300 \
  --threshold 80 \
  --comparison-operator GreaterThanThreshold

# RDS Replication Lag
aws cloudwatch put-metric-alarm \
  --alarm-name rds-replica-lag \
  --alarm-description "Alert if replica lag > 10 seconds" \
  --metric-name ReplicationLag \
  --namespace AWS/RDS \
  --statistic Maximum \
  --period 60 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold
```

### 4. Secrets Rotation

```yaml
# Secrets Manager automatic rotation
Rotation: Enabled
Rotation interval: 30 days
Lambda function: auto-rotate-github-token
```

---

## Cost Optimization

### Current Monthly Estimate

```
RDS (db.t3.micro): $15
ECS Fargate (0.5 vCPU, 1 GB): $20
EC2 t3.small (Kafka/Redis): $10
ALB: $16
Data transfer: $5
Total: ~$66/month
```

**Within Free Tier:** ✓ (assuming < 750 hours used)

### Cost-Saving Decisions

| Decision | Cost Impact |
|----------|------------|
| No NAT Gateway | Save $32/month |
| Self-managed Kafka (vs. MSK) | Save $50+/month |
| Self-managed Redis (vs. ElastiCache) | Save $20+/month |
| Fargate (vs. ECS on EC2 fleet) | Save $10+/month |
| Consolidate Kafka + Redis on 1 EC2 | Save $10+/month |
| No RDS Multi-AZ (production would need) | Save $15/month |

**Total Savings: ~$137/month**

### Production Considerations

For production, add:

```
Multi-AZ RDS: +$15
MSK (managed Kafka): +$50
ElastiCache (managed Redis): +$20
Bastion host for SSH: +$10
NAT Gateway: +$32
CloudWatch detailed monitoring: +$5
Total additional: ~$132/month
```

---

## Troubleshooting

### ECS Task Stuck in PROVISIONING

**Symptoms:**
```
Task Status: PROVISIONING (for > 5 minutes)
```

**Causes:**
1. Insufficient network capacity
2. Security group blocking
3. Image pull failed

**Solution:**

```bash
# Check task details
aws ecs describe-tasks \
  --cluster citicore-cluster \
  --tasks arn:... \
  --region ap-south-1

# Look for stopCode, stoppingAt
# If image pull failed:
aws ecs update-service \
  --cluster citicore-cluster \
  --service config-server \
  --force-new-deployment
```

### Config Server Can't Reach Kafka

**Symptoms:**
```
Kafka = DOWN in health endpoint
```

**Diagnosis:**

```bash
# SSH into ECS task (use CloudShell)
aws ecs execute-command \
  --cluster citicore-cluster \
  --task <task-id> \
  --container config-server \
  --interactive \
  --command "/bin/bash"

# Inside container
nc -zv 10.0.1.87 9092
# Should show: Connection successful
```

**Solution:**

1. Verify EC2 security group ingress rule allows port 9092 from ECS SG
2. Verify Kafka container is running: `docker ps | grep kafka`
3. Check Kafka logs: `docker logs kafka`

### RDS Replication Stopped

**Symptoms:**
```
Replication State: Not replicating
Last Error: Duplicate entry
```

**Solution:**

```sql
-- On replica
STOP REPLICA;

-- Fix the issue
DELETE FROM table WHERE ...;

-- Resume
START REPLICA;
SHOW REPLICA STATUS\G
```

If unrecoverable, recreate replica:

```bash
aws rds delete-db-instance \
  --db-instance-identifier citicore-mysql-replica \
  --skip-final-snapshot

# Wait for deletion, then:
aws rds create-db-instance-read-replica \
  --db-instance-identifier citicore-mysql-replica \
  --source-db-instance-identifier citicore-mysql-primary
```

---

## Interview-Ready Decisions

### Decision 1: Why Use VPC with Private Subnets?

**Context:**

Could have launched everything in public subnets with public IPs.

**Decision:**

Use private subnets for ECS, RDS, Kafka, Redis; only ALB in public.

**Why:**

```
Security:
├─ Limits attack surface (only ALB exposed)
├─ No direct internet routes to databases
└─ Isolates services from external access

Compliance:
├─ Banking requires internal-only data tier
├─ RDS best practice: private deployment
└─ reduces risk of credential exposure

Scalability:
├─ Services can scale within private subnets
├─ NAT Gateway enables secure outbound access (if needed)
└─ Easier to add/remove services

Cost:
├─ Initially lower (no NAT Gateway)
└─ Trade-off: future NAT Gateway cost for security
```

**Interview Response:**

"I architected the VPC with public/private subnet separation following AWS Well-Architected Framework. Only the ALB is internet-facing; all application and database services live in private subnets. This reduces the attack surface significantly—compromising the ALB doesn't expose our databases or Kafka. For banking, this separation is essential for compliance. While it adds slight complexity, the security and auditability benefits are worth it."

---

### Decision 2: Why Self-Managed Kafka Instead of MSK?

**Context:**

AWS offers Amazon MSK (Managed Streaming for Kafka).

**Decision:**

Self-managed Kafka on EC2 using Docker.

**Why:**

```
Cost:
├─ MSK: $50+/month
├─ Self-managed: $10/month (EC2 only)
└─ 5x cost difference

Complexity:
├─ MSK: Automatic scaling, patching, monitoring
├─ Self-managed: Manual management
└─ Trade-off acceptable for learning environment

Control:
├─ Self-managed: Full visibility into configuration
├─ Helps understand Kafka internals
└─ MSK abstracts complexity

Production consideration:
└─ Would use MSK for reliability/SLA
```

**Interview Response:**

"For this learning environment, I chose self-managed Kafka on EC2 to minimize costs and maintain full control over configuration. This allowed me to deeply understand Kafka's architecture, topics, partitions, and replication. For production, I'd absolutely use MSK because AWS handles patching, scaling, and failover—banking applications can't afford downtime. But for development, the trade-off was justified."

---

### Decision 3: Why Config Server on ECS Instead of Lambda?

**Context:**

Could have used AWS Lambda for Config Server.

**Decision:**

Run Config Server as a long-lived ECS service.

**Why:**

```
Lambda Limitations:
├─ 15-minute timeout (insufficient for Spring Boot startup)
├─ No persistent connections to Git
├─ Cold start delays for clients
└─ Not suitable for long-lived services

ECS Advantages:
├─ Continuous running process
├─ Spring Cloud Bus maintains Kafka connection
├─ Low latency for client requests
├─ Perfect fit for Spring Cloud Config
└─ Fargate removes operational overhead

Cost Trade-off:
├─ ECS Fargate: $20/month
├─ Lambda (if viable): $5/month
└─ The ECS cost is justified for reliability
```

**Interview Response:**

"Config Server is a long-lived service that maintains persistent connections to Git and Kafka for Spring Cloud Bus. AWS Lambda, with its 15-minute execution limit and cold starts, is fundamentally incompatible with this architecture. I chose ECS with Fargate, which gives me the reliability of a continuous service without managing the underlying EC2 infrastructure. The $20/month Fargate cost is worthwhile for the architectural correctness."

---

### Decision 4: Why GitHub Webhook Over Polling?

**Context:**

Could have Config Server periodically poll GitHub for changes.

**Decision:**

Use GitHub webhooks to immediately notify Config Server.

**Why:**

```
Polling Approach:
├─ Constant API calls (cost)
├─ Latency: 1-5 minutes delay
├─ Wasted bandwidth if no changes
└─ Adds load to GitHub

Webhook Approach:
├─ Event-driven, instantaneous
├─ Only triggers on actual changes
├─ Lower GitHub API cost
└─ Immediate configuration refresh
```

**Interview Response:**

"I used GitHub webhooks because it's the most efficient approach. When a developer pushes configuration changes, GitHub immediately notifies our Config Server via HTTP webhook. The Config Server processes the change, publishes a refresh event to Kafka via Spring Cloud Bus, and connected services receive the update within milliseconds. This is far better than polling every minute and waiting for 60+ seconds of delay. For banking, where configuration changes might include rate limits or feature flags, this near-zero latency is valuable."

---

### Decision 5: Why Kafka Topic 'springCloudBus' Instead of Custom Topic?

**Context:**

Could create topic 'config-refresh' or 'citicore-config-events'.

**Decision:**

Use Spring Cloud Bus default topic: 'springCloudBus'.

**Why:**

```
Spring Cloud Bus Convention:
├─ Well-known by Spring developers
├─ Configuration simpler (uses defaults)
├─ Auto-created by Spring Cloud Bus
└─ Easier onboarding for new team members

Custom Topic Approach:
├─ More descriptive naming
├─ Better organization if many topics
└─ Slight additional complexity

Pragmatism:
└─ No benefit to custom topic for single use case
```

**Interview Response:**

"I kept Spring Cloud Bus's default topic name 'springCloudBus' rather than customizing it to something like 'config-refresh'. In microservices, it's often better to follow conventions than customize every detail. New team members immediately recognize 'springCloudBus' as a Spring Cloud Bus topic. There's no downside to using the default, and it reduces cognitive load. Customization should be reserved for cases where it adds real clarity or solves a specific problem."

---

# CitiCore Eureka Service - Comprehensive Detailed Notes

**Complete guide to building, deploying, and managing Eureka service registry for microservices discovery on AWS ECS Fargate with Spring Cloud integration.**

---

## Table of Contents

1. [Service Discovery Fundamentals](#service-discovery-fundamentals)
2. [Why Eureka for Banking](#why-eureka-for-banking)
3. [Eureka Architecture](#eureka-architecture)
4. [Spring Boot & Spring Cloud Setup](#spring-boot--spring-cloud-setup)
5. [Eureka Application Class](#eureka-application-class)
6. [Eureka Configuration](#eureka-configuration)
7. [Maven Build Process](#maven-build-process)
8. [Dockerization](#dockerization)
9. [Amazon ECR Registry](#amazon-ecr-registry)
10. [ECS Task Definition](#ecs-task-definition)
11. [IAM Roles](#iam-roles)
12. [ECS Cluster & Service](#ecs-cluster--service)
13. [VPC & Networking](#vpc--networking)
14. [Security Groups](#security-groups)
15. [Eureka Health Monitoring](#eureka-health-monitoring)
16. [Client Registration Flow](#client-registration-flow)
17. [Single vs Clustered Eureka](#single-vs-clustered-eureka)
18. [Real Issues & Solutions](#real-issues--solutions)
19. [Interview-Ready Explanations](#interview-ready-explanations)

---

## Service Discovery Fundamentals

### Definition

**Service Discovery** is the mechanism by which microservices automatically locate and communicate with each other without hardcoding network addresses. In a dynamic environment like Kubernetes or AWS ECS, services are constantly being created, destroyed, and replaced.

```
Problem: Static Service Addresses
├─ Auth Service at http://10.0.1.50:8081
├─ When container restarts, gets new IP: 10.0.1.100
├─ Services using old address fail
└─ Manual reconfiguration needed every restart

Solution: Dynamic Service Discovery
├─ Auth Service registers: "I'm at 10.0.1.100:8081"
├─ Service registry tracks current address
├─ Other services query registry
├─ Automatic discovery, no hardcoding
└─ Works across restarts and replacements
```

### What Is a Service Registry

```
Service Registry = Phone Directory for Services

Traditional Phone Directory:
├─ Alice's number: 555-0001
├─ Bob's number: 555-0002
└─ Charlie's number: 555-0003

Service Registry (Eureka):
├─ Auth Service: http://10.0.1.50:8081
├─ Account Service: http://10.0.1.51:8082
├─ User Service: http://10.0.1.52:8083
└─ Updated in real-time as services restart
```

### Why Dynamic Discovery Matters for Banking

```
Without Service Discovery (Manual):
├─ Deploy Auth Service: write 10.0.1.50:8081 in config
├─ Deploy Account Service: write 10.0.1.51:8082 in config
├─ Task crashes, restarts at 10.0.1.100
├─ Update config: 10.0.1.100:8081
├─ Redeploy all services depending on Auth
└─ Manual process, error-prone, slow

With Service Discovery (Automatic):
├─ Auth Service registers with Eureka
├─ Account Service queries Eureka: "Where is Auth?"
├─ Task crashes, restarts at 10.0.1.100
├─ Auth Service re-registers at new IP
├─ Account Service automatically gets new address
└─ Automatic, fast, reliable
```

---

## Why Eureka for Banking

### Definition

**Eureka** is Netflix's open-source service discovery server, now part of Spring Cloud. It maintains a registry of all microservice instances currently running and healthy.

### Problems It Solves for CitiCore

**Problem 1: Container IP Changes**

```
Scenario: Account Service Task Crashes

Current State:
Account Service Container
├─ IP: 10.0.1.50
├─ Port: 8081
└─ Registered with Eureka

Container Crashes and ECS Replaces It:
New Account Service Container
├─ IP: 10.0.1.75 (different!)
├─ Port: 8081 (same)
└─ Re-registers with Eureka

Without Eureka (Problem):
├─ Other services still think Account is at 10.0.1.50
├─ Requests fail
├─ Manual intervention needed

With Eureka (Solution):
├─ Eureka automatically updates: Account now at 10.0.1.75
├─ Other services query Eureka
├─ Automatic discovery of new IP
└─ No manual intervention needed
```

**Problem 2: Multiple Instances**

```
Scenario: Scale Account Service to 3 instances

With Eureka:
Account Service Instances:
├─ Instance 1: 10.0.1.50:8081 → REGISTERED
├─ Instance 2: 10.0.1.51:8081 → REGISTERED
└─ Instance 3: 10.0.1.52:8081 → REGISTERED

Client Query: "Where is Account Service?"
Eureka Response: [10.0.1.50:8081, 10.0.1.51:8081, 10.0.1.52:8081]

Client Load Balance: Use round-robin or random
Result: Requests distributed across all 3 instances
```

**Problem 3: Service Health**

```
Scenario: Instance becomes unhealthy

Eureka Health Check:
├─ Eureka pings each instance: GET /actuator/health
├─ Instance 2 doesn't respond (crashed)
├─ Eureka removes from registry: no longer returns 10.0.1.51

Client Query: "Where is Account Service?"
Eureka Response: [10.0.1.50:8081, 10.0.1.52:8081]

Result: Automatic failover, requests never hit dead instance
```

---

## Eureka Architecture

### What You Implemented

```
CitiCore Microservices Discovery:

              ┌─────────────┐
              │   Eureka    │
              │   Server    │
              │  (Port 8761)│
              └──────┬──────┘
                     │
      ┌──────────────┼──────────────┐
      │              │              │
      ▼              ▼              ▼
   Account        Auth           User
   Service       Service         Service
      │              │              │
      └──────────────┼──────────────┘
                     │
      ┌──────────────┼──────────────┐
      │              │              │
      ▼              ▼              ▼
  Register       Register        Register
  "I'm here"    "I'm here"      "I'm here"
      │              │              │
      └──────────────┴──────────────┘
                     │
                     ▼
              Eureka Registry
              ├─ Account: [...]
              ├─ Auth: [...]
              └─ User: [...]
```

### Deployment Dependencies

```
Account Service needs:
├─ Config Server (get configuration)
├─ Eureka Server (discover other services)
├─ RDS (store data)
├─ Kafka (publish events)
└─ Redis (cache)

Deployment Order:
1. Config Server (services need config)
2. Eureka (services need discovery)
3. Account Service (can now register with Eureka and get config)

Why this order?
├─ If Eureka not running, Account Service fails to register
├─ If Config Server not running, Account can't get configuration
└─ Both must be ready before Account starts
```

---

## Spring Boot & Spring Cloud Setup

### Definition

**Spring Boot** is a framework for building standalone Java applications quickly.  
**Spring Cloud** adds distributed systems capabilities like service discovery, configuration, and circuit breakers.

### Versions Matter

```
Version Alignment is Critical:

Eureka Project Original:
├─ Java: Unknown
├─ Spring Boot: 4.0.5 (doesn't exist, typo?)
├─ Spring Cloud: 2025.1.1 (future version)
└─ Problem: Incompatible with other services

CitiCore Platform Standard:
├─ Java: 17 (LTS version, stable)
├─ Spring Boot: 3.2.4 (stable, widely used)
├─ Spring Cloud: 2023.0.3 (compatible with Boot 3.2.4)
└─ Solution: Align Eureka to platform versions

Why Alignment Matters:
├─ Different Spring Boot versions: different classpath
├─ Different Spring Cloud versions: different APIs
├─ Version mismatch: ClassNotFoundException, NoSuchMethodError
├─ Aligned versions: all services speak same language
└─ Build, test, deploy once, reuse configuration
```

### Step-by-Step Setup

**Step 1: Create Spring Boot Project**

```bash
# Using Spring Initializr or Maven archetype
mvn archetype:generate \
  -DgroupId=com.citicore \
  -DartifactId=eureka-server \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false
```

**Step 2: Update pom.xml**

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.citicore</groupId>
    <artifactId>eureka-server</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.4</version>
        <relativePath/>
    </parent>
    
    <properties>
        <java.version>17</java.version>
        <spring-cloud.version>2023.0.3</spring-cloud.version>
    </properties>
    
    <dependencies>
        <!-- Eureka Server -->
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-netflix-eureka-server</artifactId>
        </dependency>
        
        <!-- Actuator (health checks) -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        
        <!-- Web framework -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>
    
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.cloud</groupId>
                <artifactId>spring-cloud-dependencies</artifactId>
                <version>${spring-cloud.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

**Step 3: Create Application Configuration**

```yaml
# application.yml
server:
  port: 8761

spring:
  application:
    name: eureka-server

eureka:
  client:
    register-with-eureka: false
    fetch-registry: false
```

---

## Eureka Application Class

### Definition

The main Spring Boot application class that enables Eureka Server functionality.

### What You Implemented

```java
package com.citicore.eureka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class EurekaServerApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(EurekaServerApplication.class, args);
    }
}
```

### Why Each Annotation

```
@SpringBootApplication
├─ Enables component scanning
├─ Enables auto-configuration
├─ Enables property support
└─ Shorthand for @Configuration + @EnableAutoConfiguration + @ComponentScan

@EnableEurekaServer
├─ Enables Eureka Server functionality
├─ Registers all Eureka server beans
├─ Starts the service registry
└─ Without it: Just a normal Spring Boot app, not a registry
```

### Real Mistake You Fixed

```
Original (Wrong):
package com.citicore.notification;
class EurekaServerApplication { }

Problem:
├─ Package name: notification (should be eureka)
├─ Confusing for developers
├─ Wrong directory structure
└─ Violates naming conventions

Fixed:
package com.citicore.eureka;
class EurekaServerApplication { }

Benefit:
├─ Clear purpose from package name
├─ Easier to navigate codebase
├─ Follows Java conventions
└─ Self-documenting code
```

---

## Eureka Configuration

### Definition

**Configuration** specifies how Eureka operates: port, clustering, health checks, client behavior.

### What You Implemented

**Single Instance Configuration:**

```yaml
# application.yml (Single Eureka Server, no clustering)

server:
  port: 8761                    # Eureka listens on standard port

spring:
  application:
    name: eureka-server        # Application name in service discovery

eureka:
  client:
    register-with-eureka: false  # Don't register Eureka with itself
    fetch-registry: false        # Don't fetch from other Eurekas (single instance)
  
  server:
    enable-self-preservation: true  # Keep instances if network issues
```

### Why Each Setting

```
server.port: 8761
├─ Standard Eureka port
├─ Netflix convention, widely recognized
├─ Easy to document and remember
└─ Different from app servers (8080, 8081, etc.)

spring.application.name: eureka-server
├─ Name used in logs and dashboards
├─ Shows up in Spring Cloud Bus events
├─ Helps identify service in traces
└─ Must be lowercase, no spaces

register-with-eureka: false
├─ If true: Eureka registers itself as a client
├─ For single server: not needed
├─ For clustering: true (each Eureka is client of others)
├─ Single instance: false (no other Eurekas to register with)
└─ Prevents unnecessary self-registration

fetch-registry: false
├─ If true: Eureka fetches registry from other Eurekas
├─ For single server: not needed (I AM the registry)
├─ For clustering: true (need to sync with other Eurekas)
└─ Single instance: false (no other Eurekas to fetch from)
```

### Single vs Clustered Configuration

```
Single Eureka (Development):
├─ register-with-eureka: false
├─ fetch-registry: false
└─ Only one instance, no replication

Clustered Eureka (Production):
├─ register-with-eureka: true
├─ fetch-registry: true
├─ Each Eureka registers with others
└─ Registry replicated across all instances
```

---

## Maven Build Process

### Definition

**Maven** is a build automation tool that compiles Java code, manages dependencies, runs tests, and packages applications.

### Build Process

**Step 1: Compile Source Code**

```bash
mvn clean
# Removes previous build artifacts

mvn compile
# Compiles Java source files in src/main/java
# Produces class files in target/classes
```

**Step 2: Run Tests**

```bash
mvn test
# Runs tests in src/test/java
# Verifies functionality before packaging
```

**Step 3: Package Application**

```bash
mvn package
# Creates JAR file: target/eureka-server-0.0.1-SNAPSHOT.jar
# Includes compiled classes + dependencies + manifest
# Ready to run with: java -jar eureka-server-0.0.1-SNAPSHOT.jar
```

**Complete Build Command**

```bash
mvn clean package
# All three steps in one command
# Produces executable JAR at target/eureka-server-0.0.1-SNAPSHOT.jar

# Test build output
java -jar target/eureka-server-0.0.1-SNAPSHOT.jar
# Starts Eureka locally on port 8761
```

### Step-by-Step Process

**Step 1: Verify pom.xml**

```bash
# Check dependencies resolve
mvn dependency:tree

# Output shows dependency hierarchy
# Catches version conflicts early
```

**Step 2: Run Tests**

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=EurekaServerApplicationTest

# Skip tests (faster for quick build)
mvn clean package -DskipTests
```

**Step 3: Verify JAR**

```bash
# Check JAR contents
jar tf target/eureka-server-0.0.1-SNAPSHOT.jar | head -20

# Extract and inspect manifest
jar xf target/eureka-server-0.0.1-SNAPSHOT.jar META-INF/MANIFEST.MF
cat META-INF/MANIFEST.MF

# Output shows:
# Main-Class: org.springframework.boot.loader.JarLauncher
# Class-Path: BOOT-INF/classes/ BOOT-INF/lib/...
```

---

## Dockerization

### Definition

**Docker** packages the JAR, JVM, and dependencies into a container—a lightweight, portable, reproducible environment.

### What You Implemented

**Dockerfile:**

```dockerfile
# Dockerfile
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/eureka-server-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8761

ENTRYPOINT ["java", "-jar", "app.jar"]
```

### Why Each Line

```
FROM eclipse-temurin:17-jre
├─ Base image: minimal Java 17 runtime
├─ eclipse-temurin: open-source, well-maintained
├─ jre (not jdk): smaller, no compiler needed
├─ Reduces image size by 200+ MB
└─ Result: ~200 MB image vs 500+ MB with JDK

WORKDIR /app
├─ Sets container working directory
├─ All subsequent commands run from /app
├─ Keeps container filesystem organized
└─ RUN, COPY, ENTRYPOINT reference /app

COPY target/eureka-server-0.0.1-SNAPSHOT.jar app.jar
├─ Copies Maven-built JAR into container
├─ Renames to app.jar (shorter, easier to reference)
├─ Context: current directory (project root)
├─ JAR must exist before docker build (run mvn package first)
└─ If JAR not found: build fails with COPY failed

EXPOSE 8761
├─ Documents that container listens on port 8761
├─ Does NOT actually publish port
├─ Publishing happens with: docker run -p 8761:8761
├─ Helpful for documentation and orchestration
└─ RUN command: maps container 8761 to host port

ENTRYPOINT ["java", "-jar", "app.jar"]
├─ Command to run when container starts
├─ Array form: ["java", "-jar", "app.jar"]
├─ Starts Spring Boot Eureka application
├─ Runs as PID 1 in container
└─ Container stops when JAR process stops
```

### Step-by-Step Docker Build

**Step 1: Build Maven JAR**

```bash
# Must do this first!
mvn clean package

# Verify JAR exists
ls -lh target/eureka-server-0.0.1-SNAPSHOT.jar
# Output: -rw-r--r-- 1 user group 45M eureka-server-0.0.1-SNAPSHOT.jar
```

**Step 2: Build Docker Image**

```bash
# Build image with tag
docker build -t citicore/eureka-server:1.0 .

# Output shows build layers:
# Step 1/5 : FROM eclipse-temurin:17-jre
# Step 2/5 : WORKDIR /app
# ...
# Successfully built abc123def456
# Successfully tagged citicore/eureka-server:1.0
```

**Step 3: Test Locally**

```bash
# Run container locally
docker run -d -p 8761:8761 citicore/eureka-server:1.0

# Map container port 8761 to host port 8761
# -d: run in background
# Container ID: abc123def456

# Check logs
docker logs abc123def456

# Should show:
# Starting EurekaServerApplication
# Started EurekaServerApplication in X seconds
```

**Step 4: Verify Running**

```bash
# Test health endpoint
curl http://localhost:8761/actuator/health

# Output:
# {"status":"UP"}

# Stop container
docker stop abc123def456
```

---

## Amazon ECR Registry

### Definition

**Amazon ECR (Elastic Container Registry)** is AWS's managed Docker image repository. It stores Docker images that ECS can pull and run.

### What You Implemented

**ECR Repository:**

```
Repository: citicore/eureka-server
Account: 580655778303
Region: ap-south-1

Image URI: 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0
           ├─ Account ID: 580655778303 (your AWS account)
           ├─ Region: ap-south-1 (Mumbai region)
           ├─ Repository: citicore/eureka-server
           └─ Tag: 1.0 (version)
```

### Step-by-Step Process

**Step 1: Create ECR Repository**

```bash
# Create repository
aws ecr create-repository \
  --repository-name citicore/eureka-server \
  --region ap-south-1

# Output includes:
# "repositoryUri": "580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server"
```

**Step 2: Authenticate Docker with ECR**

```bash
# Get authorization token
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin \
  580655778303.dkr.ecr.ap-south-1.amazonaws.com

# Output: Login Succeeded
```

**Step 3: Tag Local Image for ECR**

```bash
# Tag with ECR URI
docker tag citicore/eureka-server:1.0 \
  580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0

# Verify
docker images | grep eureka-server
# Shows both local and ECR-tagged images
```

**Step 4: Push to ECR**

```bash
# Push image
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0

# Output shows upload progress:
# Pushing layers...
# Pushed layer abc123...
# Pushed image digest sha256:def456...

# Verify in ECR
aws ecr list-images --repository-name citicore/eureka-server
```

---

## ECS Task Definition

### Definition

**ECS Task Definition** is a blueprint describing how Eureka container should run: image, CPU, memory, ports, environment variables, logging.

### What You Implemented

```yaml
# Task Definition: citicore-eureka-server:1

Name: citicore-eureka-server
Revision: 1

Compute:
  Launch Type: FARGATE
  Operating System: Linux
  Architecture: X86_64
  CPU: 0.5 vCPU
  Memory: 1 GB
  Network Mode: awsvpc

Container:
  Name: eureka-server
  Image: 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0
  Port Mapping:
    Container Port: 8761
    Protocol: TCP
  
  Logging:
    Log Driver: awslogs
    Log Group: /ecs/citicore-eureka-server
    Region: ap-south-1
  
  Health Check:
    Command: ["CMD-SHELL", "curl -f http://localhost:8761/actuator/health || exit 1"]
    Interval: 30 seconds
    Timeout: 5 seconds
    Retries: 3
    Start Period: 60 seconds
```

### Why These Values

```
CPU: 0.5 vCPU (0.25 to 4.0 range)
├─ Eureka is lightweight
├─ Single instance sufficient
├─ Not CPU-intensive (mostly network I/O)
└─ 0.5 vCPU provides enough compute

Memory: 1 GB (512 MB to 30 GB range)
├─ JVM base: ~100 MB
├─ Spring Cloud libraries: ~200 MB
├─ Registry data in memory: ~100 MB
├─ Safety margin: ~500 MB
└─ Total: 1 GB sufficient

Network Mode: awsvpc
├─ Each task gets its own ENI (network interface)
├─ Task gets private IP in VPC
├─ Required for Service Connect
├─ Enables DNS service discovery
└─ Best practice for modern deployments

Health Check:
├─ /actuator/health endpoint
├─ Curl returns JSON with status
├─ If fails 3 times in 30 seconds: mark unhealthy
├─ Start period: wait 60s before first check
└─ Gives Spring Boot time to start
```

---

## IAM Roles

### Definition

**IAM Roles** define permissions for ECS tasks: what AWS services they can access.

### Task Role vs Execution Role

```
Task Execution Role (citicore-ecs-task-execution-role):
├─ Used BY: ECS/Fargate infrastructure
├─ Permissions:
│  ├─ ecr:GetDownloadUrlForLayer
│  ├─ ecr:BatchGetImage
│  ├─ ecr:GetAuthorizationToken
│  ├─ logs:CreateLogStream
│  └─ logs:PutLogEvents
├─ Purpose: ECS operations
└─ When: Container startup, logging

Task Role (citicore-ecs-task-role):
├─ Used BY: Application code inside container
├─ Permissions:
│  ├─ cloudwatch:PutMetricData (if publishing metrics)
│  ├─ s3:GetObject (if accessing S3)
│  └─ dynamodb:Query (if querying DynamoDB)
├─ Purpose: Application operations
└─ When: Application needs AWS APIs
```

### Why Separate Roles

```
Security Principle: Least Privilege

If single role for both:
├─ Application gets execution permissions (pull ECR, write logs)
├─ If application compromised, attacker can:
│  ├─ Pull ECR images (get other services' code)
│  ├─ Overwrite logs (hide attack traces)
│  └─ Access CloudWatch (read other apps' metrics)
└─ Too much damage

With separate roles:
├─ Application only gets app permissions
├─ If application compromised, attacker can only:
│  └─ Do what app needs to do (limited damage)
├─ ECS operations require separate role
└─ Minimum necessary permissions each
```

---

## ECS Cluster & Service

### Definition

**ECS Cluster** is a logical grouping of resources.  
**ECS Service** maintains desired number of running tasks.

### What You Implemented

```
ECS Cluster: citicore-cluster
├─ Launch Type: FARGATE
├─ Region: ap-south-1
└─ Services running:
   ├─ config-server
   ├─ citicore-eureka-server
   └─ (more services to come)

ECS Service: citicore-eureka-server
├─ Task Definition: citicore-eureka-server:1
├─ Desired Count: 1
├─ Running Count: 1
├─ Pending Count: 0
├─ Deployment Status: Success
└─ Health: Healthy
```

### Task Lifecycle

```
Service monitors health:

1. Container starts
   ├─ ECS launches container from task definition
   ├─ Application starts
   └─ Logs: "Started EurekaServerApplication"

2. Health check running
   ├─ ECS pings /actuator/health every 30 seconds
   ├─ Success: {"status":"UP"}
   └─ Count successes

3. After 2 successes
   ├─ Task marked HEALTHY
   └─ Traffic directed to task

4. Container healthy
   ├─ Receives requests
   └─ Service running

5. If container crashes
   ├─ Health checks fail
   ├─ ECS detects failure
   ├─ Automatically launches replacement
   └─ Service maintains "1 running"
```

---

## VPC & Networking

### Definition

**VPC (Virtual Private Cloud)** is your isolated network in AWS. **Subnets** divide VPC into smaller networks.

### What You Implemented

```
VPC: citicore-vpc (vpc-0d064f45265cbcdad)
CIDR: 10.0.0.0/16

Subnets:
├─ Public-A (10.0.1.0/24): ALB, internet access
├─ Public-B (10.0.2.0/24): ALB backup
├─ Private-A (10.0.11.0/24): Eureka ECS tasks
├─ Private-B (10.0.12.0/24): Eureka backup
├─ DB-A (10.0.21.0/24): RDS primary
└─ DB-B (10.0.22.0/24): RDS replica

Eureka Placement:
├─ Launch in Private-A and Private-B
├─ No public IP needed initially
├─ Internal service (used by other ECS services)
└─ Later: temporary public IP for testing
```

### Why Private Subnets for Eureka

```
Eureka is Internal Service:
├─ Used BY: Account, Auth, User services
├─ NOT used BY: Internet/external clients
├─ Should NOT be directly accessible from internet
├─ Should NOT listen to external traffic

Benefits of Private Placement:
├─ Enhanced security (not exposed to internet)
├─ No need for Internet Gateway
├─ Reduced attack surface
├─ Only other ECS services can access
└─ No public ALB needed
```

---

## Security Groups

### Definition

**Security Group** is a virtual firewall controlling inbound/outbound traffic for resources.

### What You Implemented

**Eureka Security Group (citicore-ecs-sg):**

```yaml
Inbound Rules:

Rule 1 (Eureka):
  Protocol: TCP
  Port: 8761
  Source: citicore-ecs-sg (same security group)
  Description: "Services in same SG can access Eureka"

Result:
├─ Account Service (same SG): Can access 8761 ✓
├─ Auth Service (same SG): Can access 8761 ✓
├─ External IP: Cannot access 8761 ✗
└─ Internet: Cannot access 8761 ✗
```

### Real Mistake You Avoided

```
Temporary Testing Rule (During Verification):

Rule Added:
  Protocol: TCP
  Port: 8761
  Source: 0.0.0.0/0 (ANY IP)
  Status: Allows internet access

Testing:
  curl.exe http://65.2.74.91:8761/actuator/health
  Result: {"status":"UP"} ✓

After Testing:
  Removed rule: 0.0.0.0/0
  Reason: Don't leave Eureka open to internet
  
Final State:
├─ Only citicore-ecs-sg can access
├─ Eureka remains internal service
└─ No direct internet exposure
```

### Why Removing the Rule Matters

```
Leaving 0.0.0.0/0 Open:
├─ Eureka accessible from anywhere
├─ Attackers can:
│  ├─ Enumerate all registered services
│  ├─ Find internal service locations
│  ├─ Launch targeted attacks
│  └─ Map your entire architecture
└─ Security risk

Final State (Closed):
├─ Only internal services can access
├─ External attackers can't reach
├─ Service registry hidden
└─ Harder to map architecture
```

---

## Eureka Health Monitoring

### Definition

**Health Monitoring** checks if Eureka application is running and responding correctly.

### Actuator Endpoint

```
Spring Boot Actuator provides monitoring endpoints:

GET /actuator/health
├─ Returns: {"status":"UP"} or {"status":"DOWN"}
├─ Used by: ECS health checks, ALB health checks
├─ Called every: 30 seconds (configurable)
└─ Purpose: Verify Eureka is operational
```

### Testing Eureka Health

**Local Testing:**

```bash
# Start Eureka locally
java -jar target/eureka-server-0.0.1-SNAPSHOT.jar

# Test health endpoint
curl http://localhost:8761/actuator/health

# Response:
# {"status":"UP"}

# Test Eureka dashboard
curl http://localhost:8761/

# Response: HTML with Eureka UI
```

**Remote Testing (AWS):**

```bash
# Get Eureka task public IP (temporary)
aws ecs describe-tasks \
  --cluster citicore-cluster \
  --tasks arn:aws:ecs:... \
  --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`]'

# Query health from Eureka IP
curl http://10.0.11.100:8761/actuator/health

# Response:
# {"status":"UP"}
```

---

## Client Registration Flow

### Definition

**Client Registration** is how services (Account, Auth, User) register themselves with Eureka so other services can discover them.

### Registration Process

```
Spring Boot Service with Eureka Client:

1. Application starts
   ├─ Reads configuration
   ├─ Finds Eureka URL: http://eureka-server-8761-tcp.citicore:8761/eureka
   └─ Prepares to register

2. Sends registration request
   POST /eureka/apps/ACCOUNT-SERVICE
   Body:
   {
     "instance": {
       "instanceId": "account-service",
       "hostName": "10.0.1.50",
       "app": "ACCOUNT-SERVICE",
       "ipAddr": "10.0.1.50",
       "port": {"$": 8081, "@enabled": true},
       "status": "UP"
     }
   }

3. Eureka adds to registry
   ├─ Stores instance info
   ├─ Marks as UP
   ├─ Schedules health check
   └─ Responds: HTTP 201 Created

4. Client sends heartbeats
   ├─ Every 30 seconds
   ├─ PUT /eureka/apps/ACCOUNT-SERVICE/instance-id
   └─ Eureka updates last-heartbeat timestamp

5. If heartbeat fails
   ├─ Eureka waits 90 seconds
   ├─ If 3+ missed heartbeats
   ├─ Removes instance from registry
   └─ Other services stop routing to it
```

### Client Configuration

```yaml
# Application.yml for Account Service

spring:
  application:
    name: account-service

eureka:
  client:
    register-with-eureka: true         # Register self
    fetch-registry: true               # Get list of other services
    service-url:
      defaultZone: http://eureka-server-8761-tcp.citicore:8761/eureka
    instance-info-replication-interval-seconds: 30
    
  instance:
    instance-id: account-service
    prefer-ip-address: false           # Use hostname, not IP
    lease-renewal-interval-in-seconds: 30
    lease-expiration-duration-in-seconds: 90
```

---

## Single vs Clustered Eureka

### Single Instance (Development)

```
Configuration:
├─ register-with-eureka: false
├─ fetch-registry: false
└─ Single server, no clustering

Pros:
├─ Simple to set up
├─ Low resource usage
├─ Sufficient for development
└─ One instance to manage

Cons:
├─ Single point of failure
├─ If Eureka down, services can't register
├─ No redundancy
└─ Not production-ready
```

### Clustered Eureka (Production)

```
Configuration:
├─ eureka1: register-with-eureka: true
│           service-url: http://eureka2, http://eureka3
├─ eureka2: register-with-eureka: true
│           service-url: http://eureka1, http://eureka3
└─ eureka3: register-with-eureka: true
            service-url: http://eureka1, http://eureka2

Architecture:
eureka1 ← → eureka2
  ↓       ↗
  └─ eureka3

Registry Replication:
├─ When service registers with eureka1
├─ eureka1 replicates to eureka2, eureka3
├─ All three have same registry
└─ If eureka1 down, eureka2/3 still have data

Pros:
├─ High availability (1 can fail, 2 still running)
├─ Load distribution
├─ Registry redundancy
└─ Production-ready

Cons:
├─ More complex configuration
├─ Higher resource usage (3+ instances)
├─ Network complexity (inter-eureka communication)
└─ Requires careful monitoring
```

### Migration Path

```
Development → Production Migration:

Current (Development):
├─ Single Eureka instance
├─ account-service registers
├─ works fine for 1-2 services
└─ acceptable for testing

When Ready for Production:
1. Keep eureka1 running
2. Deploy eureka2, eureka3 in parallel
3. Point all three to each other
4. Services re-register (now visible to all 3)
5. Remove single instance
6. Now have cluster of 3

No downtime for services:
├─ They keep heartbeating
├─ New instances replicate during startup
├─ Services find all 3 Eureka instances
└─ Automatic failover works
```

---

## Real Issues & Solutions

### Issue #1: Wrong Spring Boot/Cloud Versions

**Problem:**

```
Original Eureka project:
├─ Spring Boot: 4.0.5 (doesn't exist!)
├─ Spring Cloud: 2025.1.1 (future)
└─ Result: Build fails, incompatible with platform

Platform standard:
├─ Java 17
├─ Spring Boot 3.2.4
├─ Spring Cloud 2023.0.3
```

**Solution:**

```bash
# Update pom.xml to platform versions
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>3.2.4</version>
</parent>

<properties>
  <spring-cloud.version>2023.0.3</spring-cloud.version>
</properties>

# Test build
mvn clean package

# Result: Successful build, compatible with all other services
```

### Issue #2: Package Name Typo

**Problem:**

```
Original:
package com.citicore.notification;
class EurekaServerApplication { }

Issues:
├─ Package: notification (wrong, should be eureka)
├─ Confusing for developers
├─ Self-documenting code broken
└─ Wrong directory structure
```

**Solution:**

```bash
# Move file to correct package
mv src/main/java/com/citicore/notification/EurekaServerApplication.java \
   src/main/java/com/citicore/eureka/EurekaServerApplication.java

# Update package declaration
sed -i 's/package com.citicore.notification;/package com.citicore.eureka;/' \
    src/main/java/com/citicore/eureka/EurekaServerApplication.java

# Verify
grep "^package" src/main/java/com/citicore/eureka/EurekaServerApplication.java
# Output: package com.citicore.eureka;
```

### Issue #3: Application Name Typo

**Problem:**

```yaml
# Original application.yml
spring:
  application:
    name: eurka-server  # TYPO: missing 'e'

Issues:
├─ Services look for: eureka-server
├─ Find instead: eurka-server
├─ Service discovery fails
└─ Services can't find Eureka
```

**Solution:**

```yaml
# Fixed application.yml
spring:
  application:
    name: eureka-server  # Correct spelling

# Verify in logs:
# Started EurekaServerApplication in X.XXX seconds
# Shows application name correctly registered
```

### Issue #4: Testing Complicates Security

**Problem:**

```
During testing, added temporary rule:
├─ Protocol: TCP
├─ Port: 8761
├─ Source: 0.0.0.0/0 (ANY)
├─ Purpose: curl from laptop
└─ Risk: Left it open!

If forgotten:
├─ Eureka accessible from internet
├─ Attackers can enumerate services
├─ Internal architecture exposed
└─ Security breach
```

**Solution:**

```bash
# After verification testing, remove rule
aws ec2 revoke-security-group-ingress \
  --group-id sg-xxxxxxxx \
  --protocol tcp \
  --port 8761 \
  --cidr 0.0.0.0/0

# Verify only internal access remains
aws ec2 describe-security-groups \
  --group-ids sg-xxxxxxxx \
  --query 'SecurityGroups[0].IpPermissions'

# Shows only citicore-ecs-sg as source
```

---

## Interview-Ready Explanations

### "Explain Service Discovery and Why Eureka"

**Response:**

"Service discovery solves a fundamental problem in microservices architecture: how services find each other when IP addresses change constantly.

In a traditional monolith, you hardcode database connections: 'database at 192.168.1.10.' That works because hardware is stable.

In microservices with containers, tasks start and stop constantly. An Account Service task at 10.0.1.50 might crash and restart at 10.0.1.100. If other services hardcoded the old IP, they now can't reach Account Service.

Eureka solves this with dynamic registration:

1. Account Service starts: 'I'm at 10.0.1.100:8081'
2. Eureka stores: Account Service → 10.0.1.100:8081
3. Auth Service needs Account: queries Eureka
4. Eureka responds: 'Account is at 10.0.1.100'
5. Auth connects to current address

If Account crashes and restarts:
1. New Account at 10.0.1.150
2. Re-registers with Eureka
3. Eureka updates registry
4. Auth automatically gets new address
5. Seamless failover

For banking, this is critical. Services need to communicate reliably despite constant deployment, scaling, and failures. Eureka makes this automatic and transparent."

### "How would you scale Eureka to production?"

**Response:**

"Currently, I have a single Eureka instance sufficient for development. For production, I'd deploy a Eureka cluster:

**Three-instance cluster** (recommended):

```
Eureka-1 (Primary)     Eureka-2 (Replica)     Eureka-3 (Replica)
  │                      │                      │
  ├─ register-with-eureka: true
  ├─ service-url: eureka-2, eureka-3
  └─ All three synchronized

Registry replication:
├─ When Account registers with Eureka-1
├─ Eureka-1 replicates to Eureka-2 and Eureka-3
├─ All three maintain same registry
└─ If Eureka-1 fails, Eureka-2/3 still have data
```

**Why three?**

```
One failure → 2/3 operating (66% uptime)
Two failures → 1/3 operating (33% uptime, degraded)
Three failures → 0/3 operating (unavailable, but rare)

If one Eureka fails:
├─ Services continue using other two
├─ Registry still available
├─ No service outage
└─ Replicate failed instance back into cluster
```

**Deployment changes:**

```
Current: Account Service queries one Eureka
Future: Account Service queries all three Eureka URLs

spring:
  eureka:
    client:
      service-url:
        defaultZone: http://eureka-1:8761/eureka,http://eureka-2:8761/eureka,http://eureka-3:8761/eureka

Result: Client-side resilience
├─ If eureka-1 unavailable, try eureka-2
├─ Automatic failover at client level
└─ No single point of failure
```

**Monitoring additions:**

```
Per-Eureka monitoring:
├─ Instance count registered
├─ Registration failures
├─ Replication lag between instances
├─ Heartbeat failures
└─ Deregistrations

Alerts:
├─ If any Eureka instance down
├─ If registration rate drops
├─ If replication lag > 30 seconds
└─ If >5% services unregistered
```

This gives us high availability for the service registry itself, preventing Eureka from becoming a bottleneck or single point of failure."

### "Walk me through the complete deployment"

**Response:**

"The Eureka deployment follows these stages:

**Stage 1: Development**
- Write Eureka Spring Boot code
- Maven builds JAR
- Test locally on port 8761

**Stage 2: Containerization**
- Create Dockerfile (JRE 17, copy JAR, expose 8761)
- Build Docker image locally: citicore/eureka-server:1.0
- Test container locally: curl localhost:8761/actuator/health

**Stage 3: AWS Preparation**
- Create ECR repository
- Authenticate Docker with ECR
- Tag local image with ECR URI
- Push to ECR (image stored in AWS)

**Stage 4: ECS Configuration**
- Create task definition (CPU 0.5, Memory 1GB, image URI, port 8761)
- Create IAM roles (task execution role for ECR/logging)
- Create security group rule (TCP 8761 from same security group)

**Stage 5: Service Deployment**
- Create ECS service
- Point to task definition
- Specify desired count: 1
- Select VPC and subnets (private subnets)
- Assign security group

**Stage 6: Verification**
- ECS launches container from ECR image
- Spring Boot starts, logs: 'Started EurekaServerApplication'
- Health check: curl /actuator/health returns UP
- Service shows: 1 desired, 1 running, 0 pending

**Result:**
- Eureka running at http://eureka-server-8761-tcp.citicore:8761
- Ready for services to register
- Next: Deploy Account Service with Eureka client

This process demonstrates: local development → Docker → AWS ECR → ECS Fargate. Each stage is isolated, so failures don't cascade."

---

## Key Takeaways

```
✅ Service Discovery
   Dynamic resolution of service locations
   Critical for container-based architectures

✅ Eureka Architecture
   Central registry for all service instances
   Handles registration, discovery, and health

✅ Deployment Process
   Local → Docker → ECR → ECS Fargate
   Each stage validates before moving to next

✅ Single vs Cluster
   Single for dev, cluster for production
   Cluster provides high availability

✅ Security Layers
   VPC isolation, security groups, IAM roles
   Layered defense strategy

✅ Monitoring & Health
   /actuator/health endpoint
   ECS health checks every 30 seconds
   Automatic recovery on failure
```

---

## Deployment Checklist

```
Pre-Deployment:
☐ Java 17 installed
☐ Maven project structure correct
☐ pom.xml versions aligned (Boot 3.2.4, Cloud 2023.0.3)
☐ Application.yml configured correctly
☐ @EnableEurekaServer annotation present
☐ LocalDockerTesting.md passed locally

Docker Build:
☐ mvn clean package successful
☐ JAR file created in target/
☐ Docker build successful
☐ docker run test passes
☐ /actuator/health returns UP

AWS Preparation:
☐ ECR repository created
☐ Docker authenticated with ECR
☐ Image tagged correctly
☐ Image pushed to ECR
☐ Image visible in ECR console

ECS Configuration:
☐ Task definition created
☐ IAM roles attached
☐ Security group rules defined
☐ VPC and subnets selected

Deployment:
☐ ECS service created
☐ 1 task desired
☐ Task reaches RUNNING state
☐ Health check shows UP
☐ Logs show: Started EurekaServerApplication

Verification:
☐ Test health endpoint
☐ List registered applications (none yet, will add Account Service)
☐ Monitoring dashboards configured
☐ Ready for Account Service deployment
```

---
# CitiCore Banking Platform - AWS Deployment: Comprehensive Detailed Notes

**Complete guide to deploying a microservices banking application on AWS ECS Fargate with Spring Cloud, Eureka service discovery, Kafka event streaming, and ALB load balancing.**

---

## Table of Contents

1. [Deployment Order Strategy](#deployment-order-strategy)
2. [Amazon ECR (Elastic Container Registry)](#amazon-ecr-elastic-container-registry)
3. [Amazon ECS and Fargate](#amazon-ecs-and-fargate)
4. [ECS Task Definition](#ecs-task-definition)
5. [ECS Service](#ecs-service)
6. [VPC and Networking](#vpc-and-networking)
7. [IAM Roles](#iam-roles)
8. [Spring Cloud Config Server](#spring-cloud-config-server)
9. [Eureka Service Discovery](#eureka-service-discovery)
10. [ECS Service Connect](#ecs-service-connect)
11. [Application Load Balancer (ALB)](#application-load-balancer-alb)
12. [Auth Service Deployment](#auth-service-deployment)
13. [Real Issues & Troubleshooting](#real-issues--troubleshooting)
14. [Kafka Event Streaming](#kafka-event-streaming)
15. [Key Lessons Learned](#key-lessons-learned)

---

## Deployment Order Strategy

### Definition

**Deployment Order** refers to the sequence in which infrastructure components and microservices are deployed in a distributed system. It ensures that dependent services are not deployed before their dependencies are ready.

### What You Implemented

```
AWS Infrastructure (VPC, RDS, Security Groups, Subnets)
        ↓
Config Server (Centralized configuration management)
        ↓
Eureka Server (Service discovery registry)
        ↓
Auth Service (Authentication microservice)
        ↓
User Service (User management)
        ↓
Account Service (Account operations)
        ↓
Transaction Service (Transaction processing)
        ↓
Notification Service (Email/SMS notifications)
        ↓
API Gateway (External request entry point)
```

### Why This Approach

**Without proper deployment order:**

```
Problem: Deploy Auth Service first
    ↓
Auth Service tries to register with Eureka
    ↓
Eureka not running yet
    ↓
Auth Service startup fails
    ↓
Application hangs or crashes
```

**With proper deployment order:**

```
1. Deploy infrastructure (VPC ready)
2. Deploy Config Server (configuration available)
3. Deploy Eureka (service registry ready)
4. Deploy Auth Service (can register with Eureka)
5. Deploy other services (can discover each other)
```

### Step-by-Step Process

**Step 1: Prepare AWS Infrastructure**

```bash
# Create VPC
aws ec2 create-vpc --cidr-block 10.0.0.0/16 --region ap-south-1

# Create subnets
aws ec2 create-subnet --vpc-id vpc-xxx --cidr-block 10.0.1.0/24 --region ap-south-1

# Create security groups
aws ec2 create-security-group --group-name citicore-ecs-sg --vpc-id vpc-xxx --region ap-south-1
```

**Step 2: Deploy Config Server**

```bash
# Build Docker image
docker build -t citicore/config-server:1.0 .

# Push to ECR
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 580655778303.dkr.ecr.ap-south-1.amazonaws.com
docker tag citicore/config-server:1.0 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0

# Create ECS task definition
aws ecs register-task-definition --cli-input-json file://config-server-task-def.json

# Create ECS service
aws ecs create-service --cluster citicore-cluster --service-name config-server --task-definition config-server:1 --desired-count 1
```

**Step 3: Deploy Eureka Server**

```bash
# Build Docker image
docker build -t citicore/eureka-server:1.0 .

# Push to ECR
docker tag citicore/eureka-server:1.0 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0

# Create ECS task definition
aws ecs register-task-definition --cli-input-json file://eureka-task-def.json

# Create ECS service
aws ecs create-service --cluster citicore-cluster --service-name eureka-server --task-definition eureka-server:1 --desired-count 1
```

**Step 4: Deploy Auth Service (and other services)**

```bash
# Same process as above - build, push, create task def, create service
# Auth can now safely register with Eureka (already running)
```

### Interview Explanation

"I implemented intentional deployment order because microservices have dependencies. If you deploy Auth Service before Eureka, it fails because it can't find the service registry. The correct order is: infrastructure first, then central services (Config Server, Eureka), then application services. This mirrors how we'd start a real office: you prepare the office first, then hire the manager, then hire employees. The employees (microservices) need the office (infrastructure) and manager (Eureka) to function."

---

## Amazon ECR (Elastic Container Registry)

### Definition

**Amazon Elastic Container Registry (ECR)** is AWS's managed Docker container image registry. It stores Docker images that can be pulled and run by ECS (container orchestration service).

Think of it as a library for Docker images:

```
Local Docker Image
        ↓
Amazon ECR (Image Storage)
        ↓
ECS retrieves image
        ↓
Fargate runs container
```

### What You Implemented

**ECR Setup for CitiCore:**

```
AWS Account: 580655778303
Region: ap-south-1
Repositories:

1. citicore/config-server:1.0
   URI: 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0

2. citicore/eureka-server:1.0
   URI: 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0

3. citicore/auth-service:1.0
   URI: 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/auth-service:1.0
```

### Why Use ECR

**Problems it solves:**

```
Problem 1: Image Versioning
├─ Without ECR: Images scattered on laptops
├─ With ECR: Central registry, version control
└─ Benefit: Reproducible deployments

Problem 2: Image Distribution
├─ Without ECR: Manual copying to AWS
├─ With ECR: Pull from within AWS (faster, secure)
└─ Benefit: Fast image pull, private registry

Problem 3: Image Security
├─ Without ECR: Images on Docker Hub (public)
├─ With ECR: Private registry in your AWS account
└─ Benefit: Only authorized users access images

Problem 4: Integration with ECS
├─ Without ECR: Configure credentials manually
├─ With ECR: ECS assumes role, automatic access
└─ Benefit: Seamless integration
```

### Step-by-Step Process

**Step 1: Create ECR Repository**

```bash
# Create repository
aws ecr create-repository \
  --repository-name citicore/auth-service \
  --region ap-south-1

# Output:
# {
#   "repository": {
#     "repositoryUri": "580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/auth-service"
#   }
# }
```

**Step 2: Authenticate Docker with ECR**

```bash
# Get authorization token
aws ecr get-login-password --region ap-south-1

# Login Docker CLI
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin \
  580655778303.dkr.ecr.ap-south-1.amazonaws.com

# Output: Login Succeeded
```

**Step 3: Build Docker Image**

```bash
# Navigate to project directory
cd ~/citicore-platform/auth-service

# Build image
docker build -t citicore/auth-service:1.0 .

# Verify build
docker images | grep auth-service
# REPOSITORY                    TAG      IMAGE ID
# citicore/auth-service         1.0      abc123def456
```

**Step 4: Tag Image for ECR**

```bash
# Tag image with ECR URI
docker tag citicore/auth-service:1.0 \
  580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/auth-service:1.0
```

**Step 5: Push to ECR**

```bash
# Push image to ECR
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/auth-service:1.0

# Output shows push progress:
# Pushing [====>                                        ] 10%
# Pushing [=========>                                   ] 20%
# ...
# Pushed image successfully
```

**Step 6: Verify Image in ECR**

```bash
# List images in repository
aws ecr list-images \
  --repository-name citicore/auth-service \
  --region ap-south-1

# Output:
# {
#   "imageIds": [
#     {
#       "imageTag": "1.0",
#       "imageDigest": "sha256:abc123..."
#     }
#   ]
# }

# Get image details
aws ecr describe-images \
  --repository-name citicore/auth-service \
  --image-ids imageTag=1.0 \
  --region ap-south-1
```

### Interview Explanation

"ECR is AWS's private container registry. Instead of storing Docker images locally or on public Docker Hub, I use ECR to store images within AWS. This provides security (private to our account), versioning (different versions of images), and integration with ECS (ECS can pull images directly). The workflow is: build image locally, authenticate with ECR, tag image with ECR URI, push to ECR, then ECS pulls from ECR when launching tasks. It's similar to GitHub for code, but for Docker images."

---

## Amazon ECS and Fargate

### Definition

**Amazon ECS (Elastic Container Service)** is AWS's managed container orchestration service. It runs and manages Docker containers at scale.

**AWS Fargate** is the serverless compute engine used by ECS. Instead of managing EC2 instances yourself, Fargate handles the underlying infrastructure.

```
Traditional Approach:
├─ Manage EC2 servers
├─ Install Docker
├─ Manage scaling
└─ High operational overhead

Fargate Approach:
├─ Just deploy containers
├─ Fargate manages servers
├─ Auto-scaling available
└─ Low operational overhead
```

### What You Implemented

**ECS Cluster:**

```
citicore-cluster
├─ Launch Type: Fargate (serverless)
├─ Region: ap-south-1
├─ Services running:
│  ├─ config-server (CPU: 0.5, Memory: 1GB)
│  ├─ eureka-server (CPU: 0.5, Memory: 1GB)
│  └─ auth-service (CPU: 1, Memory: 3GB)
└─ Total resources managed: Automatically by Fargate
```

### Why Use ECS with Fargate

**Advantages:**

```
1. NO SERVER MANAGEMENT
   ├─ You specify containers
   ├─ AWS manages servers
   └─ Focus on application, not infrastructure

2. AUTOMATIC SCALING
   ├─ Define desired task count
   ├─ Add more tasks as needed
   └─ Fargate handles capacity

3. COST EFFICIENCY
   ├─ Pay only for resources used
   ├─ No idle server costs
   └─ Better than managing EC2

4. BUILT-IN MONITORING
   ├─ CloudWatch integration
   ├─ Health checks automatic
   └─ Easy troubleshooting

5. LOAD BALANCING
   ├─ ALB integration built-in
   ├─ Auto-distribution of traffic
   └─ High availability
```

### Step-by-Step Process

**Step 1: Create ECS Cluster**

```bash
# Create cluster (Fargate-compatible)
aws ecs create-cluster \
  --cluster-name citicore-cluster \
  --capacity-providers FARGATE FARGATE_SPOT \
  --region ap-south-1

# Output:
# {
#   "cluster": {
#     "clusterName": "citicore-cluster",
#     "status": "ACTIVE"
#   }
# }
```

**Step 2: Create Task Definition**

A task definition is a blueprint describing how containers should run.

```bash
# Create task definition file: task-definition.json
cat > task-definition.json << 'EOF'
{
  "family": "citicore-auth-service",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "3072",
  "executionRoleArn": "arn:aws:iam::580655778303:role/citicore-ecs-task-execution-role",
  "taskRoleArn": "arn:aws:iam::580655778303:role/citicore-ecs-task-role",
  "containerDefinitions": [
    {
      "name": "auth-service",
      "image": "580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/auth-service:1.0",
      "portMappings": [
        {
          "containerPort": 8081,
          "hostPort": 8081,
          "protocol": "tcp"
        }
      ],
      "essential": true,
      "environment": [
        {
          "name": "SPRING_CLOUD_CONFIG_URI",
          "value": "http://config-server:8888"
        },
        {
          "name": "EUREKA_CLIENT_SERVICEURL_DEFAULTZONE",
          "value": "http://eureka-server-8761-tcp.citicore:8761/eureka"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/citicore-auth-service",
          "awslogs-region": "ap-south-1",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -f http://localhost:8081/actuator/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3,
        "startPeriod": 60
      }
    }
  ]
}
EOF

# Register task definition
aws ecs register-task-definition \
  --cli-input-json file://task-definition.json \
  --region ap-south-1

# Output shows task definition registered with revision number
```

**Step 3: Create ECS Service**

```bash
# Create service (maintains desired number of running tasks)
aws ecs create-service \
  --cluster citicore-cluster \
  --service-name citicore-auth-service \
  --task-definition citicore-auth-service:1 \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={
    subnets=[subnet-private-a,subnet-private-b],
    securityGroups=[sg-ecs-id],
    assignPublicIp=DISABLED
  }" \
  --load-balancers targetGroupArn=arn:aws:elasticloadbalancing:...,containerName=auth-service,containerPort=8081 \
  --region ap-south-1

# Output shows service created
```

**Step 4: Verify Task is Running**

```bash
# List tasks in service
aws ecs list-tasks \
  --cluster citicore-cluster \
  --service-name citicore-auth-service \
  --region ap-south-1

# Describe task status
aws ecs describe-tasks \
  --cluster citicore-cluster \
  --tasks arn:aws:ecs:ap-south-1:580655778303:task/citicore-cluster/abc123 \
  --region ap-south-1

# Check status
# lastStatus: RUNNING (wait for this)
# desiredStatus: RUNNING
# healthStatus: HEALTHY
```

**Step 5: Monitor Task Logs**

```bash
# View logs in CloudWatch
aws logs tail /ecs/citicore-auth-service --follow --region ap-south-1

# Or view in AWS Console
# CloudWatch → Log Groups → /ecs/citicore-auth-service
```

### Interview Explanation

"ECS with Fargate is the modern way to run containers on AWS. Instead of managing EC2 servers, I define how containers should run (CPU, memory, environment variables, ports), and Fargate handles the servers automatically. The workflow is: create task definition (blueprint for container), create service (tells ECS to run X tasks), and ECS/Fargate handles launching, monitoring, and replacing failed tasks. It's like telling a taxi service 'I need 5 taxis available' instead of buying taxis and managing drivers yourself."

---

## ECS Task Definition

### Definition

**ECS Task Definition** is a JSON blueprint that describes:
- Which Docker image to use
- How much CPU and memory to allocate
- Environment variables and secrets
- Port mappings
- Logging configuration
- Health checks
- IAM roles

Think of it as a detailed specification for how a container should run.

### What You Implemented

**Task Definition for Auth Service:**

```json
{
  "family": "citicore-auth-service",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",           // 1 vCPU
  "memory": "3072",        // 3 GB RAM
  "containerDefinitions": [
    {
      "name": "auth-service",
      "image": "580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/auth-service:1.0",
      "portMappings": [
        {
          "containerPort": 8081,  // Container listens on 8081
          "hostPort": 8081
        }
      ],
      "environment": [
        {
          "name": "SPRING_CLOUD_CONFIG_URI",
          "value": "http://config-server:8888"  // Config Server location
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/citicore-auth-service",
          "awslogs-region": "ap-south-1"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -f http://localhost:8081/actuator/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "startPeriod": 60    // Give app 60 seconds to start
      }
    }
  ]
}
```

### Why This Approach

**Key decisions explained:**

```
1. CPU: 1024 (1 vCPU)
   ├─ Auth Service needs to hash passwords (CPU-intensive)
   ├─ JWT generation requires CPU
   ├─ 0.5 vCPU insufficient (was getting timeouts)
   └─ 1 vCPU provides necessary compute power

2. Memory: 3072 (3 GB)
   ├─ Spring Boot base: ~300 MB
   ├─ Spring Cloud: ~400 MB
   ├─ JWT + Security libraries: ~200 MB
   ├─ Database connections: ~200 MB
   ├─ User session data: ~100 MB
   ├─ Safety margin: ~1500 MB
   └─ Total needed: ~3000 MB

3. Health Check (startPeriod: 60)
   ├─ Spring Boot takes ~30-60 seconds to start
   ├─ Need to fetch config from Config Server
   ├─ Need to register with Eureka
   ├─ Short startPeriod causes task to be marked unhealthy
   └─ 60 seconds gives enough time

4. Network Mode: awsvpc
   ├─ Required for Fargate
   ├─ Each task gets its own ENI (network interface)
   ├─ Task gets private IP in VPC
   └─ Enables Service Connect DNS
```

### Step-by-Step Process

**Step 1: Calculate Resource Requirements**

```
For Auth Service (banking, password hashing):
├─ CPU: 1 vCPU (high CPU for crypto operations)
├─ Memory: 3 GB (generous for JVM + dependencies)
└─ Result: Well-resourced, no timeouts

For Config Server (lightweight):
├─ CPU: 0.5 vCPU (minimal)
├─ Memory: 1 GB (just config serving)
└─ Result: Cost-efficient
```

**Step 2: Define Environment Variables**

```bash
# Variables needed:
SPRING_CLOUD_CONFIG_URI: http://config-server:8888
EUREKA_CLIENT_SERVICEURL_DEFAULTZONE: http://eureka-server:8761/eureka
MYSQL_HOST: mysql.rds.amazonaws.com
KAFKA_BOOTSTRAP_SERVERS: kafka:9092

# In task definition:
"environment": [
  {"name": "SPRING_CLOUD_CONFIG_URI", "value": "http://config-server:8888"},
  {"name": "EUREKA_CLIENT_SERVICEURL_DEFAULTZONE", "value": "http://eureka-server:8761/eureka"}
]

# For secrets (passwords):
"secrets": [
  {"name": "DB_PASSWORD", "valueFrom": "arn:aws:secretsmanager:..."}
]
```

**Step 3: Configure Port Mapping**

```bash
# Container port: 8081 (app listens on this)
# Host port: 8081 (external traffic comes to this)
# In Fargate, container port = host port

"portMappings": [
  {
    "containerPort": 8081,
    "hostPort": 8081,
    "protocol": "tcp"
  }
]
```

**Step 4: Set Up Logging**

```bash
# Configure CloudWatch logging
"logConfiguration": {
  "logDriver": "awslogs",
  "options": {
    "awslogs-group": "/ecs/citicore-auth-service",
    "awslogs-region": "ap-south-1",
    "awslogs-stream-prefix": "ecs"
  }
}

# Logs accessible via:
aws logs tail /ecs/citicore-auth-service --follow
```

**Step 5: Configure Health Check**

```bash
# Health check tells ECS if task is healthy
"healthCheck": {
  "command": ["CMD-SHELL", "curl -f http://localhost:8081/actuator/health || exit 1"],
  "interval": 30,           # Check every 30 seconds
  "timeout": 5,             # Fail if takes >5 seconds
  "retries": 3,             # Mark unhealthy after 3 failures
  "startPeriod": 60         # CRITICAL: Wait 60s before starting checks
}

# Without startPeriod, task starts health checks immediately
# App takes 30-60s to start
# Health check fails before app is ready
# Task marked unhealthy and replaced (infinite loop)
```

### Interview Explanation

"Task Definition is like a recipe for how to run a container. It specifies: use this Docker image, allocate this much CPU/memory, pass these environment variables, run health checks this way, log to CloudWatch. The critical part is resource allocation—if I allocate too little CPU/memory, app becomes slow or crashes. And the health check startPeriod is crucial—give the app time to start before you start checking if it's healthy. Otherwise, you get an infinite cycle of tasks starting and immediately being killed for failing health checks."

---

## ECS Service

### Definition

**ECS Service** is an AWS resource that:
- Maintains desired number of running tasks
- Launches new tasks if one fails
- Distributes load across tasks
- Manages networking and security
- Integrates with load balancers

Think of it as a manager that ensures X tasks are always running and healthy.

### What You Implemented

```
ECS Service: citicore-auth-service
├─ Task Definition: citicore-auth-service:2
├─ Desired Count: 1 (keep 1 task running)
├─ Launch Type: FARGATE
├─ Network: VPC subnets (private-a, private-b)
├─ Security Group: citicore-ecs-sg
├─ Load Balancer: auth-alb
├─ Target Group: auth-service-tg
└─ Health Check Status: Healthy
```

### Why Use ECS Service

```
Problem 1: Task Failure
├─ Without Service: Task dies, nothing replaces it
├─ With Service: Task dies, Service launches replacement
└─ Benefit: High availability

Problem 2: Scaling
├─ Without Service: Manually launch tasks
├─ With Service: Change desired-count, Service handles it
└─ Benefit: Easy scaling

Problem 3: Load Distribution
├─ Without Service: Single task, no load sharing
├─ With Service: Multiple tasks, load balanced
└─ Benefit: Higher throughput

Problem 4: Rolling Updates
├─ Without Service: Stop old, start new (downtime)
├─ With Service: Gradual task replacement (zero downtime)
└─ Benefit: Deployment without downtime
```

### Step-by-Step Process

**Step 1: Create VPC for Service**

```bash
# Service runs in VPC
# Specify which subnets to use

"networkConfiguration": {
  "awsvpcConfiguration": {
    "subnets": [
      "subnet-private-a",  # Private subnet in AZ-a
      "subnet-private-b"   # Private subnet in AZ-b
    ],
    "securityGroups": ["sg-ecs-id"],
    "assignPublicIp": "DISABLED"  # No public IPs (use ALB)
  }
}
```

**Step 2: Create Load Balancer**

```bash
# Create ALB for external traffic
aws elbv2 create-load-balancer \
  --name auth-alb \
  --subnets subnet-public-a subnet-public-b \
  --security-groups sg-alb-id \
  --scheme internet-facing \
  --type application \
  --region ap-south-1

# Create target group (where ALB sends traffic)
aws elbv2 create-target-group \
  --name auth-service-tg \
  --protocol HTTP \
  --port 8081 \
  --vpc-id vpc-id \
  --target-type ip \
  --health-check-path /actuator/health \
  --health-check-interval-seconds 30 \
  --region ap-south-1
```

**Step 3: Create ECS Service**

```bash
aws ecs create-service \
  --cluster citicore-cluster \
  --service-name citicore-auth-service \
  --task-definition citicore-auth-service:2 \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={
    subnets=[subnet-private-a,subnet-private-b],
    securityGroups=[sg-ecs-id],
    assignPublicIp=DISABLED
  }" \
  --load-balancers targetGroupArn=arn:aws:elasticloadbalancing:...,containerName=auth-service,containerPort=8081 \
  --deployment-configuration maximumPercent=200,minimumHealthyPercent=100 \
  --region ap-south-1
```

**Step 4: Verify Service is Running**

```bash
# Get service details
aws ecs describe-services \
  --cluster citicore-cluster \
  --services citicore-auth-service \
  --region ap-south-1

# Check output:
# status: ACTIVE
# runningCount: 1
# desiredCount: 1
# deployments[0].status: PRIMARY
```

**Step 5: Check Task Health**

```bash
# List tasks
aws ecs list-tasks \
  --cluster citicore-cluster \
  --service-name citicore-auth-service \
  --region ap-south-1

# Describe task
aws ecs describe-tasks \
  --cluster citicore-cluster \
  --tasks arn:aws:ecs:... \
  --region ap-south-1

# Verify:
# lastStatus: RUNNING
# healthStatus: HEALTHY
```

### Interview Explanation

"ECS Service is a manager for containers. I tell it: 'Keep 1 Auth Service task running at all times.' If the task crashes, Service automatically launches a replacement. If I want to scale up to 3 tasks, I change desired-count to 3, and Service launches 2 more. When deploying a new version, the Service does a rolling update: gradually replaces old tasks with new ones, so there's no downtime. It's like having a manager that ensures your restaurant always has 5 chefs on duty—if one gets sick, they hire another."

---

## VPC and Networking

### Definition

**VPC (Virtual Private Cloud)** is your own isolated network in AWS. Within it, you define:
- IP address range (CIDR block)
- Subnets (smaller divisions)
- Internet access (Internet Gateway)
- Routing rules
- Security (Security Groups, Network ACLs)

### What You Implemented

```
VPC: vpc-0d064f45265cbcdad
CIDR: 10.0.0.0/16 (65,536 IP addresses available)

Subnets:
├─ Public-A: 10.0.1.0/24 (public, 254 IPs) - ALB here
├─ Public-B: 10.0.2.0/24 (public, 254 IPs) - ALB backup
├─ Private-A: 10.0.11.0/24 (private, 254 IPs) - Auth Service here
├─ Private-B: 10.0.12.0/24 (private, 254 IPs) - Auth Service backup
├─ DB-A: 10.0.21.0/24 (database only, 254 IPs) - RDS primary
└─ DB-B: 10.0.22.0/24 (database only, 254 IPs) - RDS replica

Security Groups:
├─ ALB SG: Allows internet traffic on port 80/443
├─ ECS SG: Allows ALB on port 8081, internal communication
├─ RDS SG: Allows ECS on port 3306
├─ Kafka SG: Allows ECS on port 9092
└─ Redis SG: Allows ECS on port 6379
```

### Why This Architecture

```
1. PUBLIC SUBNETS FOR ALB
   ├─ ALB needs internet access
   ├─ Sends traffic to private ECS
   └─ Benefit: Separation of concerns

2. PRIVATE SUBNETS FOR APPLICATION
   ├─ ECS tasks not directly accessible from internet
   ├─ Must go through ALB
   └─ Benefit: Security isolation

3. DATABASE SUBNETS
   ├─ RDS only in database subnets
   ├─ Cannot be accessed from public
   └─ Benefit: Database protection

4. MULTIPLE AVAILABILITY ZONES
   ├─ Subnet-A in ap-south-1a
   ├─ Subnet-B in ap-south-1b
   └─ Benefit: High availability (one AZ fails, other keeps running)
```

### Step-by-Step Process

**Step 1: Create VPC**

```bash
aws ec2 create-vpc \
  --cidr-block 10.0.0.0/16 \
  --region ap-south-1

# Output: vpc-0d064f45265cbcdad
```

**Step 2: Create Internet Gateway**

```bash
# Create IGW
aws ec2 create-internet-gateway --region ap-south-1

# Attach to VPC
aws ec2 attach-internet-gateway \
  --internet-gateway-id igw-id \
  --vpc-id vpc-0d064f45265cbcdad \
  --region ap-south-1
```

**Step 3: Create Subnets**

```bash
# Public subnet A
aws ec2 create-subnet \
  --vpc-id vpc-0d064f45265cbcdad \
  --cidr-block 10.0.1.0/24 \
  --availability-zone ap-south-1a \
  --region ap-south-1

# Public subnet B
aws ec2 create-subnet \
  --vpc-id vpc-0d064f45265cbcdad \
  --cidr-block 10.0.2.0/24 \
  --availability-zone ap-south-1b \
  --region ap-south-1

# Private subnet A
aws ec2 create-subnet \
  --vpc-id vpc-0d064f45265cbcdad \
  --cidr-block 10.0.11.0/24 \
  --availability-zone ap-south-1a \
  --region ap-south-1

# Private subnet B
aws ec2 create-subnet \
  --vpc-id vpc-0d064f45265cbcdad \
  --cidr-block 10.0.12.0/24 \
  --availability-zone ap-south-1b \
  --region ap-south-1

# DB subnet A
aws ec2 create-subnet \
  --vpc-id vpc-0d064f45265cbcdad \
  --cidr-block 10.0.21.0/24 \
  --availability-zone ap-south-1a \
  --region ap-south-1

# DB subnet B
aws ec2 create-subnet \
  --vpc-id vpc-0d064f45265cbcdad \
  --cidr-block 10.0.22.0/24 \
  --availability-zone ap-south-1b \
  --region ap-south-1
```

**Step 4: Create Route Tables**

```bash
# Route table for public subnets
aws ec2 create-route-table \
  --vpc-id vpc-0d064f45265cbcdad \
  --region ap-south-1

# Add route to IGW (public internet access)
aws ec2 create-route \
  --route-table-id rt-public \
  --destination-cidr-block 0.0.0.0/0 \
  --gateway-id igw-id \
  --region ap-south-1

# Associate public subnets
aws ec2 associate-route-table \
  --subnet-id subnet-public-a \
  --route-table-id rt-public \
  --region ap-south-1

# Route table for private subnets (no internet route)
aws ec2 create-route-table \
  --vpc-id vpc-0d064f45265cbcdad \
  --region ap-south-1

# Associate private subnets
aws ec2 associate-route-table \
  --subnet-id subnet-private-a \
  --route-table-id rt-private \
  --region ap-south-1
```

**Step 5: Create Security Groups**

```bash
# ALB Security Group
aws ec2 create-security-group \
  --group-name alb-sg \
  --description "ALB security group" \
  --vpc-id vpc-0d064f45265cbcdad \
  --region ap-south-1

# Allow HTTP from internet
aws ec2 authorize-security-group-ingress \
  --group-id sg-alb \
  --protocol tcp \
  --port 80 \
  --cidr 0.0.0.0/0 \
  --region ap-south-1

# ECS Security Group
aws ec2 create-security-group \
  --group-name ecs-sg \
  --description "ECS security group" \
  --vpc-id vpc-0d064f45265cbcdad \
  --region ap-south-1

# Allow ALB to reach ECS on port 8081
aws ec2 authorize-security-group-ingress \
  --group-id sg-ecs \
  --protocol tcp \
  --port 8081 \
  --source-group sg-alb \
  --region ap-south-1
```

### Interview Explanation

"VPC is your own isolated network on AWS. I designed it with public subnets for the load balancer (needs internet access), private subnets for application servers (protected from internet), and database subnets for RDS (even more protected). I spread subnets across two availability zones for high availability—if one data center fails, the other keeps running. Security groups act as firewalls: the ALB accepts traffic from the internet, but the ECS tasks only accept traffic from the ALB, and the database only accepts from the application servers. It's like a castle with outer walls (public), middle walls (private), and inner vault (database)."

---

## IAM Roles

### Definition

**IAM (Identity and Access Management)** roles define permissions for AWS services. There are two important roles for ECS:

1. **Task Role**: Permissions for the application inside the container
2. **Task Execution Role**: Permissions for ECS to manage the container

### What You Implemented

**Task Execution Role: citicore-ecs-task-execution-role**

```
Purpose: ECS infrastructure operations
Allows:
├─ ecr:GetDownloadUrlForLayer
├─ ecr:BatchGetImage
├─ ecr:GetAuthorizationToken
├─ logs:CreateLogStream
├─ logs:PutLogEvents
└─ secretsmanager:GetSecretValue

When ECS needs to:
├─ Pull image from ECR
├─ Write logs to CloudWatch
└─ Retrieve secrets from Secrets Manager
```

**Task Role: citicore-ecs-task-role**

```
Purpose: Application operations within container
Allows:
├─ dynamodb:*          (if using DynamoDB)
├─ s3:GetObject        (if using S3)
├─ secretsmanager:GetSecretValue
└─ cloudwatch:PutMetricData

When application needs to:
├─ Store data in DynamoDB
├─ Access S3 buckets
└─ Publish metrics to CloudWatch
```

### Why Separate Roles

```
Task Execution Role:
├─ Used by ECS/Fargate itself
├─ For infrastructure operations
└─ Minimal permissions

Task Role:
├─ Used by application code
├─ For business logic
└─ More permissions needed

Benefit: Least privilege principle
├─ Application only has permissions it needs
├─ If application compromised, limited damage
└─ Security best practice
```

### Step-by-Step Process

**Step 1: Create Task Execution Role**

```bash
# Create role
aws iam create-role \
  --role-name citicore-ecs-task-execution-role \
  --assume-role-policy-document file://trust-policy.json

# trust-policy.json:
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

# Attach policy for ECR access
aws iam attach-role-policy \
  --role-name citicore-ecs-task-execution-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly

# Attach policy for CloudWatch logs
aws iam attach-role-policy \
  --role-name citicore-ecs-task-execution-role \
  --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess

# Attach policy for Secrets Manager
aws iam attach-role-policy \
  --role-name citicore-ecs-task-execution-role \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
```

**Step 2: Create Task Role**

```bash
# Create role
aws iam create-role \
  --role-name citicore-ecs-task-role \
  --assume-role-policy-document file://trust-policy.json

# Create inline policy for application-specific permissions
aws iam put-role-policy \
  --role-name citicore-ecs-task-role \
  --policy-name citicore-app-policy \
  --policy-document file://app-policy.json

# app-policy.json:
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:ap-south-1:580655778303:secret:citicore/*"
    }
  ]
}
```

**Step 3: Use Roles in Task Definition**

```bash
# In task definition JSON:
{
  "executionRoleArn": "arn:aws:iam::580655778303:role/citicore-ecs-task-execution-role",
  "taskRoleArn": "arn:aws:iam::580655778303:role/citicore-ecs-task-role"
}
```

### Interview Explanation

"IAM roles control what permissions tasks have. The execution role is for ECS to pull images from ECR and write logs to CloudWatch—infrastructure stuff. The task role is for the application code inside the container to access AWS services like Secrets Manager. They're separate because the application shouldn't have permissions it doesn't need. If the application is compromised, the attacker only gets the permissions granted to the task role, which is limited. It's the principle of least privilege: each service gets only the permissions it requires."

---

## Spring Cloud Config Server

### Definition

**Spring Cloud Config Server** is a centralized configuration management service. Instead of embedding configuration in code or environment variables, services fetch configuration from a central server at startup.

```
Without Config Server:
├─ Config in application.yml (in code)
├─ Config in environment variables
└─ Hard to update without redeployment

With Config Server:
├─ Config in Git repository
├─ Services fetch at startup
├─ Change without redeployment (Spring Cloud Bus)
└─ Single source of truth
```

### What You Implemented

```
Config Server: citicore-config-repo
├─ Repository: Rabbaniinamdar/citicore-config-repo (GitHub)
├─ Port: 8888
├─ Files:
│  ├─ application.yml (shared config)
│  ├─ auth-service.yml
│  ├─ account-service.yml
│  ├─ user-service.yml
│  ├─ transaction-service.yml
│  ├─ notification-service.yml
│  └─ apigateway-service.yml
└─ Benefits:
   ├─ Version control for configurations
   ├─ Easy rollback of config changes
   └─ Track who changed what
```

### Why Use Config Server

```
Problem 1: Configuration Duplication
├─ Without: Each service has its own copy of shared config
├─ With: Shared config in one place
└─ Benefit: DRY principle (Don't Repeat Yourself)

Problem 2: Configuration Changes
├─ Without: Recompile, rebuild Docker image, redeploy
├─ With: Change in Git, server fetches new config
└─ Benefit: Fast updates, no redeployment

Problem 3: Configuration Consistency
├─ Without: Easy to have config drift between services
├─ With: All services use same source
└─ Benefit: Consistency, fewer surprises

Problem 4: Multiple Environments
├─ Without: Hard to manage dev/staging/prod configs
├─ With: Different branches or files per environment
└─ Benefit: Environment-specific configurations
```

### Step-by-Step Process

**Step 1: Create GitHub Configuration Repository**

```bash
# Create repo: citicore-config-repo

# Structure:
citicore-config-repo/
├─ application.yml          # Shared config
├─ auth-service.yml
├─ account-service.yml
└─ ...

# application.yml (shared):
spring:
  kafka:
    bootstrap-servers: kafka:9092
  
eureka:
  client:
    service-url:
      defaultZone: http://eureka-server:8761/eureka

# auth-service.yml (auth-specific):
spring:
  datasource:
    url: jdbc:mysql://mysql:3306/auth_db
    username: app_user
    password: ${DB_PASSWORD}
  
server:
  port: 8081
```

**Step 2: Configure Config Server Application**

```bash
# application.yml for Config Server itself:
spring:
  application:
    name: config-server
  cloud:
    config:
      server:
        git:
          uri: https://github.com/Rabbaniinamdar/citicore-config-repo
          clone-on-start: true
          default-label: main
          username: ${GITHUB_USER}
          password: ${GITHUB_TOKEN}

server:
  port: 8888
```

**Step 3: Start Config Server**

```bash
# Build Docker image
docker build -t citicore/config-server:1.0 .

# Push to ECR
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/config-server:1.0

# Deploy to ECS
aws ecs update-service \
  --cluster citicore-cluster \
  --service config-server \
  --task-definition config-server:1 \
  --force-new-deployment
```

**Step 4: Configure Service to Use Config Server**

```bash
# Create bootstrap.yml in Auth Service:
spring:
  application:
    name: auth-service
  cloud:
    config:
      uri: http://config-server:8888
      fail-fast: true

# Auth Service fetches config on startup from Config Server
```

**Step 5: Verify Configuration**

```bash
# Config Server provides endpoints:
curl http://config-server:8888/auth-service/default
# Returns: auth-service configuration

curl http://config-server:8888/account-service/default
# Returns: account-service configuration

curl http://config-server:8888/application/default
# Returns: shared configuration
```

### Interview Explanation

"Config Server centralizes configuration management. Instead of baking configuration into Docker images or using environment variables, services fetch their configuration from a Git-backed server at startup. This provides several benefits: version control for configurations (track changes, rollback), single source of truth (no config drift), and fast updates (change Git, service picks it up without redeployment). For microservices running 10+ instances, this beats managing individual configs per instance. Combined with Spring Cloud Bus, you can broadcast configuration changes to all services instantly."

---

## Eureka Service Discovery

### Definition

**Eureka** is a service discovery tool that maintains a registry of microservice instances. Instead of hardcoding IP addresses, services register with Eureka and discover each other dynamically.

```
Without Eureka (hardcoded):
├─ Account Service at 10.0.1.50:8081
├─ User Service at 10.0.1.51:8082
└─ If 10.0.1.50 crashes, everything breaks

With Eureka (dynamic):
├─ Account Service registers: "I'm at 10.0.1.50:8081"
├─ Account Service crashes, replaced at 10.0.1.60:8081
├─ Eureka updated automatically
└─ User Service discovers new address automatically
```

### What You Implemented

```
Eureka Server:
├─ Service: citicore-eureka-server
├─ Port: 8761
├─ Task Definition: citicore-eureka-server:1
├─ Running: 1 task
└─ Status: HEALTHY

Configuration:
eureka:
  client:
    register-with-eureka: false  (Eureka doesn't register itself)
    fetch-registry: false         (Single Eureka, no clustering)
  server:
    enable-self-preservation: false

Registered Services:
├─ auth-service (http://eureka-server-8761-tcp.citicore:8761/eureka)
├─ account-service
├─ user-service
└─ ... (others as deployed)
```

### Why Use Eureka

```
Problem 1: Hardcoded IP Addresses
├─ ECS Fargate replaces tasks, IP changes
├─ Hardcoded addresses break
└─ Solution: Dynamic discovery

Problem 2: Load Balancing
├─ Multiple instances of same service
├─ Need to distribute requests
└─ Eureka provides list of healthy instances

Problem 3: Failure Detection
├─ Task crashes or becomes unhealthy
├─ Must be removed from rotation
└─ Eureka health checks detect failures

Problem 4: Scaling
├─ Add more instances automatically
├─ Other services automatically discover them
└─ No configuration changes needed
```

### Step-by-Step Process

**Step 1: Build Eureka Server**

```bash
# pom.xml dependencies:
<dependency>
  <groupId>org.springframework.cloud</groupId>
  <artifactId>spring-cloud-starter-netflix-eureka-server</artifactId>
</dependency>

# Main class:
@SpringBootApplication
@EnableEurekaServer
public class EurekaServerApplication {
  public static void main(String[] args) {
    SpringApplication.run(EurekaServerApplication.class, args);
  }
}

# application.yml:
spring:
  application:
    name: eureka-server
server:
  port: 8761
eureka:
  client:
    register-with-eureka: false
    fetch-registry: false
```

**Step 2: Build Docker Image**

```bash
# Dockerfile:
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/eureka-server-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8761
ENTRYPOINT ["java", "-jar", "app.jar"]

# Build:
docker build -t citicore/eureka-server:1.0 .

# Test locally:
docker run -d -p 8761:8761 citicore/eureka-server:1.0
# Visit: http://localhost:8761/
```

**Step 3: Deploy to ECS**

```bash
# Push to ECR
docker push 580655778303.dkr.ecr.ap-south-1.amazonaws.com/citicore/eureka-server:1.0

# Create task definition
aws ecs register-task-definition --cli-input-json file://eureka-task-def.json

# Create service
aws ecs create-service \
  --cluster citicore-cluster \
  --service-name citicore-eureka-server \
  --task-definition citicore-eureka-server:1 \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[...],securityGroups=[...]}"
```

**Step 4: Configure Services to Register with Eureka**

```bash
# Auth Service bootstrap.yml:
eureka:
  client:
    register-with-eureka: true  (Register this service)
    fetch-registry: true        (Discover other services)
    service-url:
      defaultZone: http://eureka-server-8761-tcp.citicore:8761/eureka
  instance:
    hostname: auth-service
    lease-renewal-interval-in-seconds: 10  (Send heartbeat every 10s)

# Account Service bootstrap.yml: (same pattern)
eureka:
  client:
    register-with-eureka: true
    fetch-registry: true
    service-url:
      defaultZone: http://eureka-server-8761-tcp.citicore:8761/eureka
```

**Step 5: Verify Services Are Registered**

```bash
# Check Eureka dashboard
curl http://eureka-server:8761/

# Get registered apps
curl http://eureka-server:8761/eureka/apps

# Output shows:
{
  "applications": {
    "application": [
      {
        "name": "AUTH-SERVICE",
        "instance": [
          {
            "instanceId": "auth-service",
            "hostName": "10.0.11.50",
            "port": 8081,
            "status": "UP"
          }
        ]
      }
    ]
  }
}

# Get specific service
curl http://eureka-server:8761/eureka/apps/AUTH-SERVICE
```

### Interview Explanation

"Eureka solves the problem of dynamic discovery in microservices. When a container starts in Fargate, it gets a private IP address. Instead of hardcoding that address, the service registers with Eureka: 'I'm Auth Service at 10.0.11.50:8081.' Other services query Eureka: 'Where is Auth Service?' and get the current address. If a task crashes and restarts at a different IP, Eureka updates automatically. It's like a phone directory: instead of remembering phone numbers, you call the directory, ask 'What's Bob's number?', and get the current number. If Bob moves and gets a new number, he updates the directory, and everyone automatically calls the new number."

---

## ECS Service Connect

### Definition

**ECS Service Connect** is AWS's feature for service-to-service networking within ECS. It provides:
- DNS-based service discovery
- Built-in load balancing
- Network isolation
- No need for external service mesh

```
Without Service Connect:
├─ Services use IP addresses directly
├─ Hard to manage, not flexible
└─ Services tightly coupled

With Service Connect:
├─ Services use DNS names
├─ Automatic load balancing
├─ Namespace isolation
└─ Easy scaling
```

### What You Implemented

```
Service Connect Namespace: citicore

Eureka Server Discovery Name:
├─ Name: eureka-server-8761-tcp
├─ DNS: eureka-server-8761-tcp.citicore:8761
└─ Used by Auth Service to reach Eureka

Auth Service Discovery Name:
├─ Name: auth-service-8081-tcp
├─ DNS: auth-service-8081-tcp.citicore:8081
└─ Used by other services to reach Auth

Configuration:
├─ Namespace: citicore (shared namespace)
├─ Network Mode: awsvpc (required for Service Connect)
└─ Services can discover each other via DNS
```

### Why Use Service Connect

```
Problem 1: Service-to-Service Communication
├─ Without Service Connect: Hard to reach other services
├─ With Service Connect: Use DNS names
└─ Benefit: Decoupling from IP addresses

Problem 2: Multiple Instances
├─ Without: Need to manually load balance
├─ With: Automatic load balancing across instances
└─ Benefit: High availability

Problem 3: Network Flexibility
├─ Without: Need API Gateway or Service Mesh
├─ With: Built into ECS
└─ Benefit: Simplicity, no additional tools
```

### Step-by-Step Process

**Step 1: Enable Service Connect on Service**

```bash
# Create service with Service Connect
aws ecs create-service \
  --cluster citicore-cluster \
  --service-name citicore-eureka-server \
  --task-definition citicore-eureka-server:1 \
  --desired-count 1 \
  --launch-type FARGATE \
  --service-connect-configuration '{
    "enabled": true,
    "namespace": "citicore",
    "services": [
      {
        "portName": "tcp",
        "discoveryName": "eureka-server-8761-tcp",
        "clientAliases": [
          {
            "port": 8761,
            "dnsName": "eureka-server-8761-tcp.citicore"
          }
        ]
      }
    ]
  }'

# Or update existing service
aws ecs update-service \
  --cluster citicore-cluster \
  --service citicore-eureka-server \
  --service-connect-configuration '{
    "enabled": true,
    "namespace": "citicore",
    ...
  }'
```

**Step 2: Configure Services to Use Service Connect DNS**

```bash
# Auth Service bootstrap.yml:
eureka:
  client:
    register-with-eureka: true
    fetch-registry: true
    service-url:
      defaultZone: http://eureka-server-8761-tcp.citicore:8761/eureka
      # Uses Service Connect DNS name instead of IP
  instance:
    prefer-ip-address: false  # Use hostname, not IP
```

**Step 3: Verify Service Connect**

```bash
# From inside Auth Service container:
curl http://eureka-server-8761-tcp.citicore:8761/

# If working:
# Response: Eureka Server page

# If not working (UnknownHostException):
# Need to verify:
# 1. Service Connect enabled on both services
# 2. Namespace names match
# 3. Discovery names correct
# 4. Network mode is awsvpc
```

**Step 4: Handle DNS Issues (UnknownHostException)**

```
Problem: UnknownHostException: eureka-server-8761-tcp.citicore

Root Causes:
├─ Service Connect not enabled on Eureka
├─ Discovery name misspelled
├─ Namespace name doesn't match
├─ Network mode not awsvpc
└─ Service not in same namespace

Solution:
1. Check Eureka service has Service Connect enabled
   aws ecs describe-services --cluster citicore-cluster --services citicore-eureka-server
   
2. Verify namespace matches
   Both services should have: "namespace": "citicore"
   
3. Check discovery name in Auth config
   Should match Eureka's discoveryName: "eureka-server-8761-tcp"
   
4. Verify network mode
   Task definition should have: "networkMode": "awsvpc"
   
5. Check logs in CloudWatch
   aws logs tail /ecs/citicore-auth-service --follow
   Look for: "failed to resolve eureka-server-8761-tcp.citicore"
```

### Interview Explanation

"Service Connect is ECS's built-in networking for service-to-service communication. Instead of services hardcoding IP addresses of other services, they use DNS names through Service Connect. Auth Service doesn't need to know Eureka's IP; it just asks: 'Where is eureka-server-8761-tcp.citicore?' and Service Connect responds with the current IP. If Eureka task crashes and restarts at a different IP, Service Connect updates automatically. It's simpler than a service mesh, no external components needed—just configure the namespace and discovery names, and ECS handles routing."

---

## Application Load Balancer (ALB)

### Definition

**Application Load Balancer (ALB)** distributes incoming traffic across multiple targets (ECS tasks). It's layer 7 aware (understands HTTP, hostnames, paths).

```
Without ALB:
├─ Clients connect directly to tasks
├─ If task crashes, connection lost
├─ No load balancing
└─ Client gets specific IP

With ALB:
├─ Clients connect to ALB (single entry point)
├─ ALB distributes to healthy tasks
├─ If task crashes, traffic automatically diverted
└─ Automatic failover
```

### What You Implemented

```
Auth ALB:
├─ Name: auth-alb
├─ Port: 80 (HTTP) → routes to port 8081
├─ Subnets: Public subnets (public-a, public-b)
├─ Security Group: alb-sg
├─ Target Group: auth-service-tg
├─ Targets: Auth Service ECS tasks
├─ Health Check: /actuator/health
└─ Status: Active, all targets healthy

Routing:
Internet:80
   ↓
ALB:80
   ↓
Target Group
   ↓
Auth Service Task 1: 10.0.11.50:8081
Auth Service Task 2: 10.0.11.51:8081
Auth Service Task 3: 10.0.11.52:8081
   ↓
Distributed across all healthy tasks
```

### Why Use ALB

```
Problem 1: Single Point of Failure
├─ Without: If Auth Service task crashes, service down
├─ With: Other tasks take traffic
└─ Benefit: High availability

Problem 2: Load Distribution
├─ Without: All traffic to one task
├─ With: Traffic distributed across tasks
└─ Benefit: Better performance

Problem 3: Scaling
├─ Without: Hard to add new tasks
├─ With: New tasks automatically added to ALB
└─ Benefit: Easy horizontal scaling

Problem 4: Failure Recovery
├─ Without: Manual intervention to failover
├─ With: Automatic detection and rerouting
└─ Benefit: Self-healing infrastructure
```

### Step-by-Step Process

**Step 1: Create ALB**

```bash
# Create load balancer
aws elbv2 create-load-balancer \
  --name auth-alb \
  --subnets subnet-public-a subnet-public-b \
  --security-groups sg-alb-id \
  --scheme internet-facing \
  --type application \
  --region ap-south-1

# Output:
# {
#   "LoadBalancers": [
#     {
#       "LoadBalancerArn": "arn:aws:elasticloadbalancing:...",
#       "DNSName": "auth-alb-123456789.ap-south-1.elb.amazonaws.com"
#     }
#   ]
# }
```

**Step 2: Create Target Group**

```bash
# Target group is where ALB sends traffic
aws elbv2 create-target-group \
  --name auth-service-tg \
  --protocol HTTP \
  --port 8081 \
  --vpc-id vpc-id \
  --target-type ip \
  --health-check-enabled \
  --health-check-path /actuator/health \
  --health-check-protocol HTTP \
  --health-check-port 8081 \
  --health-check-interval-seconds 30 \
  --health-check-timeout-seconds 5 \
  --healthy-threshold-count 2 \
  --unhealthy-threshold-count 3 \
  --region ap-south-1

# Output:
# {
#   "TargetGroups": [
#     {
#       "TargetGroupArn": "arn:aws:elasticloadbalancing:...",
#       "TargetGroupName": "auth-service-tg"
#     }
#   ]
# }
```

**Step 3: Create ALB Listener**

```bash
# Listener tells ALB what to do with incoming traffic
aws elbv2 create-listener \
  --load-balancer-arn arn:aws:elasticloadbalancing:... \
  --protocol HTTP \
  --port 80 \
  --default-actions Type=forward,TargetGroupArn=arn:... \
  --region ap-south-1

# Listener configuration:
# HTTP:80 → Forward to target group (auth-service-tg)
# Port 8081 is where the tasks listen
```

**Step 4: Verify Target Health**

```bash
# Check target health
aws elbv2 describe-target-health \
  --target-group-arn arn:aws:elasticloadbalancing:... \
  --region ap-south-1

# Output:
# {
#   "TargetHealthDescriptions": [
#     {
#       "Target": {
#         "Id": "10.0.11.50",
#         "Port": 8081
#       },
#       "TargetHealth": {
#         "State": "healthy",
#         "Reason": "N/A"
#       }
#     }
#   ]
# }

# If unhealthy:
# Check:
# 1. Task running
# 2. Security group allows ALB
# 3. /actuator/health endpoint working
# 4. App has started (check startPeriod)
```

**Step 5: Connect ECS Service to ALB**

```bash
# When creating ECS service, specify target group
aws ecs create-service \
  --cluster citicore-cluster \
  --service-name citicore-auth-service \
  --task-definition citicore-auth-service:1 \
  --desired-count 1 \
  --launch-type FARGATE \
  --load-balancers targetGroupArn=arn:...,containerName=auth-service,containerPort=8081 \
  --region ap-south-1

# This tells ECS to:
# 1. Launch Auth Service tasks
# 2. Register them with ALB target group
# 3. ALB automatically routes traffic to healthy tasks
```

**Step 6: Test ALB**

```bash
# Get ALB DNS name
aws elbv2 describe-load-balancers \
  --names auth-alb \
  --region ap-south-1

# Test endpoint
curl http://auth-alb-123456789.ap-south-1.elb.amazonaws.com/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Pass@123"}'

# If working:
# Response: 201 Created (or appropriate response)

# If not working:
# Check:
# 1. ALB status (Active)
# 2. Target health (Healthy)
# 3. Security group rules
# 4. CloudWatch logs
```

### Interview Explanation

"ALB is the gateway between the internet and our services. Instead of clients connecting directly to specific task IPs (which change constantly in Fargate), they connect to the ALB DNS name, which is stable. The ALB has a target group that knows about all Auth Service tasks. When a request comes in, ALB checks which targets are healthy and routes to one of them. If a task crashes, the ALB detects it's unhealthy and stops sending traffic there. New traffic goes to remaining healthy tasks. When we scale up (add more tasks), they're automatically added to the target group and receive traffic. It's the load-balancing glue that holds everything together."

---

## Real Issues & Troubleshooting

### Issue #1: 504 Gateway Timeout on Registration

**Problem:**

```
POST /api/v1/auth/register
Status: 504 Gateway Timeout
Response: "upstream request timeout"
Duration: 16+ seconds before timing out
```

**Root Cause Analysis:**

The troubleshooting process went through multiple layers:

```
Layer 1: ECS Task Status
├─ ✅ Task is RUNNING
└─ Task health checks passing

Layer 2: ALB Target Health
├─ ✅ Target shows HEALTHY
└─ No connection issues to task

Layer 3: ALB Health Endpoint
├─ ✅ /actuator/health returns 200 OK
└─ Application responding to health checks

Layer 4: Application Logic
├─ ✅ User saved to database
├─ ✅ OTP generated
├─ ✅ Response prepared
└─ ⚠️ BUT: Timeout still occurring

Layer 5: Kafka Producer
├─ ❌ Publishing OTP event to Kafka
├─ ❌ Topic 'otp-topic' does not exist
└─ HANGING waiting for response

Root Cause Found: Kafka topic missing
```

**The Issue:**

```
Auth Service Registration Flow:
1. Validate input
2. Hash password
3. Save user to database
4. Generate OTP
5. Publish VerificationOtpEvent to Kafka → HANGS HERE
6. Return response

Kafka Issue:
├─ Topic 'otp-topic' not created
├─ Kafka tries to create it
├─ Broker config says: don't auto-create
├─ Kafka hangs waiting
├─ ALB timeout after 30 seconds
├─ Returns 504 to client
└─ User still saved in database!

Important: 504 doesn't mean operation failed
├─ User WAS saved
├─ OTP WAS generated
├─ But client got timeout
└─ Could be duplicate user on retry
```

**Solution:**

```bash
# Step 1: Verify Kafka is running
docker exec kafka bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Step 2: Check existing topics
docker exec kafka bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Step 3: Create missing otp-topic
docker exec kafka bin/kafka-topics.sh \
  --create \
  --topic otp-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Step 4: Verify topic created
docker exec kafka bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Step 5: Test producer
docker exec kafka bin/kafka-console-consumer.sh \
  --topic otp-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Step 6: Retry registration
curl -X POST http://auth-alb/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Pass@123"}'

# Result: Now completes successfully (or different error if other issues)
```

**Key Lesson:**

```
504 Gateway Timeout does NOT mean:
├─ Request failed
├─ Data not saved
├─ Operation incomplete
└─ Everything is bad

504 means:
├─ ALB didn't get response within timeout period
├─ Could be hanging on external service (Kafka, DB, etc.)
├─ Check application logs
├─ Data might be partially saved
└─ Retry might cause issues (duplicates)
```

### Issue #2: UnknownHostException for Eureka Service Connect DNS

**Problem:**

```
CloudWatch logs show:
UnknownHostException: eureka-server-8761-tcp.citicore
```

**Root Cause:**

```
Auth Service tries to register with Eureka:
├─ Configuration: eureka.client.service-url.defaultZone=http://eureka-server-8761-tcp.citicore:8761/eureka
├─ Auth Service tries to resolve DNS
├─ Java DNS resolver can't find eureka-server-8761-tcp.citicore
├─ Throws UnknownHostException
└─ Service fails to register

Why?
├─ Service Connect not enabled on Eureka
├─ Or discovery name is different
├─ Or namespace is different
└─ Or Service Connect DNS not propagated yet
```

**Solution:**

```bash
# Step 1: Verify Eureka has Service Connect enabled
aws ecs describe-services \
  --cluster citicore-cluster \
  --services citicore-eureka-server \
  --region ap-south-1 | jq '.services[0].serviceConnectConfiguration'

# Should show:
# {
#   "enabled": true,
#   "namespace": "citicore",
#   "services": [
#     {
#       "discoveryName": "eureka-server-8761-tcp"
#     }
#   ]
# }

# Step 2: If Service Connect not enabled, update it
aws ecs update-service \
  --cluster citicore-cluster \
  --service citicore-eureka-server \
  --service-connect-configuration '{
    "enabled": true,
    "namespace": "citicore",
    "services": [
      {
        "portName": "tcp",
        "discoveryName": "eureka-server-8761-tcp",
        "clientAliases": [
          {
            "port": 8761,
            "dnsName": "eureka-server-8761-tcp.citicore"
          }
        ]
      }
    ]
  }' \
  --force-new-deployment

# Step 3: Wait for Eureka to redeploy
aws ecs wait services-stable \
  --cluster citicore-cluster \
  --services citicore-eureka-server

# Step 4: Check Auth Service configuration matches
# bootstrap.yml should have:
# eureka:
#   client:
#     service-url:
#       defaultZone: http://eureka-server-8761-tcp.citicore:8761/eureka

# Step 5: Restart Auth Service
aws ecs update-service \
  --cluster citicore-cluster \
  --service citicore-auth-service \
  --force-new-deployment

# Step 6: Monitor logs
aws logs tail /ecs/citicore-auth-service --follow

# Should see: "Started AuthServiceApplication" (without UnknownHostException)
```

### Issue #3: Kafka UNKNOWN_TOPIC_OR_PARTITION Error

**Problem:**

```
CloudWatch logs:
org.apache.kafka.common.errors.UnknownTopicOrPartitionException: Topic 'otp-topic' not found
```

**Root Cause:**

```
Kafka broker configuration:
├─ auto.create.topics.enable = false
├─ Topics must be created manually
├─ Application tries to produce to non-existent topic
└─ Kafka rejects the message
```

**Solution:**

```bash
# Same as 504 troubleshooting, create the topic:
docker exec kafka bin/kafka-topics.sh \
  --create \
  --topic otp-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Verify:
docker exec kafka bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep otp-topic
```

### Troubleshooting Process (General Pattern)

```
When something fails:

1. CHECK ECS TASK
   ├─ Task running?
   ├─ Task healthy?
   └─ CloudWatch logs
   
2. CHECK ALB
   ├─ ALB active?
   ├─ Target healthy?
   ├─ Test health endpoint
   └─ Check target group
   
3. CHECK APPLICATION LOGS
   ├─ What error message?
   ├─ Stack trace?
   └─ When did it start?
   
4. CHECK DEPENDENCIES
   ├─ Config Server reachable?
   ├─ Eureka reachable?
   ├─ Database reachable?
   ├─ Kafka reachable?
   └─ Redis reachable?
   
5. CHECK CONFIGURATION
   ├─ URLs correct?
   ├─ Credentials correct?
   ├─ Environment variables set?
   └─ Secrets accessible?
   
6. VERIFY DATA
   ├─ Was the operation actually completed?
   ├─ Check database for data
   ├─ Check Kafka for messages
   └─ Don't assume failure from timeout
   
7. NARROW DOWN LAYER
   ├─ Network issue (security groups)?
   ├─ DNS issue (Service Connect)?
   ├─ Application issue (code bug)?
   ├─ Configuration issue (wrong URL)?
   └─ External dependency (Kafka not responding)?
```

---

## Kafka Event Streaming

### Definition

**Kafka** is a distributed event streaming platform. It decouples services by using publish-subscribe messaging instead of direct service calls.

```
Direct Call (Tight Coupling):
Auth Service → Call Notification Service → Send Email
Problem: If Notification Service down, Auth fails

Kafka (Loose Coupling):
Auth Service → Publish to Kafka → Notification Service reads
Benefit: If Notification Service down, Kafka queues events
```

### What You Implemented

**Kafka on EC2:**

```
Kafka Broker:
├─ Server: citicore-infra (EC2 t3.small)
├─ Port: 9092
├─ Docker Container: apache/kafka:4.0.1
├─ Mode: KRaft (no ZooKeeper)
└─ Bootstrap Servers: 10.0.1.87:9092

Topics:
├─ otp-topic: OTP verification events
├─ auth-events: Authentication events
├─ account-events: Account events
└─ ... (others as needed)

Producer (Auth Service):
├─ Publishes VerificationOtpEvent to otp-topic
├─ Event contains: email, otp, timestamp
└─ Notification Service consumes

Consumer (Notification Service - Not Yet Deployed):
├─ Reads from otp-topic
├─ Sends OTP via email
└─ Marks event processed
```

### Why Use Kafka

```
Problem 1: Synchronous Dependencies
├─ Auth calls Notification directly
├─ If Notification slow/down, Auth affected
└─ Solution: Async via Kafka

Problem 2: Event Loss
├─ If Notification Service crashes, event lost
├─ Solution: Kafka persists events

Problem 3: Scaling Consumers
├─ Without Kafka: Hardcoded consumers
├─ With Kafka: Add consumers dynamically
└─ Benefit: Easy to add Notification instances

Problem 4: Event History
├─ Without: No record of events
├─ With: Kafka retains events (configurable)
└─ Benefit: Audit trail, replay if needed
```

### Auth Service Kafka Flow

```
User Registration:
1. POST /api/v1/auth/register
2. Auth Service:
   ├─ Validate input
   ├─ Hash password
   ├─ Save user to MySQL
   ├─ Generate OTP
   ├─ Create VerificationOtpEvent
   └─ Publish to Kafka (otp-topic)

3. Kafka:
   ├─ Receives event
   ├─ Stores in topic partition
   └─ Persists to disk

4. Notification Service (when deployed):
   ├─ Reads from otp-topic
   ├─ Extracts email and OTP
   ├─ Calls email provider
   └─ Sends email

5. User receives:
   ├─ Email with OTP
   └─ Can verify email by calling /api/v1/auth/verify

6. After verification:
   ├─ User status → ACTIVE
   └─ Can login to system
```

### Step-by-Step Kafka Setup

**Step 1: Start Kafka on EC2**

```bash
# SSH into EC2
ssh -i key.pem ubuntu@ec2-instance.ap-south-1.compute.amazonaws.com

# Create docker-compose.yml:
version: '3'
services:
  kafka:
    image: apache/kafka:4.0.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://kafka:9092,CONTROLLER://kafka:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://10.0.1.87:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LOG_CLEANUP_POLICY: delete
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'

# Start Kafka
docker-compose up -d
```

**Step 2: Create Kafka Topics**

```bash
# Access Kafka container
docker exec kafka bash

# Create otp-topic
bin/kafka-topics.sh \
  --create \
  --topic otp-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Create auth-events topic
bin/kafka-topics.sh \
  --create \
  --topic auth-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Step 3: Configure Auth Service Kafka Producer**

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: 10.0.1.87:9092
    producer:
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

# Java code
@Service
public class OTPEventProducer {
  @Autowired
  private KafkaTemplate<String, VerificationOtpEvent> kafkaTemplate;
  
  public void publishOTPEvent(String email, String otp) {
    VerificationOtpEvent event = new VerificationOtpEvent();
    event.setEmail(email);
    event.setOtp(otp);
    event.setTimestamp(System.currentTimeMillis());
    
    kafkaTemplate.send("otp-topic", email, event);
  }
}

# In registration endpoint
@PostMapping("/register")
public ResponseEntity<?> register(@RequestBody RegisterRequest request) {
  // ... validation and user creation ...
  
  String otp = generateOTP();
  
  // Publish to Kafka
  otpEventProducer.publishOTPEvent(request.getEmail(), otp);
  
  return ResponseEntity.status(HttpStatus.CREATED).body(new RegistrationResponse(...));
}
```

**Step 4: Verify Producer Works**

```bash
# Subscribe to topic to see messages
docker exec kafka bin/kafka-console-consumer.sh \
  --topic otp-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Make registration request
curl -X POST http://auth-service/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Pass@123"}'

# Should see OTP event appear in consumer output:
# {"email":"test@example.com","otp":"123456","timestamp":1693478400000}
```

---

## Key Lessons Learned

### Lesson 1: 504 Doesn't Mean Operation Failed

```
Misconception:
├─ 504 response
└─ Request failed, user not created

Reality:
├─ 504 response from ALB timeout
├─ BUT user was likely already created in database
├─ AND OTP was generated
├─ AND event was published (or hanging on Kafka)
└─ Retry could create duplicate user

Implication:
├─ Check database for data even after 504
├─ Consider idempotency (generate same OTP for same email)
└─ Don't blindly retry without checking state
```

### Lesson 2: ALB Health Checks ≠ Business Logic Health

```
Health Check: /actuator/health
├─ Tests if application started
├─ Tests database connectivity
├─ Returns 200 OK if basic functions work
└─ Good for infrastructure health

Business Logic Problem:
├─ Kafka topic missing
├─ Causes timeout
├─ But health check still passes
└─ Application "healthy" but functionally broken

Implication:
├─ Health checks are not comprehensive
├─ Check application logs for actual errors
├─ Monitor business metrics separately
└─ ALB thinks everything is fine, but users get timeouts
```

### Lesson 3: Service Connect is for Internal Communication

```
ALB (External Traffic):
├─ Handles traffic from internet
├─ Routes to ECS tasks
└─ Example: auth-alb → Auth Service tasks

Service Connect (Internal Traffic):
├─ Handles service-to-service communication
├─ Auth Service → Eureka Server
├─ Auth Service → Config Server
└─ DNS-based discovery

Don't Confuse:
├─ ALB is NOT for internal communication
├─ Service Connect is NOT for external traffic
└─ Both needed, different purposes
```

### Lesson 4: Kafka Doesn't Need Consumers to Store Events

```
Misconception:
├─ If Notification Service not deployed
├─ Kafka won't accept messages
├─ Or messages are lost

Reality:
├─ Kafka stores events regardless of consumers
├─ Consumer just reads what's there
├─ If Notification Service deployed later, it reads past events
└─ Kafka persists based on retention policy

Implication:
├─ Deploy producer first
├─ Events queue up waiting for consumer
├─ Deploy consumer later, it processes queued events
└─ Good for phased deployments
```

### Lesson 5: Infrastructure Layers Are Separate

```
ECS Task Status:
├─ Task RUNNING
└─ ✅ Task is up

Target Health:
├─ Target HEALTHY
└─ ✅ ALB can reach task

Health Endpoint:
├─ /actuator/health returns 200
└─ ✅ Application responding

Business Operation:
├─ Registration call times out
└─ ❌ Something in business logic hanging

Each layer must be verified separately:
├─ Don't assume one layer is fine
├─ Check all layers when troubleshooting
└─ Problem often deeper than it appears
```

### Lesson 6: Infrastructure Dependencies Must Exist First

```
Deployment Order Matters:
1. Infrastructure (VPC, RDS, Kafka, Redis) ← Must be first
2. Config Server ← Services need config
3. Eureka ← Services need discovery
4. Application Services ← Depend on above

If Services Deployed First:
├─ Config Server not running
├─ Can't fetch configuration
├─ Service startup fails
└─ Application never registers with Eureka

If Eureka Not Ready:
├─ Services can't register
├─ Other services can't discover them
└─ Service-to-service calls fail

Implication:
├─ Plan deployment sequence carefully
├─ Build scripts to automate order
└─ Document dependencies
```

---

## Summary and Next Steps

### Current Status

```
✅ COMPLETE:
├─ AWS Infrastructure (VPC, Subnets, Security Groups)
├─ Amazon ECR (Container registry)
├─ Amazon ECS & Fargate (Container orchestration)
├─ Config Server (Configuration management)
├─ Eureka Server (Service discovery)
├─ Auth Service (Authentication)
├─ Application Load Balancer
├─ ECS Service Connect (Internal networking)
├─ Kafka (Event streaming)
└─ OTP event publishing

❌ PENDING:
├─ Notification Service (Email/SMS)
├─ OTP email delivery
├─ Email verification flow
├─ User activation
├─ Account Service
├─ User Service
├─ Transaction Service
└─ API Gateway
```

### Next Phase: Notification Service

```
Notification Service will:
1. Consume VerificationOtpEvent from Kafka
2. Configure email provider (SendGrid, AWS SES, etc.)
3. Send OTP to user's email
4. Complete email verification flow

Deployment Steps:
├─ Build Notification Service Spring Boot app
├─ Add Kafka consumer configuration
├─ Add email provider integration
├─ Test locally
├─ Build Docker image
├─ Push to ECR
├─ Create ECS task definition
├─ Create ECS service
├─ Enable Service Connect
├─ Deploy
└─ Test end-to-end flow
```

---

## Interview-Ready Explanations

### "Walk me through your CitiCore deployment"

**Response:**

"I'm building a microservices banking platform on AWS. The architecture is:

1. **Infrastructure**: I created a VPC with public subnets for the load balancer, private subnets for application services, and database subnets for RDS.

2. **Container Orchestration**: I use ECS with Fargate for serverless container management. This means I just specify how many tasks to run and Fargate manages the servers.

3. **Configuration Management**: Config Server centralizes configuration in a GitHub repository. Services fetch config at startup instead of hardcoding values.

4. **Service Discovery**: Eureka is the service registry. Services register with Eureka, and other services discover them dynamically. This solves the problem of Fargate tasks getting new IPs when they restart.

5. **Load Balancing**: ALB distributes external traffic across Auth Service tasks. If a task crashes, ALB automatically routes traffic to healthy ones.

6. **Internal Communication**: Service Connect provides DNS-based discovery for service-to-service communication (Auth → Eureka, Auth → Config Server).

7. **Event Streaming**: Kafka decouples services. Auth Service publishes OTP events to Kafka, and the Notification Service consumes them to send emails.

The deployment order was intentional: infrastructure first, then Config Server, then Eureka, then Auth Service. Each service depends on previous layers.

I encountered issues like a 504 timeout caused by a missing Kafka topic, and UnknownHostException because Service Connect wasn't enabled on Eureka. Troubleshooting involved checking each layer: ECS task status, ALB target health, application logs, and external dependencies."

---
