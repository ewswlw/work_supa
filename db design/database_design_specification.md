# 🏗️ Bond Trading Database Design Specification
## Supabase Production-Ready Implementation

---

## 📋 **PROJECT OVERVIEW**

**Database Type**: Bond Trading & Portfolio Management  
**Platform**: Supabase (PostgreSQL)  
**Tier**: Free Tier Optimized  
**Data Volume**: ~2.4M+ records across 4 main tables  
**Update Frequency**: Manual daily uploads (irregular)  

---

## 🗂️ **TABLE STRUCTURE DESIGN**

### **1. 📊 UNIVERSE (Security Master Data)**
```sql
CREATE TABLE universe (
    -- Composite Primary Key
    date DATE NOT NULL,
    cusip VARCHAR(20) NOT NULL,
    
    -- Security Identifiers
    security VARCHAR(100),
    benchmark_cusip VARCHAR(20),
    bloomberg_cusip VARCHAR(20),
    isin VARCHAR(20),
    
    -- Security Details
    security_type VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'CAD',
    maturity_date DATE,
    coupon DECIMAL(10,6),
    credit_rating VARCHAR(10),
    
    -- Issuer Information
    company_symbol VARCHAR(20),
    issuer_name VARCHAR(100),
    
    -- Benchmark Data
    benchmark_maturity_date DATE,
    
    -- Financial Metrics (stored as integers to avoid floating point issues)
    face_value_outstanding BIGINT, -- stored in cents
    pct_of_outstanding DECIMAL(10,8),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    PRIMARY KEY (date, cusip),
    CHECK (coupon >= 0),
    CHECK (face_value_outstanding >= 0)
);
```

### **2. 🔗 SECURITY_MAPPING (G_Spread to CUSIP Bridge)**
```sql
CREATE TABLE security_mapping (
    id SERIAL PRIMARY KEY,
    security_name VARCHAR(100) NOT NULL UNIQUE,
    cusip VARCHAR(20),
    
    -- Additional mapping fields
    security_type VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign Key (soft reference - may not always exist)
    FOREIGN KEY (cusip) REFERENCES universe(cusip) ON DELETE SET NULL
);
```

### **3. 📈 PORTFOLIO (Holdings Data)**
```sql
CREATE TABLE portfolio (
    -- Composite Primary Key
    date DATE NOT NULL,
    cusip VARCHAR(20) NOT NULL,
    account VARCHAR(100) NOT NULL,
    portfolio VARCHAR(100) NOT NULL,
    
    -- Security Details
    security VARCHAR(100),
    security_type VARCHAR(50),
    underlying_security VARCHAR(100),
    underlying_cusip VARCHAR(20),
    
    -- Position Data
    quantity DECIMAL(18,4),
    face_value BIGINT, -- stored in cents
    price DECIMAL(12,6),
    value_pct_nav DECIMAL(10,8),
    
    -- Duration & Risk Metrics
    modified_duration DECIMAL(10,6),
    position_cr01 DECIMAL(12,6),
    position_pvbp DECIMAL(12,6),
    security_pvbp DECIMAL(12,6),
    security_cr01 DECIMAL(12,6),
    
    -- Yield & Spread Data
    yield_to_mat DECIMAL(10,6),
    yield_calc DECIMAL(10,6),
    benchmark_yield DECIMAL(10,6),
    oas_bloomberg DECIMAL(10,6),
    spread_calculated DECIMAL(10,6),
    
    -- Classification
    maturity_bucket VARCHAR(50),
    trade_group VARCHAR(50),
    strategy VARCHAR(50),
    security_classification VARCHAR(50),
    funding_status VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    PRIMARY KEY (date, cusip, account, portfolio),
    FOREIGN KEY (cusip) REFERENCES universe(cusip) ON DELETE CASCADE,
    CHECK (quantity IS NULL OR quantity >= 0),
    CHECK (price IS NULL OR price >= 0)
);
```

### **4. 🔄 RUNS (Trading/Transaction Data)**
```sql
CREATE TABLE runs (
    -- Composite Primary Key
    date DATE NOT NULL,
    cusip VARCHAR(20) NOT NULL,
    dealer VARCHAR(100) NOT NULL,
    
    -- Security Reference
    security VARCHAR(100),
    reference_security VARCHAR(100),
    
    -- Trade Details
    trade_type VARCHAR(50),
    quantity DECIMAL(18,4),
    price DECIMAL(12,6),
    yield_value DECIMAL(10,6),
    
    -- Spread & Risk Metrics
    spread DECIMAL(10,6),
    duration DECIMAL(10,6),
    dv01 DECIMAL(12,6),
    
    -- Settlement
    settlement_date DATE,
    trade_date DATE,
    
    -- Additional Fields (based on actual data structure)
    benchmark VARCHAR(100),
    maturity_date DATE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    PRIMARY KEY (date, cusip, dealer),
    FOREIGN KEY (cusip) REFERENCES universe(cusip) ON DELETE CASCADE,
    CHECK (quantity IS NULL OR quantity >= 0),
    CHECK (price IS NULL OR price >= 0)
);
```

### **5. 📊 G_SPREAD (Historical Spread Data) - PARTITIONED**
```sql
-- Create parent table
CREATE TABLE g_spread (
    date DATE NOT NULL,
    security VARCHAR(100) NOT NULL,
    
    -- Spread Data (stored as decimal for precision)
    g_spread DECIMAL(10,6),
    spread_change DECIMAL(10,6),
    
    -- Volume & Market Data
    volume DECIMAL(18,4),
    price DECIMAL(12,6),
    yield_value DECIMAL(10,6),
    
    -- Reference Data
    duration DECIMAL(10,6),
    credit_rating VARCHAR(10),
    maturity_date DATE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    PRIMARY KEY (date, security),
    FOREIGN KEY (security) REFERENCES security_mapping(security_name) ON DELETE CASCADE
) PARTITION BY RANGE (date);

-- Create yearly partitions for storage efficiency
CREATE TABLE g_spread_2023 PARTITION OF g_spread
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE g_spread_2024 PARTITION OF g_spread
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE g_spread_2025 PARTITION OF g_spread
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Future partitions can be added as needed
```

---

## 🚀 **INDEXES FOR PERFORMANCE**

```sql
-- Universe Indexes
CREATE INDEX idx_universe_cusip ON universe(cusip);
CREATE INDEX idx_universe_date ON universe(date);
CREATE INDEX idx_universe_security_type ON universe(security_type);
CREATE INDEX idx_universe_maturity ON universe(maturity_date);

-- Portfolio Indexes
CREATE INDEX idx_portfolio_date ON portfolio(date);
CREATE INDEX idx_portfolio_account ON portfolio(account);
CREATE INDEX idx_portfolio_strategy ON portfolio(strategy);
CREATE INDEX idx_portfolio_value ON portfolio(face_value) WHERE face_value IS NOT NULL;

-- Runs Indexes
CREATE INDEX idx_runs_date ON runs(date);
CREATE INDEX idx_runs_security ON runs(security);
CREATE INDEX idx_runs_trade_date ON runs(trade_date);

-- G_Spread Indexes (on each partition)
CREATE INDEX idx_g_spread_2023_security ON g_spread_2023(security);
CREATE INDEX idx_g_spread_2024_security ON g_spread_2024(security);
CREATE INDEX idx_g_spread_2025_security ON g_spread_2025(security);

-- Security Mapping Indexes
CREATE INDEX idx_security_mapping_cusip ON security_mapping(cusip);
CREATE UNIQUE INDEX idx_security_mapping_name ON security_mapping(security_name);
```

---

## 🔧 **DATABASE CONSTRAINTS & VALIDATION**

```sql
-- Add data validation constraints
ALTER TABLE universe ADD CONSTRAINT chk_universe_date_reasonable 
    CHECK (date >= '2020-01-01' AND date <= CURRENT_DATE + INTERVAL '1 year');

ALTER TABLE portfolio ADD CONSTRAINT chk_portfolio_nav_percent 
    CHECK (value_pct_nav IS NULL OR (value_pct_nav >= -2.0 AND value_pct_nav <= 2.0));

ALTER TABLE runs ADD CONSTRAINT chk_runs_settlement_after_trade 
    CHECK (settlement_date IS NULL OR trade_date IS NULL OR settlement_date >= trade_date);

-- Add currency validation
ALTER TABLE universe ADD CONSTRAINT chk_universe_currency 
    CHECK (currency IN ('CAD', 'USD', 'EUR', 'GBP'));
```

---

## 📦 **STORAGE OPTIMIZATION STRATEGIES**

### **1. Data Types Optimization**
- **Monetary Values**: BIGINT (cents) for exact precision, DECIMAL for percentages
- **Text Fields**: VARCHAR with appropriate limits vs TEXT for large fields
- **Dates**: DATE vs TIMESTAMP based on precision needs

### **2. Compression Settings**
```sql
-- Enable compression for large tables
ALTER TABLE g_spread SET (toast_tuple_target = 128);
ALTER TABLE portfolio SET (toast_tuple_target = 128);
```

### **3. Partition Management**
```sql
-- Automatic partition creation function
CREATE OR REPLACE FUNCTION create_yearly_partition(table_name TEXT, year_value INTEGER)
RETURNS VOID AS $$
DECLARE
    start_date DATE := DATE(year_value || '-01-01');
    end_date DATE := DATE((year_value + 1) || '-01-01');
    partition_name TEXT := table_name || '_' || year_value;
BEGIN
    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                   partition_name, table_name, start_date, end_date);
    EXECUTE format('CREATE INDEX idx_%s_security ON %I(security)', partition_name, partition_name);
END;
$$ LANGUAGE plpgsql;
```

---

## 🔄 **DATA UPLOAD PROCEDURES**

### **1. Security Mapping Population**
```sql
-- Populate security mapping from existing data
INSERT INTO security_mapping (security_name, cusip, security_type)
SELECT DISTINCT 
    g.security,
    u.cusip,
    u.security_type
FROM (SELECT DISTINCT security FROM g_spread_raw) g
LEFT JOIN universe u ON u.security = g.security
ON CONFLICT (security_name) DO UPDATE SET
    cusip = EXCLUDED.cusip,
    security_type = EXCLUDED.security_type,
    updated_at = NOW();
```

### **2. Batch Upload Template**
```sql
-- Template for daily data uploads
DO $$
DECLARE
    upload_date DATE := CURRENT_DATE;
BEGIN
    -- 1. Upload Universe data
    INSERT INTO universe (date, cusip, security, ...)
    SELECT upload_date, cusip, security, ...
    FROM universe_staging
    ON CONFLICT (date, cusip) DO UPDATE SET
        security = EXCLUDED.security,
        updated_at = NOW();
    
    -- 2. Upload Portfolio data
    INSERT INTO portfolio (date, cusip, account, portfolio, ...)
    SELECT upload_date, cusip, account, portfolio, ...
    FROM portfolio_staging
    ON CONFLICT (date, cusip, account, portfolio) DO UPDATE SET
        quantity = EXCLUDED.quantity,
        updated_at = NOW();
    
    -- 3. Upload Runs data
    INSERT INTO runs (date, cusip, dealer, ...)
    SELECT upload_date, cusip, dealer, ...
    FROM runs_staging
    ON CONFLICT (date, cusip, dealer) DO UPDATE SET
        quantity = EXCLUDED.quantity,
        price = EXCLUDED.price;
    
    -- 4. Upload G_Spread data
    INSERT INTO g_spread (date, security, g_spread, ...)
    SELECT upload_date, security, g_spread, ...
    FROM g_spread_staging
    ON CONFLICT (date, security) DO UPDATE SET
        g_spread = EXCLUDED.g_spread;
END $$;
```

---

## 📊 **COMMON QUERY PATTERNS**

### **1. Portfolio Summary by Date**
```sql
SELECT 
    date,
    account,
    COUNT(*) as positions,
    SUM(face_value)/100.0 as total_value_cad,
    AVG(modified_duration) as avg_duration
FROM portfolio 
WHERE date = CURRENT_DATE
GROUP BY date, account
ORDER BY total_value_cad DESC;
```

### **2. Recent Spread Changes**
```sql
SELECT 
    gs.date,
    gs.security,
    gs.g_spread,
    LAG(gs.g_spread) OVER (PARTITION BY gs.security ORDER BY gs.date) as prev_spread,
    gs.g_spread - LAG(gs.g_spread) OVER (PARTITION BY gs.security ORDER BY gs.date) as spread_change
FROM g_spread gs
WHERE gs.date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY gs.date DESC, ABS(spread_change) DESC NULLS LAST;
```

### **3. Trading Activity Summary**
```sql
SELECT 
    r.date,
    r.dealer,
    COUNT(*) as trade_count,
    SUM(r.quantity) as total_quantity,
    AVG(r.price) as avg_price
FROM runs r
WHERE r.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY r.date, r.dealer
ORDER BY r.date DESC, total_quantity DESC;
```

---

## 🛡️ **DATA QUALITY & MONITORING**

### **1. Data Quality Checks**
```sql
-- Create data quality monitoring views
CREATE VIEW data_quality_summary AS
SELECT 
    'universe' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT cusip) as unique_cusips,
    MAX(date) as latest_date,
    COUNT(*) FILTER (WHERE cusip IS NULL) as null_cusips
FROM universe
UNION ALL
SELECT 
    'portfolio' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT cusip) as unique_cusips,
    MAX(date) as latest_date,
    COUNT(*) FILTER (WHERE cusip IS NULL) as null_cusips
FROM portfolio;
```

### **2. Storage Monitoring**
```sql
-- Monitor table sizes for free tier management
CREATE VIEW table_sizes AS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY size_bytes DESC;
```

---

## 📈 **IMPLEMENTATION ROADMAP**

### **Phase 1: Core Tables (Week 1)**
1. ✅ Create Universe table
2. ✅ Create Security Mapping table  
3. ✅ Create basic indexes
4. ✅ Test data upload from current parquet files

### **Phase 2: Transaction Tables (Week 2)**
1. ✅ Create Portfolio table
2. ✅ Create Runs table
3. ✅ Implement data validation constraints
4. ✅ Test composite primary keys

### **Phase 3: Time Series Optimization (Week 3)**
1. ✅ Create partitioned G_Spread table
2. ✅ Implement partition management
3. ✅ Optimize indexes for query patterns
4. ✅ Monitor storage usage

### **Phase 4: Production Readiness (Week 4)**
1. ✅ Create monitoring views
2. ✅ Implement data quality checks
3. ✅ Document upload procedures
4. ✅ Performance testing and optimization

---

## 💰 **FREE TIER OPTIMIZATION**

**Supabase Free Tier Limits:**
- Database Size: 500MB
- Storage: 1GB  
- Bandwidth: 2GB/month

**Optimization Strategies:**
1. **Partitioning**: Reduces query costs on large tables
2. **Data Types**: Optimal storage for financial data
3. **Indexes**: Strategic indexing for common queries
4. **Compression**: Enable for text-heavy fields
5. **Archival**: Consider archiving old partitions if needed

**Estimated Storage Usage:**
- Universe: ~15MB (16K records)
- Portfolio: ~5MB (2K records)  
- Runs: ~25MB (92K records)
- G_Spread: ~180MB (2.37M records)
- **Total: ~225MB** (well within 500MB limit)

---

## 🎯 **NEXT STEPS**

1. **Review Schema**: Confirm table structures match your data
2. **Create Tables**: Run the SQL scripts in Supabase
3. **Test Upload**: Upload sample data to validate structure
4. **Optimize**: Adjust indexes based on actual query patterns
5. **Monitor**: Track storage usage and performance
