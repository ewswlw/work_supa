## First Iteration Of What Questions Need To Be Answered Based on the Parquet Files

📋 DATA ANALYSIS SUMMARY
Your Bond Trading Database Contains:
Universe: 16,503 securities (main reference table)
Runs: 91,633 trading transactions
Portfolio: 1,938 holdings records
G_Spread: 2.37M historical spread data points (1,102+ securities)
🔍 DETAILED CLARIFYING QUESTIONS FOR DATABASE DESIGN
1. BUSINESS RULES & VALIDATION
Primary Keys: Should CUSIP be the sole primary key for Universe, or do you need composite keys for any tables?
Data Frequency: How often is each dataset updated? (daily, real-time, weekly?)
Historical Retention: How long do you need to keep historical data for compliance/analysis?
Audit Requirements: Do you need to track all changes (who, when, old/new values) for regulatory compliance?
2. UNIVERSE TABLE (Security Master)
CUSIP vs Security mapping: I see G_Spread uses 'Security' instead of CUSIP. Should I create a mapping table, or can we standardize on one identifier?
Static vs Dynamic data: Which fields in Universe change over time vs remain static?
Benchmark relationships: How should benchmark securities be linked? (separate table or foreign key?)
3. PORTFOLIO TABLE (Holdings)
Account structure: I see multiple account types (BMO - Credit Opp, TD FIAF, etc.). Should accounts be a separate table?
Position tracking: Do you need position-level audit trails for regulatory reporting?
Valuation frequency: Are portfolio valuations daily, or multiple times per day?
4. RUNS TABLE (Trading)
Trade lifecycle: Do you need to track trade statuses (pending, executed, settled, cancelled)?
Transaction types: What types of transactions should be tracked beyond basic buys/sells?
Counterparty data: Should counterparties be a separate table for compliance tracking?
5. G_SPREAD TABLE (Market Data)
Data volume: With 2.37M records, should this be partitioned by date ranges for performance?
Real-time updates: Is this market data updated in real-time or batched?
Data quality: How should we handle missing/stale spread data?
6. PERFORMANCE & SCALABILITY
Query patterns: What are your most common queries? (portfolio views, P&L, risk analysis?)
Reporting requirements: Do you need materialized views for common reports?
Concurrent users: How many users will access the database simultaneously?
7. COMPLIANCE & SECURITY
Regulatory requirements: Any specific compliance requirements (CFTC, SEC, IIROC)?
Data classification: Are there different security levels for different data types?
Backup/Recovery: What are your RTO/RPO requirements for disaster recovery?
8. INTEGRATION POINTS
Data sources: Where does this data come from? (Bloomberg, internal systems, manual entry?)
External systems: What systems need to read from/write to this database?
Real-time feeds: Do you need real-time market data integration?
🎯 NEXT STEPS
Once you answer these questions, I'll create:
📊 Detailed Database Schema - Complete table definitions with constraints
🔗 Relationship Diagrams - Visual representation of all table relationships
⚡ Performance Optimization Plan - Indexing and partitioning strategies
🛡️ Security & Compliance Framework - Audit trails and access controls
📝 Migration Scripts - Step-by-step Supabase implementation
🧪 Data Validation Rules - Business rule enforcement
📋 Operational Procedures - Backup, monitoring, and maintenance
Please answer the questions above in order of priority - I'll start designing the most critical components first and iterate based on your feedback.