-- Enable extensions
-- Note: pgvector and uuid-ossp require custom-compiled PostgreSQL image
-- For Alpine images, we skip these extensions
-- CREATE EXTENSION IF NOT EXISTS pgvector;
-- CREATE EXTENSION IF NOT EXISTS uuid-ossp;

-- Create stock_news table
CREATE TABLE IF NOT EXISTS stock_news (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    source VARCHAR(50),
    published_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    version INT DEFAULT 0,
    
    CONSTRAINT ticker_length CHECK (length(ticker) >= 1 AND length(ticker) <= 10)
);

-- Create index on ticker for faster lookups
CREATE INDEX idx_stock_news_ticker ON stock_news(ticker);
CREATE INDEX idx_stock_news_published_at ON stock_news(published_at DESC);
CREATE INDEX idx_stock_news_updated_at ON stock_news(updated_at DESC);

-- Add pgvector column for embeddings (optional backup)
-- Commented out: pgvector not available in Alpine PostgreSQL
-- Uncomment these lines if using a pgvector-enabled PostgreSQL image
-- ALTER TABLE stock_news ADD COLUMN IF NOT EXISTS embedding vector(1536);
-- CREATE INDEX IF NOT EXISTS idx_stock_news_embedding ON stock_news USING ivfflat (embedding vector_cosine_ops);

-- Enable logical replication for Debezium
ALTER TABLE stock_news REPLICA IDENTITY FULL;

-- Seed data: 15 realistic stock news items
INSERT INTO stock_news (ticker, title, content, source, published_at, updated_at) VALUES
('TSLA', 'Tesla Q1 2026 Deliveries Exceed Expectations', 
 'Tesla announced record quarterly deliveries of 1.8M vehicles in Q1 2026, driven by strong international demand and new Model 2 production ramp. CEO Elon Musk indicated plans to accelerate Gigafactory Berlin expansion. Stock rose 5.2% in after-hours trading.',
 'Reuters', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'),

('AAPL', 'Apple Announces Breakthrough in Neural Processing Chip', 
 'Apple revealed its latest neural processing architecture, promising 3x faster AI inference for on-device machine learning. The new chip will be integrated into iPhone 17 and MacBook Pro M4 Pro variants. Analysts predict significant competitive advantage vs. competitors.',
 'Bloomberg', NOW() - INTERVAL '4 hours', NOW() - INTERVAL '4 hours'),

('MSFT', 'Microsoft Expands Azure AI Services with New Foundation Models',
 'Microsoft announced five new foundation models exclusively for Azure customers, including specialized models for financial services and healthcare. Partnership with OpenAI deepens with new commercial licensing agreements. Enterprise cloud deployments expected to accelerate.',
 'TechCrunch', NOW() - INTERVAL '6 hours', NOW() - INTERVAL '6 hours'),

('GOOGL', 'Google Quantum Computer Achieves Error-Correction Milestone',
 'Google''s Willow quantum chip successfully demonstrated quantum error correction, reducing errors exponentially as more qubits are added. Industry experts call this a breakthrough moment. Potential applications in drug discovery and materials science within 5-10 years.',
 'MIT Tech Review', NOW() - INTERVAL '8 hours', NOW() - INTERVAL '8 hours'),

('META', 'Meta Unveils Next-Generation VR Headset with 8K Resolution',
 'Meta announced Project Aria 2, featuring dual 8K displays, eye-tracking, and full-body haptics. Preorders begin April 1st with $3,299 price tag. Company targets 50M active VR users by 2027 through major gaming partnerships and enterprise adoption programs.',
 'VentureBeat', NOW() - INTERVAL '10 hours', NOW() - INTERVAL '10 hours'),

('NVDA', 'NVIDIA Reports Record Data Center Revenue in Q1 2026',
 'NVIDIA beat earnings expectations with $28.7B data center revenue, up 40% YoY. Strong demand for H200 chips and blackwell architecture drives growth. Forward guidance suggests continued momentum in AI infrastructure buildout globally.',
 'Seeking Alpha', NOW() - INTERVAL '12 hours', NOW() - INTERVAL '12 hours'),

('AMZN', 'Amazon Web Services Launches Real-Time Vector Database Service',
 'AWS introduced Amazon Vector, a fully managed vector database service for RAG and semantic search workloads. Competitive pricing vs. Pinecone and Weaviate. Integration with Bedrock AI services provides turnkey solution for enterprises.',
 'AWS Blog', NOW() - INTERVAL '14 hours', NOW() - INTERVAL '14 hours'),

('NFLX', 'Netflix Reaches 300M Subscribers, Plans Gaming Expansion',
 'Netflix reported adding 15M subscribers in Q1 2026, reaching 300M milestone. Company announced $2B investment in gaming platform with exclusive titles. Advertising tier now represents 35% of new signups.',
 'Variety', NOW() - INTERVAL '16 hours', NOW() - INTERVAL '16 hours'),

('SNOW', 'Snowflake Announces Cortex AI with Native LLM Integration',
 'Snowflake launched Cortex, enabling enterprises to build AI applications directly on their data warehouse without moving data. Early customers report 60% reduction in time-to-insight. Stock rallied 8.3% on announcement.',
 'DataEng Weekly', NOW() - INTERVAL '18 hours', NOW() - INTERVAL '18 hours'),

('PYPL', 'PayPal Acquires AI Fintech Startup for $4.2B',
 'PayPal announced acquisition of synthetic intelligence payment platform SyntheticAI. Deal aims to enhance fraud detection and personalized payment recommendations. Expected close Q3 2026, accretive to earnings by 2027.',
 'CNBC', NOW() - INTERVAL '20 hours', NOW() - INTERVAL '20 hours'),

('MSTR', 'Microstrategy Q1 Earnings Beat, AI Product Adoption Up 300%',
 'Microstrategy reported 300% increase in AI product adoption in Q1 2026. Enterprise customers expanding use of Dossier AI for self-service analytics. Management raises full-year guidance on strong pipeline.',
 'InvestorPlace', NOW() - INTERVAL '22 hours', NOW() - INTERVAL '22 hours'),

('CRM', 'Salesforce Integrates Einstein AI Deeper Across Platform',
 'Salesforce announced native AI co-pilot integration in all core modules (Sales Cloud, Service Cloud, Commerce). Customers report 25% productivity gains in pilot programs. Marketing spend on AI features increased 40% YoY.',
 'Forbes', NOW() - INTERVAL '24 hours', NOW() - INTERVAL '24 hours'),

('ADBE', 'Adobe Announces AI-Powered Content Generation in Creative Suite',
 'Adobe unveiled Firefly AI integrated across Photoshop, Illustrator, and Premiere Pro. Content creators can generate backgrounds, extend images, and edit videos with natural language prompts. Subscription revenue expected to accelerate.',
 'MacRumors', NOW() - INTERVAL '26 hours', NOW() - INTERVAL '26 hours'),

('IBM', 'IBM Unveils Quantum-Classical Hybrid Computing Platform',
 'IBM released new quantum computing platform combining quantum processors with classical supercomputers. Early partners in pharmaceuticals and optimization report significant speedups. Cloud access available through IBM Quantum Network.',
 'HPC Wire', NOW() - INTERVAL '28 hours', NOW() - INTERVAL '28 hours'),

('PLTR', 'Palantir Wins $850M US Military AI Contract',
 'Palantir secured $850M multi-year contract to build AI targeting system for US Department of Defense. Represents significant validation for commercial AI applications in defense sector. Stock surged 12% on announcement.',
 'Defense News', NOW() - INTERVAL '30 hours', NOW() - INTERVAL '30 hours');

-- Verify data inserted
SELECT COUNT(*) as row_count FROM stock_news;
