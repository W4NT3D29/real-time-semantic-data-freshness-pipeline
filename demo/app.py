"""Streamlit dashboard for real-time semantic data freshness proof."""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import streamlit as st

logger = logging.getLogger(__name__)

# Streamlit page config
st.set_page_config(
    page_title="Real-Time Semantic Data Freshness",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_db_connection():
    """Get cached database connection."""
    import os
    
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "stock_news_db"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_secret"),
    )


def get_latest_news(limit: int = 10) -> list:
    """Fetch latest stock news."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, ticker, title, content, source, published_at, updated_at, embedding IS NOT NULL as has_embedding
            FROM stock_news
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return rows
    except Exception as e:
        logger.error(f"Failed to fetch news: {e}")
        st.error(f"Database error: {e}")
        return []


def get_sync_stats() -> dict:
    """Get synchronization statistics."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM stock_news WHERE embedding IS NOT NULL")
        synced_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stock_news")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MAX(updated_at) FROM stock_news WHERE embedding IS NOT NULL")
        last_sync = cursor.fetchone()[0]
        
        cursor.close()
        
        return {
            "synced": synced_count,
            "total": total_count,
            "last_sync": last_sync,
        }
    except Exception as e:
        logger.error(f"Failed to fetch stats: {e}")
        return {"synced": 0, "total": 0, "last_sync": None}


def main():
    """Main Streamlit app."""
    st.title("📊 Real-Time Semantic Data Freshness Pipeline")
    st.markdown(
        "Watch stock news embeddings sync in real-time from PostgreSQL → Kafka → OpenAI/Ollama → Pinecone"
    )

    # Metrics row
    col1, col2, col3 = st.columns(3)
    
    stats = get_sync_stats()
    
    with col1:
        st.metric("Total News Items", stats["total"])
    
    with col2:
        if stats["total"] > 0:
            sync_pct = (stats["synced"] / stats["total"]) * 100
        else:
            sync_pct = 0
        st.metric("Embedded & Synced", f"{stats['synced']}/{stats['total']} ({sync_pct:.1f}%)")
    
    with col3:
        if stats["last_sync"]:
            now = datetime.now(stats["last_sync"].tzinfo)
            freshness_ms = int((now - stats["last_sync"]).total_seconds() * 1000)
            st.metric("Last Sync Freshness", f"{freshness_ms}ms ago", delta=f"Updated now")
        else:
            st.metric("Last Sync Freshness", "Never", delta="Waiting for sync...")

    st.divider()

    # Data table
    st.subheader("📰 Latest Stock News")
    
    news = get_latest_news(20)
    
    if news:
        # Format data for display
        data = []
        for row in news:
            id, ticker, title, content, source, pub_at, upd_at, has_embedding = row
            data.append({
                "Ticker": ticker,
                "Title": title[:60] + "..." if len(title) > 60 else title,
                "Source": source or "N/A",
                "Content": content[:50] + "..." if len(content) > 50 else content,
                "Published": pub_at.strftime("%Y-%m-%d %H:%M") if pub_at else "N/A",
                "Updated": upd_at.strftime("%Y-%m-%d %H:%M") if upd_at else "N/A",
                "Embedded": "✅" if has_embedding else "⏳",
            })
        
        st.dataframe(data, use_container_width=True, height=400)
    else:
        st.info("No news items found. Run `make insert-test` to add test data.")

    st.divider()

    # Auto-refresh
    st.subheader("⚙️ Settings")
    refresh_interval = st.slider("Refresh interval (seconds)", 1, 60, 5)
    
    # Use a placeholder for auto-refresh
    placeholder = st.empty()
    
    while True:
        with placeholder.container():
            st.info(f"Auto-refreshing in {refresh_interval}s... (Disable in sidebar)")
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
