import json
import os
import glob
import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

KAFKA_BOOTSTRAP      = "localhost:9092"
SCHEMA_REGISTRY_URL  = "http://localhost:8081"
KAFKA_UI_URL         = "http://localhost:8080"
OUTPUT_DIR           = "D:/clickstream-pipeline/dags/output"

st.set_page_config(
    page_title="Clickstream Pipeline",
    page_icon="📊",
    layout="wide"
)

# ── sidebar navigation ──────────────────────────────────────────────────────
page = st.sidebar.selectbox(
    "Navigate",
    ["Live Event Feed", "Funnel Analysis", "Session Insights", "Pipeline Health"]
)
st.sidebar.markdown("---")
st.sidebar.markdown("**Clickstream Pipeline**")
st.sidebar.markdown("Real-time e-commerce analytics")
st.sidebar.markdown("Built with Kafka · Spark · Airflow")


# ── helpers ──────────────────────────────────────────────────────────────────
def get_kafka_topics():
    """Fetch topic stats from Kafka UI REST API."""
    try:
        r = requests.get(f"{KAFKA_UI_URL}/api/clusters/local/topics", timeout=3)
        if r.status_code == 200:
            return r.json().get("topics", [])
    except Exception:
        pass
    return []


def get_schema_registry_subjects():
    """Fetch registered schemas from Schema Registry."""
    try:
        r = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=3)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return []


def load_latest_output(prefix):
    """Load the most recent JSON output file matching prefix."""
    pattern = f"{OUTPUT_DIR}/{prefix}_*.json"
    files   = sorted(glob.glob(pattern), reverse=True)
    if not files:
        return None
    with open(files[0]) as f:
        return json.load(f)


def load_all_funnels():
    """Load all funnel files for trend analysis."""
    files = sorted(glob.glob(f"{OUTPUT_DIR}/funnel_*.json"))
    funnels = []
    for f in files:
        with open(f) as fh:
            data = json.load(fh)
            funnels.append(data)
    return funnels


# ── Page 1: Live Event Feed ───────────────────────────────────────────────────
if page == "Live Event Feed":
    st.title("📡 Live Event Feed")
    st.markdown("Real-time message counts from Kafka topics")

    topics = get_kafka_topics()

    clickstream_topics = [
        t for t in topics
        if t.get("name", "").startswith("clickstream")
    ]

    if not clickstream_topics:
        st.warning("Could not reach Kafka UI. Make sure Docker is running at localhost:8080.")
    else:
        # metric cards row
        cols = st.columns(len(clickstream_topics))
        for i, topic in enumerate(clickstream_topics):
            name     = topic.get("name", "").replace("clickstream.", "")
            msg_count = topic.get("messagesPerSec", 0) or topic.get("segmentsCount", 0)
            size     = topic.get("segmentsSize", 0)
            with cols[i]:
                st.metric(
                    label=name.upper(),
                    value=f"{topic.get('messagesCount', 0):,}",
                    delta="messages total"
                )

        st.markdown("---")

        # bar chart of message counts per topic
        names  = [t.get("name", "").replace("clickstream.", "") for t in clickstream_topics]
        counts = [t.get("messagesCount", 0) for t in clickstream_topics]

        fig = go.Figure(go.Bar(
            x=names,
            y=counts,
            marker_color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"],
            text=counts,
            textposition="auto",
        ))
        fig.update_layout(
            title="Total Messages per Topic",
            xaxis_title="Topic",
            yaxis_title="Message Count",
            height=400,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

        # expected funnel ratios
        st.markdown("### Expected Event Ratios")
        st.markdown("Based on simulator weights: 70% pageviews · 15% clicks · 10% cart · 4% purchases · 1% sessions")

        total = sum(counts) or 1
        ratios = [round(c / total * 100, 1) for c in counts]

        fig2 = go.Figure(go.Bar(
            x=names,
            y=ratios,
            marker_color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"],
            text=[f"{r}%" for r in ratios],
            textposition="auto",
        ))
        fig2.update_layout(
            title="Actual Event Type Distribution (%)",
            xaxis_title="Event Type",
            yaxis_title="% of Total",
            height=350,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig2, use_container_width=True)


# ── Page 2: Funnel Analysis ───────────────────────────────────────────────────
elif page == "Funnel Analysis":
    st.title("🔽 Funnel Analysis")
    st.markdown("Conversion funnel from the hourly batch DAG")

    funnel = load_latest_output("funnel")

    if not funnel:
        st.warning(f"No funnel data found in {OUTPUT_DIR}. Run `python dags/run_hourly_summary.py` first.")
    else:
        st.caption(f"Last computed: {funnel.get('computed_at', 'unknown')}")

        # funnel metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Page Views",     f"{funnel['page_views']:,}")
        col2.metric("Product Clicks", f"{funnel['product_clicks']:,}")
        col3.metric("Cart Adds",      f"{funnel['cart_adds']:,}")
        col4.metric("Purchases",      f"{funnel['purchases']:,}")

        st.markdown("---")

        # funnel chart
        stages  = ["Page Views", "Product Clicks", "Cart Adds", "Purchases"]
        values  = [
            funnel["page_views"],
            funnel["product_clicks"],
            funnel["cart_adds"],
            funnel["purchases"],
        ]

        fig = go.Figure(go.Funnel(
            y=stages,
            x=values,
            textinfo="value+percent initial",
            marker_color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"],
        ))
        fig.update_layout(
            title="Conversion Funnel",
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

        # drop-off rates
        st.markdown("### Drop-off Rates")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("View → Click",    f"{funnel['view_to_click_rate']}%")
        col2.metric("Click → Cart",    f"{funnel['click_to_cart_rate']}%")
        col3.metric("Cart → Purchase", f"{funnel['cart_to_purchase_rate']}%")
        col4.metric("Overall Conv.",   f"{funnel['overall_conversion']}%")

        # trend if multiple runs exist
        all_funnels = load_all_funnels()
        if len(all_funnels) > 1:
            st.markdown("### Conversion Trend Across Runs")
            times       = [f["computed_at"][:19] for f in all_funnels]
            conversions = [f["overall_conversion"] for f in all_funnels]

            fig2 = px.line(
                x=times, y=conversions,
                labels={"x": "Run Time", "y": "Overall Conversion %"},
                title="Overall Conversion Rate Over Time",
                markers=True,
            )
            st.plotly_chart(fig2, use_container_width=True)


# ── Page 3: Session Insights ──────────────────────────────────────────────────
elif page == "Session Insights":
    st.title("👤 Session Insights")
    st.markdown("Session-level metrics from the batch pipeline")

    sessions = load_latest_output("sessions")

    if not sessions:
        st.warning(f"No session data found in {OUTPUT_DIR}. Run `python dags/run_hourly_summary.py` first.")
    else:
        total      = len(sessions)
        converted  = sum(1 for s in sessions if s["converted"])
        avg_dur    = sum(s["duration_seconds"] for s in sessions) / total
        avg_pages  = sum(s["page_views"] for s in sessions) / total
        avg_spend  = sum(s["total_spend"] for s in sessions if s["converted"]) / max(converted, 1)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Sessions",   total)
        col2.metric("Converted",        converted)
        col3.metric("Conv. Rate",       f"{round(converted/total*100, 1)}%")
        col4.metric("Avg Duration",     f"{round(avg_dur, 0)}s")
        col5.metric("Avg Order Value",  f"${round(avg_spend, 2)}")

        st.markdown("---")

        # duration distribution
        durations = [s["duration_seconds"] for s in sessions]
        fig = px.histogram(
            x=durations,
            nbins=20,
            title="Session Duration Distribution (seconds)",
            labels={"x": "Duration (s)", "y": "Sessions"},
            color_discrete_sequence=["#1f77b4"],
        )
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        # converted vs not
        col1, col2 = st.columns(2)
        with col1:
            fig2 = go.Figure(go.Pie(
                labels=["Converted", "Not Converted"],
                values=[converted, total - converted],
                marker_colors=["#2ca02c", "#d62728"],
                hole=0.4,
            ))
            fig2.update_layout(title="Session Conversion Split", height=350)
            st.plotly_chart(fig2, use_container_width=True)

        with col2:
            # page views per session distribution
            page_views = [s["page_views"] for s in sessions]
            fig3 = px.histogram(
                x=page_views,
                nbins=15,
                title="Page Views per Session",
                labels={"x": "Page Views", "y": "Sessions"},
                color_discrete_sequence=["#ff7f0e"],
            )
            fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3, use_container_width=True)


# ── Page 4: Pipeline Health ───────────────────────────────────────────────────
elif page == "Pipeline Health":
    st.title("🏥 Pipeline Health")
    st.markdown("Infrastructure status across all pipeline components")

    col1, col2, col3 = st.columns(3)

    # Kafka UI health
    with col1:
        st.markdown("### Kafka Broker")
        try:
            r = requests.get(f"{KAFKA_UI_URL}/api/clusters/local/brokers", timeout=3)
            if r.status_code == 200:
                brokers = r.json()
                st.success(f"✅ Online — {len(brokers)} broker(s)")
                for b in brokers:
                    st.write(f"Broker {b.get('id')}: {b.get('host')}:{b.get('port')}")
            else:
                st.error("❌ Kafka UI unreachable")
        except Exception as e:
            st.error(f"❌ {e}")

    # Schema Registry health
    with col2:
        st.markdown("### Schema Registry")
        subjects = get_schema_registry_subjects()
        if subjects:
            st.success(f"✅ Online — {len(subjects)} schemas")
            for s in sorted(subjects):
                st.write(f"• {s}")
        else:
            st.error("❌ Schema Registry unreachable")

    # Output files health
    with col3:
        st.markdown("### Batch Pipeline")
        funnel_files  = glob.glob(f"{OUTPUT_DIR}/funnel_*.json")
        session_files = glob.glob(f"{OUTPUT_DIR}/sessions_*.json")
        if funnel_files:
            latest = sorted(funnel_files)[-1]
            ts     = os.path.basename(latest).replace("funnel_", "").replace(".json", "")
            st.success(f"✅ {len(funnel_files)} run(s) complete")
            st.write(f"Latest: {ts}")
            st.write(f"Session files: {len(session_files)}")
        else:
            st.warning("⚠️ No batch runs found yet")
            st.write(f"Run: `python dags/run_hourly_summary.py`")

    st.markdown("---")

    # topic detail table
    st.markdown("### Kafka Topics Detail")
    topics = get_kafka_topics()
    clickstream = [t for t in topics if t.get("name", "").startswith("clickstream")]

    if clickstream:
        rows = []
        for t in clickstream:
            rows.append({
                "Topic":      t.get("name", ""),
                "Partitions": t.get("partitionsCount", 1),
                "Messages":   f"{t.get('messagesCount', 0):,}",
                "Replicas":   t.get("replicationFactor", 1),
            })
        st.table(rows)
    else:
        st.warning("No clickstream topics found.")