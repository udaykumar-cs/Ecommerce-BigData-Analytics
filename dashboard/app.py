
import streamlit as st
import pandas as pd
import os
import glob

# -------------------------------------------------
# PAGE CONFIGURATION
# -------------------------------------------------

st.set_page_config(
    page_title="E-Commerce Analytics | Intelligence Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def project_path(path):
    return os.path.join(ROOT, path)


# -------------------------------------------------
# CUSTOM DESIGN
# -------------------------------------------------

st.markdown("""
<style>

/* Main application */
.stApp {
    background: #0b1120;
    color: #f1f5f9;
}

/* Main content spacing */
.block-container {
    padding-top: 2rem;
    padding-bottom: 3rem;
    max-width: 1500px;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #111c32 0%, #101827 100%);
    border-right: 1px solid #263650;
}

section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] p {
    color: #e2e8f0;
}

/* Hero banner */
.hero {
    background: linear-gradient(
        120deg,
        #142b52 0%,
        #174c78 48%,
        #087e91 100%
    );
    padding: 35px 38px;
    border-radius: 20px;
    margin-bottom: 30px;
    border: 1px solid rgba(125, 211, 252, 0.25);
    box-shadow: 0 12px 35px rgba(0, 0, 0, 0.25);
}

.hero h1 {
    color: white;
    font-size: 34px;
    font-weight: 800;
    margin: 0;
    letter-spacing: -1px;
}

.hero p {
    color: #dbeafe;
    font-size: 15px;
    margin-top: 12px;
    margin-bottom: 0;
}

.hero-tag {
    display: inline-block;
    background: rgba(255,255,255,0.13);
    color: #e0f2fe;
    padding: 6px 13px;
    border-radius: 20px;
    margin: 18px 6px 0 0;
    font-size: 12px;
    border: 1px solid rgba(255,255,255,0.15);
}

/* Section headings */
.section-heading {
    font-size: 23px;
    font-weight: 750;
    color: #f8fafc;
    margin-top: 25px;
    margin-bottom: 17px;
    padding-left: 13px;
    border-left: 4px solid #38bdf8;
}

/* KPI cards */
.kpi-card {
    background: linear-gradient(145deg, #17263d, #111c30);
    border: 1px solid #2a3d59;
    border-radius: 15px;
    padding: 20px;
    min-height: 125px;
    box-shadow: 0 5px 18px rgba(0,0,0,0.18);
    margin-bottom: 12px;
}

.kpi-card:hover {
    border-color: #38bdf8;
}

.kpi-label {
    color: #9fb3ca;
    font-size: 13px;
    font-weight: 600;
    margin-bottom: 12px;
}

.kpi-value {
    color: #f8fafc;
    font-size: 27px;
    font-weight: 800;
    letter-spacing: -0.5px;
}

.kpi-icon {
    font-size: 21px;
    float: right;
}

/* Info panels */
.info-panel {
    background: #142238;
    border: 1px solid #2a3d59;
    border-radius: 13px;
    padding: 18px 20px;
    color: #cbd5e1;
    line-height: 1.7;
    margin: 12px 0;
}

/* Dataframes */
div[data-testid="stDataFrame"] {
    border: 1px solid #263650;
    border-radius: 12px;
    overflow: hidden;
}

/* Metrics */
div[data-testid="stMetric"] {
    background: #17263d;
    border: 1px solid #2a3d59;
    padding: 15px;
    border-radius: 12px;
}

/* Buttons */
.stButton button {
    border-radius: 9px;
}

/* Captions */
.stCaption {
    color: #94a3b8;
}

</style>
""", unsafe_allow_html=True)


# -------------------------------------------------
# DATA LOADERS
# -------------------------------------------------

@st.cache_data
def load_retail():
    return pd.read_csv(project_path("data/processed/online_retail.csv"))


@st.cache_data
def load_comparison():
    return pd.read_csv(project_path("results/model_comparison.csv"))


@st.cache_data
def load_hierarchical():
    return pd.read_csv(
        project_path("results/hierarchical_customer_segments.csv")
    )


# -------------------------------------------------
# REUSABLE UI COMPONENTS
# -------------------------------------------------

def show_banner(title, subtitle):
    st.markdown(f"""
    <div class="hero">
        <h1>{title}</h1>
        <p>{subtitle}</p>
        <span class="hero-tag">⚡ PySpark</span>
        <span class="hero-tag">📡 Apache Kafka</span>
        <span class="hero-tag">🤖 Machine Learning</span>
        <span class="hero-tag">🔗 NetworkX</span>
        <span class="hero-tag">📊 Streamlit</span>
    </div>
    """, unsafe_allow_html=True)


def show_heading(title):
    st.markdown(
        f'<div class="section-heading">{title}</div>',
        unsafe_allow_html=True
    )


def show_kpis(items):
    columns = st.columns(len(items))

    for col, item in zip(columns, items):
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">
                    {item["label"]}
                    <span class="kpi-icon">{item["icon"]}</span>
                </div>
                <div class="kpi-value">{item["value"]}</div>
            </div>
            """, unsafe_allow_html=True)


# -------------------------------------------------
# SIDEBAR NAVIGATION
# -------------------------------------------------

st.sidebar.markdown("""
<div style="padding: 12px 0 22px 0;">
    <div style="font-size:30px;">🛒</div>
    <div style="font-size:22px;font-weight:800;color:#f8fafc;">
        E-Commerce
    </div>
    <div style="font-size:12px;color:#94a3b8;">
        ANALYTICS PLATFORM
    </div>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

st.sidebar.markdown("### 📍 Navigation")

page = st.sidebar.radio(
    "Explore dashboard",
    [
        "Sales Analytics",
        "Customer Segmentation",
        "ML Performance",
        "Graph Analytics",
        "Real-Time Streaming"
    ],
    label_visibility="collapsed"
)

st.sidebar.markdown("---")

st.sidebar.markdown("""
<div style="padding-top:10px;">
    <p style="font-size:12px;color:#94a3b8;">
        PROJECT: REAL-TIME E-COMMERCE ANALYTICS
    </p>
    <p style="font-size:12px;color:#94a3b8;">
        Big Data Processing & Customer Intelligence
    </p>
    <p style="font-size:11px;color:#64748b;">
        Built with Python | PySpark | Kafka | NetworkX
    </p>
</div>
""", unsafe_allow_html=True)


# =================================================
# 1. SALES ANALYTICS
# =================================================

if page == "Sales Analytics":

    show_banner(
        "🛒 Real-Time E-Commerce Analytics",
        "Transforming retail data into actionable business intelligence."
    )

    show_heading("📈 Historical Sales Overview")

    df = load_retail()

    df = df.dropna(subset=["CustomerID"])

    df = df[
        (df["Quantity"] > 0) &
        (df["UnitPrice"] > 0) &
        (~df["InvoiceNo"].astype(str).str.startswith(("C", "A")))
    ].copy()

    df["Revenue"] = df["Quantity"] * df["UnitPrice"]

    show_kpis([
        {
            "label": "Total Revenue",
            "value": f"£{df['Revenue'].sum():,.0f}",
            "icon": "💰"
        },
        {
            "label": "Unique Customers",
            "value": f"{df['CustomerID'].nunique():,}",
            "icon": "👥"
        },
        {
            "label": "Unique Products",
            "value": f"{df['StockCode'].nunique():,}",
            "icon": "📦"
        },
        {
            "label": "Transactions",
            "value": f"{df['InvoiceNo'].nunique():,}",
            "icon": "🧾"
        }
    ])

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    show_heading("📊 Revenue Trends")

    monthly = (
        df.groupby(df["InvoiceDate"].dt.to_period("M"))["Revenue"]
        .sum()
    )

    monthly.index = monthly.index.astype(str)

    st.line_chart(
        monthly,
        color="#38bdf8",
        height=350
    )

    col1, col2 = st.columns(2)

    with col1:
        show_heading("🏆 Top 10 Products")

        top_products = (
            df.groupby("Description")["Revenue"]
            .sum()
            .nlargest(10)
            .sort_values()
        )

        top_products.index = [
            str(x)[:27] for x in top_products.index
        ]

        st.bar_chart(
            top_products,
            color="#38bdf8",
            height=400,
            horizontal=True
        )

    with col2:
        show_heading("🌍 Country-wise Revenue")

        country_sales = (
            df.groupby("Country")["Revenue"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .sort_values()
        )

        st.bar_chart(
            country_sales,
            color="#2dd4bf",
            height=400,
            horizontal=True
        )


# =================================================
# 2. CUSTOMER SEGMENTATION
# =================================================

elif page == "Customer Segmentation":

    show_banner(
        "👥 Customer Intelligence",
        "Discover customer groups through RFM analysis and hierarchical clustering."
    )

    hierarchical = load_hierarchical()

    show_heading("Customer Segmentation Overview")

    cluster_counts = (
        hierarchical["Cluster"]
        .value_counts()
        .sort_index()
    )

    show_kpis([
        {
            "label": "Customers Segmented",
            "value": f"{len(hierarchical):,}",
            "icon": "👥"
        },
        {
            "label": "Number of Clusters",
            "value": f"{hierarchical['Cluster'].nunique()}",
            "icon": "🎯"
        },
        {
            "label": "Highest Cluster Size",
            "value": f"{cluster_counts.max():,}",
            "icon": "📊"
        }
    ])

    col1, col2 = st.columns(2)

    with col1:
        show_heading("📊 Cluster Distribution")

        st.bar_chart(
            cluster_counts,
            color="#38bdf8",
            height=350
        )

    with col2:
        show_heading("📋 Cluster Profiles")

        profile = hierarchical.groupby("Cluster").agg(
            Customers=("CustomerID", "count"),
            Avg_Recency=("Recency", "mean"),
            Avg_Frequency=("Frequency", "mean"),
            Avg_Monetary=("Monetary", "mean")
        ).round(2)

        st.dataframe(
            profile,
            use_container_width=True,
            height=350
        )

    show_heading("🌳 Hierarchical Clustering Dendrogram")

    dendrogram = project_path("results/hierarchical_dendrogram.png")

    if os.path.exists(dendrogram):
        st.image(dendrogram, use_container_width=True)
    else:
        st.warning("Dendrogram image not found.")


# =================================================
# 3. MACHINE LEARNING
# =================================================

elif page == "ML Performance":

    show_banner(
        "🤖 Machine Learning Intelligence",
        "Evaluate predictive models for customer behavior and purchasing patterns."
    )

    comparison = load_comparison()

    show_heading("Model Comparison")

    st.dataframe(
        comparison,
        use_container_width=True,
        hide_index=True
    )

    # Logistic Regression
    show_heading("🎯 Logistic Regression — Classification")

    logistic = comparison[
        comparison["Algorithm"] == "Logistic Regression"
    ]

    if not logistic.empty:

        metrics = logistic.iloc[0]

        show_kpis([
            {
                "label": "Accuracy",
                "value": f"{metrics['Accuracy']:.2%}",
                "icon": "🎯"
            },
            {
                "label": "Precision",
                "value": f"{metrics['Precision']:.2%}",
                "icon": "📌"
            },
            {
                "label": "Recall",
                "value": f"{metrics['Recall']:.2%}",
                "icon": "🔍"
            },
            {
                "label": "F1 Score",
                "value": f"{metrics['F1']:.2%}",
                "icon": "⚖️"
            }
        ])

    # Linear Regression
    show_heading("📈 Linear Regression — Spending Prediction")

    linear = comparison[
        comparison["Algorithm"] == "Linear Regression"
    ]

    if not linear.empty:

        metrics = linear.iloc[0]

        show_kpis([
            {
                "label": "RMSE",
                "value": f"£{metrics['RMSE']:,.2f}",
                "icon": "📉"
            },
            {
                "label": "MAE",
                "value": f"£{metrics['MAE']:,.2f}",
                "icon": "📊"
            },
            {
                "label": "R² Score",
                "value": f"{metrics['R2']:.2%}",
                "icon": "📐"
            }
        ])

    # Clustering
    show_heading("🧩 Clustering — Silhouette Scores")

    clustering = comparison[
        comparison["Algorithm"].isin(
            ["K-Means", "Hierarchical Clustering"]
        )
    ]

    if not clustering.empty:
        st.bar_chart(
            clustering.set_index("Algorithm")["Silhouette"],
            color="#2dd4bf",
            height=300
        )


# =================================================
# 4. GRAPH ANALYTICS
# =================================================

elif page == "Graph Analytics":

    show_banner(
        "🔗 Customer-Product Network",
        "Explore relationships between customers and the products they purchase."
    )

    show_heading("Network Graph Overview")

    show_kpis([
        {
            "label": "Total Graph Nodes",
            "value": "8,003",
            "icon": "🔵"
        },
        {
            "label": "Customer Nodes",
            "value": "4,338",
            "icon": "👥"
        },
        {
            "label": "Product Nodes",
            "value": "3,665",
            "icon": "📦"
        },
        {
            "label": "Graph Edges",
            "value": "266,792",
            "icon": "🔗"
        }
    ])

    graph_image = project_path(
        "results/customer_product_graph.png"
    )

    show_heading("🕸️ Customer-Product Bipartite Graph")

    if os.path.exists(graph_image):
        st.image(graph_image, use_container_width=True)
    else:
        st.warning("Graph image not found.")

    st.markdown("""
    <div class="info-panel">
        <b>How to interpret this graph</b><br>
        🔵 Customer nodes represent individual customers.<br>
        🟢 Product nodes represent purchased products.<br>
        🔗 Edges represent purchasing relationships between customers and products.<br>
        📊 Edge weights represent the total quantity purchased.
    </div>
    """, unsafe_allow_html=True)


# =================================================
# 5. REAL-TIME STREAMING
# =================================================

elif page == "Real-Time Streaming":

    show_banner(
        "⚡ Real-Time Event Intelligence",
        "Monitor customer events processed through Apache Kafka and Spark Structured Streaming."
    )

    show_heading("Streaming Pipeline Overview")

    st.markdown("""
    <div class="info-panel">
        <b>Kafka Producer</b> &nbsp; → &nbsp;
        <b>Kafka Topic</b> &nbsp; → &nbsp;
        <b>Spark Structured Streaming</b> &nbsp; → &nbsp;
        <b>Parquet Storage</b> &nbsp; → &nbsp;
        <b>Dashboard</b>
    </div>
    """, unsafe_allow_html=True)

    event_path = project_path("data/streaming/events")

    parquet_files = glob.glob(
        os.path.join(event_path, "*.parquet")
    ) if os.path.exists(event_path) else []

    if parquet_files:

        events = pd.read_parquet(event_path)

        show_heading("📡 Live Event Statistics")

        show_kpis([
            {
                "label": "Total Events",
                "value": f"{len(events):,}",
                "icon": "⚡"
            },
            {
                "label": "Unique Customers",
                "value": f"{events['customer_id'].nunique():,}",
                "icon": "👥"
            },
            {
                "label": "Event Types",
                "value": f"{events['event_type'].nunique()}",
                "icon": "📊"
            }
        ])

        col1, col2 = st.columns(2)

        with col1:
            show_heading("📊 Event Type Distribution")

            event_counts = events["event_type"].value_counts()

            st.bar_chart(
                event_counts,
                color="#38bdf8",
                height=350
            )

        with col2:
            show_heading("📋 Event Summary")

            st.dataframe(
                event_counts.rename("Count"),
                use_container_width=True
            )

        show_heading("🧾 Recent Customer Events")

        st.dataframe(
            events.sort_values(
                "timestamp",
                ascending=False
            ).head(20),
            use_container_width=True,
            hide_index=True
        )

        st.caption(
            "Source: Spark Structured Streaming Parquet output. "
            "Refresh this dashboard to load newly processed events."
        )

    else:
        st.warning("No streaming Parquet events found yet.")


# -------------------------------------------------
# FOOTER
# -------------------------------------------------

st.markdown("---")

st.markdown("""
<div style="text-align:center;padding:15px;color:#64748b;">
    <div style="font-size:13px;font-weight:600;">
        REAL-TIME E-COMMERCE ANALYTICS PLATFORM
    </div>
    <div style="font-size:11px;margin-top:7px;">
        Powered by Python • PySpark • Apache Kafka • Machine Learning • NetworkX • Streamlit
    </div>
</div>
""", unsafe_allow_html=True)
