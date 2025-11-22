import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
import pymongo  
import io

# Page configuration
st.set_page_config(
    page_title="Air Quality Dashboard",
    page_icon="🌬️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Here we need to specify all the name of columns based on the data produce by
# producer.py
AIR_QUALITY_METRICS = [
    "pm10", "pm2_5", "carbon_monoxide", "carbon_dioxide", 
    "nitrogen_dioxide", "sulphur_dioxide", "ozone", "methane", 
    "uv_index_clear_sky", "uv_index", "dust", "aerosol_optical_depth"
]


# in this static function I added this helper to prevent reconnecting to MongoDB on every button click.
@st.cache_resource
def get_mongo_connection(uri):
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info() # Trigger connection check
        return client
    except Exception:
        return None

# same with this area
@st.cache_data
def convert_fig_to_png(fig):
    buf = io.BytesIO()
    fig.write_image(buf, format="png")
    return buf

def setup_sidebar():
    """
    Sidebar configuration for Kafka and MongoDB
    """
    st.sidebar.title("Dashboard Controls")
    
    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    kafka_topic = st.sidebar.text_input("Kafka Topic", value="streaming-data")
    
    st.sidebar.subheader("Historical Storage (MongoDB)")

    # Pre-filled with your provided URI structure (User must enter password)
    # For this entry bar We include a URI bar so Any user can use the URI of their database to query an air quality dataset of their own
    # if they have their own database to use
    default_uri = "mongodb+srv://qkgaanduque_db_user:Plmoknijb26!@groceryinventorysystem.ntr788c.mongodb.net/?appName=GroceryInventorySystem"
    
    mongo_uri = st.sidebar.text_input(
        "MongoDB URI", 
        value=default_uri,
        type="password", # Hides characters for security if you paste a real password
        help="Enter your connection string. Replace <db_password> with your actual password."
    )
    
    db_name = st.sidebar.text_input("Database Name", value="BIGDATA_PROJECT")
    collection_name = st.sidebar.text_input("Collection Name", value="air_quality")
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri,
        "db_name": db_name,
        "collection_name": collection_name
    }

def generate_sample_data():
    """Fallback data generator if connections fail"""
    current_time = datetime.now()
    data = []
    for i in range(10):
        ts = current_time - timedelta(minutes=i)
        for metric in ["pm10", "ozone"]:
            data.append({
                'timestamp': ts,
                'value': 50 + (i * 2),
                'metric_type': metric,
                'sensor_id': 'SAMPLE_SENSOR'
            })
    return pd.DataFrame(data)



# Here we create a consumer which will get the latest data sent by the producer which will be use
# for the streaming of the data into the surface layer of UI

def consume_kafka_data(config):
    """
    Real-time Consumer
    Reads 'Wide' JSON from the Producer and converts to 'Long' format
    """
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    
    # Initialize Consumer if not exists
    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000 
            )
        except Exception as e:
            st.error(f"Failed to connect to Kafka: {e}")
            st.session_state[cache_key] = None
    
    consumer = st.session_state[cache_key]
    
    if consumer:
        try:
            messages = []
            raw_messages = []
            start_time = time.time()
            
            # Fetch a batch of messages
            # Reduced wait time from 2s to 0.5s to prevent UI lag
            while time.time() - start_time < 0.5 and len(raw_messages) < 50:
                msg_pack = consumer.poll(timeout_ms=100)
                for tp, batch in msg_pack.items():
                    for m in batch:
                        raw_messages.append(m.value)
            
            # Process the "Wide" messages from the Producer into "Long" format
            for data in raw_messages:
                timestamp_str = data.get('timestamp')
                sensor_id = data.get('sensor_id', 'UNKNOWN')
                
                if timestamp_str:
                    try:
                        if timestamp_str.endswith('Z'):
                            timestamp_str = timestamp_str[:-1] + '+00:00'
                        ts = datetime.fromisoformat(timestamp_str)
                        
                        # LOOP THROUGH YOUR METRICS
                        for metric in AIR_QUALITY_METRICS:
                            if metric in data and data[metric] is not None:
                                messages.append({
                                    'timestamp': ts,
                                    'value': float(data[metric]),
                                    'metric_type': metric,
                                    'sensor_id': sensor_id
                                })
                    except Exception as e:
                        continue 

            if messages:
                df = pd.DataFrame(messages)
                if 'data_cache' not in st.session_state:
                    st.session_state['data_cache'] = pd.DataFrame()
                
                st.session_state['data_cache'] = pd.concat([st.session_state['data_cache'], df], ignore_index=True)
                st.session_state['data_cache'] = st.session_state['data_cache'].sort_values('timestamp').tail(500)
                return st.session_state['data_cache']
            
            if 'data_cache' in st.session_state and not st.session_state['data_cache'].empty:
                return st.session_state['data_cache']
                
            return generate_sample_data()
                
        except Exception as e:
            st.error(f"Error processing data: {e}")
            return generate_sample_data()
    else:
        return generate_sample_data()



# This fuction will be used to give a capability to our streamlit app
# to request data from our mongoDB server (Historical Data)

def query_historical_data(config, time_range="1h", metrics=None):

    mongo_uri = config.get("mongo_uri")
    db_name = config.get("db_name", "BIGDATA_PROJECT")
    collection_name = config.get("collection_name", "air_quality")
    
    # Safety check for the default placeholder password
    # Of course we need to include password I will keep my own
    if "<db_password>" in mongo_uri:
        st.warning("⚠️ Please replace <db_password> in the sidebar with your actual MongoDB password.")
        return None

    try:
        
        
        # Here I implement caching connection instead of creating a new one every time
        # Since contributes to the lag of the system
        client = get_mongo_connection(mongo_uri)
        if not client:
            st.error("Failed to connect to MongoDB")
            return None

        db = client["BIGDATA_PROJECT"]
        collection = db["air_quality"]
        
        
        # This is lightweight and prevents crashing later on
        # client.server_info() # Handled in the helper now

        # Calculate Time Filter
        end_date = datetime.now()
        start_date = end_date
        
        if time_range == "1h":
            start_date = end_date - timedelta(hours=1)
        elif time_range == "24h":
            start_date = end_date - timedelta(hours=24)
        elif time_range == "7d":
            start_date = end_date - timedelta(days=7)
        elif time_range == "30d":
            start_date = end_date - timedelta(days=30)
        elif time_range == "All Time":
             start_date = datetime.min

        # Execute Query
        # We filter by timestamp >= start_date
        query = {"timestamp": {"$gte": start_date}}
        
        # Optimization, only fetch fields we actually need to speed up transfer
        projection = {"timestamp": 1, "_id": 0} # Always get timestamp, exclude mongo ID
        if metrics:
            for m in metrics:
                projection[m] = 1
        # If no metrics selected (unlikely), we fetch all (projection remains basic)
        
        cursor = collection.find(query, projection)
        
        # Convert to DataFrame
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return pd.DataFrame()

        # Transform to Long Format for Plotly
        # Your MongoDB stores data in "Wide" format (cols: pm10, ozone, etc.)
        # Plotly needs "Long" format (rows: metric_type=pm10, value=50)
        
        if metrics is None or len(metrics) == 0:
             metrics = AIR_QUALITY_METRICS
        
        # Only keep columns that actually exist in the returned data
        valid_metrics = [m for m in metrics if m in df.columns]
        
        if not valid_metrics:
            return df

        # Melt: timestamp, variable (metric_type), value
        long_df = filtered_df = df.melt(
            id_vars=['timestamp'], 
            value_vars=valid_metrics,
            var_name='metric_type', 
            value_name='value'
        )
        
        return long_df.sort_values('timestamp')

    except Exception as e:
        st.error(f"Error connecting to MongoDB: {e}")
        return None


# This is where the data get from the consumer function gets works
# It shows the real time data in a line plot style


def display_real_time_view(config, refresh_interval):
    """Real-time Tab Logic"""
    st.header("📈 Air Quality Real-time Stream")
    
    refresh_state = st.session_state.refresh_state
    st.info(f"Auto-refresh: {'On' if refresh_state['auto_refresh'] else 'Off'} ({refresh_interval}s)")
    
    real_time_data = consume_kafka_data(config)
    
    if not real_time_data.empty:
        st.subheader("🛠️ Select Metrics to View")
        selected_metrics = st.multiselect(
            "Choose pollutants:", 
            options=AIR_QUALITY_METRICS,
            default=["pm10", "ozone"]
        )
        
        if not selected_metrics:
            st.warning("Select at least one metric.")
            return

        filtered_df = real_time_data[real_time_data['metric_type'].isin(selected_metrics)]
        
        st.subheader("💡 Latest Readings")
        latest_df = filtered_df.sort_values('timestamp').groupby('metric_type').tail(1)
        cols = st.columns(len(selected_metrics))
        
        for idx, row in enumerate(latest_df.itertuples()):
            if idx < len(cols):
                cols[idx].metric(label=row.metric_type.upper(), value=f"{row.value:.2f}")

        st.subheader("📈 Trend Over Time")
        fig = px.line(
            filtered_df,
            x='timestamp',
            y='value',
            color='metric_type',
            title="Real-time Air Quality Trends",
            template="plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("View Raw Data"):
            st.dataframe(filtered_df.sort_values('timestamp', ascending=False))
    else:
        st.warning("Waiting for data from Kafka...")



# This is the function wherein the user can query a data for the specific timeline
# This uses the function of query historical data to the MongoDB which will be use 
# In showing the data

def display_historical_view(config):
    """Historical Tab Logic (MongoDB)"""
    st.header("📊 Historical Data Analysis (MongoDB)")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Historical Filters")
    
    # Interactive controls
    col1, col2 = st.columns(2)
    
    with col1:
        time_range = st.selectbox(
            "Time Range", 
            ["24h", "7d", "30d", "All Time"],
            index=1
        )
    
    with col2:
        selected_metrics = st.multiselect(
            "Select Metrics", 
            options=AIR_QUALITY_METRICS,
            default=["pm10", "pm2_5", "ozone"]
        )
    
    if st.button("🔍 Load Historical Data"):
        with st.spinner("Querying MongoDB Atlas..."):
            # Call the MongoDB query function
            historical_data = query_historical_data(config, time_range, selected_metrics)
            
            if historical_data is not None and not historical_data.empty:
                st.success(f"Loaded {len(historical_data)} records from MongoDB.")
                
                # TREND CHART
                st.subheader(f"Trends ({time_range})")
                fig = px.line(
                    historical_data,
                    x='timestamp',
                    y='value',
                    color='metric_type',
                    title=f"Historical Air Quality - {time_range}",
                    template="plotly_white"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                
                
                buf = convert_fig_to_png(fig)

                # Create the download button
                st.download_button(
                    label="📥 Download Trend Chart (PNG)",
                    data=buf,
                    file_name=f"trend_analysis_{time_range}.png",
                    mime="image/png"
                )
                
                # --- STATISTICS ---
                st.subheader("Data Statistics")
                stats = historical_data.groupby('metric_type')['value'].describe()
                st.dataframe(stats)
                
            elif historical_data is not None:
                st.warning("Connected to MongoDB, but no data found for the selected range/metrics.")

def display_air_visuals(config):
    st.header("💨 Air Quality Visualizations Dashboard")

    st.sidebar.markdown("---")
    st.sidebar.subheader("Visual Settings")

    # User selects metrics for visualization
    selected_metrics = st.multiselect(
        "Select metrics to include in analytics:",
        options=AIR_QUALITY_METRICS,
        default=["pm10", "pm2_5", "ozone"]
    )

    if not selected_metrics:
        st.warning("Please select at least one metric.")
        return

    # Load ALL TIME historical data
    with st.spinner("Loading data for visual analytics..."):
        df = query_historical_data(config, "All Time", selected_metrics)

    if df is None or df.empty:
        st.warning("No historical data available to generate visuals.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    st.success(f"Loaded {len(df)} rows for visual analytics 🎉")

    # KPI SUMMARY CARDS
    st.subheader("📘 Key Statistics Overview")
    kpi_cols = st.columns(len(selected_metrics))
    for i, metric in enumerate(selected_metrics):
        metric_df = df[df["metric_type"] == metric]
        if not metric_df.empty:
            avg_val = metric_df["value"].mean()
            max_val = metric_df["value"].max()
            kpi_cols[i].metric(
                label=f"{metric.upper()} (avg)",
                value=f"{avg_val:.2f}",
                delta=f"Max: {max_val:.2f}"
            )

    # CORRELATION HEATMAP
    st.subheader("📊 Metric Correlation Heatmap")
    wide_df = df.pivot(index="timestamp", columns="metric_type", values="value")
    corr = wide_df.corr()
    fig_corr = px.imshow(
        corr,
        text_auto=True,
        color_continuous_scale="Blues",
        title="Correlation Between Air Quality Metrics"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

    st.info(
        """
        The Heatmap provided above show the correlation between every
        columns of the data that we use:
        - **1.0 (Dark Blue):** Perfect positive correlation (if one goes up, the other goes up)
        - **-1.0 (White/Light):** Negative correlation (if one goes up, the other goes down).
        - **0:** No relationship.
        """
    )
    
    
    # We use cached image converter to make it fast
    buf = convert_fig_to_png(fig_corr)

    st.download_button(
        label="📥 Download Correlation Heatmap",
        data=buf,
        file_name="correlation_heatmap.png",
        mime="image/png"
    )

    st.markdown("---")

    # DISTRIBUTION ANALYSIS (HISTOGRAM + BOX)
    st.subheader("📦 Metric Distribution Analysis")
    selected_single_metric = st.selectbox(
        "Select metric for distribution charts:",
        selected_metrics
    )
    metric_df = df[df["metric_type"] == selected_single_metric]
    colA, colB = st.columns(2)

    with colA:
        st.markdown("### Histogram")
        fig_hist = px.histogram(
            metric_df, 
            x="value", 
            title=f"Distribution of {selected_single_metric.upper()}",
            nbins=30
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        #This is the description
        st.caption(f"The Histogram shows the frequency of specific {selected_single_metric} values. Taller bars mean that value occurs more often.")

        # Image cached image converter to make it fast
        buf_hist = convert_fig_to_png(fig_hist)
        st.download_button(
            label="📥 Download Histogram",
            data=buf_hist,
            file_name=f"{selected_single_metric}_histogram.png",
            mime="image/png"
        )

    with colB:
        st.markdown("### Box Plot")
        fig_box = px.box(
            metric_df,
            y="value",
            title=f"Box Plot – {selected_single_metric.upper()}"
        )
        st.plotly_chart(fig_box, use_container_width=True)

        st.caption(f"The Box Plot highlights outliers. The points floating above the box are unusually high readings for {selected_single_metric}.")

        
        # We cached image converter to make it fask
        buf_box = convert_fig_to_png(fig_box)
        st.download_button(
            label="📥 Download Box Plot",
            data=buf_box,
            file_name=f"{selected_single_metric}_boxplot.png",
            mime="image/png"
        )

    # COMPARISON: SCATTER + TRENDLINE
    st.subheader("🔍 Compare Two Metrics")
    col1, col2 = st.columns(2)
    metric_x = col1.selectbox("X-Axis Metric", selected_metrics)
    metric_y = col2.selectbox("Y-Axis Metric", selected_metrics)

    if metric_x != metric_y:
        merged = df.pivot(index="timestamp", columns="metric_type", values="value").dropna()
        fig_scatter = px.scatter(
            merged,
            x=metric_x,
            y=metric_y,
            trendline="ols",
            title=f"Relationship Between {metric_x.upper()} and {metric_y.upper()}"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown(f"""
        **Analysis:**
        This chart compares **{metric_x}** vs **{metric_y}**. 
        The **Trendline (OLS)** shows the general direction. If the line goes up, these pollutants likely have a common source.
        """)
        
        
        # Since the system is lag
        # we try to implement cached image converter just to make it fast
        buf_scatter = convert_fig_to_png(fig_scatter)
        st.download_button(
            label="📥 Download Scatter + Trendline",
            data=buf_scatter,
            file_name=f"{metric_x}_{metric_y}_scatter.png",
            mime="image/png"
        )
    else:
        st.info("Select two different metrics to compare.")

    st.markdown("---")
    st.markdown("😊 *Hoping that you gained Insights about the Visualization* 😊")

def display_save_data(config):
    st.header("💾 Export Your Data & Visuals")
    st.info("You can export raw data or download charts from the Air Quality Visuals tab.")

    # Load Data to Export
    st.subheader("📦 Select Data to Export")
    
    export_metrics = st.multiselect(
        "Choose metrics to include:",
        options=AIR_QUALITY_METRICS,
        default=["pm10", "pm2_5", "ozone"]
    )

    time_range = st.selectbox(
        "Select time range:",
        ["24h", "7d", "30d", "All Time"],
        index=1
    )

    if st.button("📥 Load Data"):
        with st.spinner("Fetching data..."):
            df_export = query_historical_data(config, time_range, export_metrics)

        if df_export is None or df_export.empty:
            st.warning("No data found for the selected metrics/time range.")
            return

        st.success(f"Loaded {len(df_export)} rows.")
        st.dataframe(df_export.head(20))

        # Download Buttons
        col1, col2, col3 = st.columns(3)

        # CSV
        col1.download_button(
            label="⬇️ Download CSV",
            data=df_export.to_csv(index=False).encode('utf-8'),
            file_name="air_quality_data.csv",
            mime="text/csv"
        )

        # Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_export.to_excel(writer, index=False, sheet_name='Data')
        col2.download_button(
            label="⬇️ Download Excel",
            data=output.getvalue(),
            file_name="air_quality_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # JSON
        col3.download_button(
            label="⬇️ Download JSON",
            data=df_export.to_json(orient='records').encode('utf-8'),
            file_name="air_quality_data.json",
            mime="application/json"
        )

    st.markdown("---")
    st.markdown("### *Thankyou For downloading Data please use it properly*😁")


def main():
    st.title("🚀 Air Quality Monitoring Dashboard")
    st.markdown("### 📍 Technological Institute of the Philippines - Quezon City")

    # Visualizing the Architecture Pipeline using Columns
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns([1, 0.2, 1, 0.2, 1])
    
    with col1:
        st.info("📡 **Source**\n\nIoT Sensors (Producer)")
    with col2:
        st.markdown("<h3 style='text-align: center;'>➜</h3>", unsafe_allow_html=True)
    with col3:
        st.warning("⚡ **Stream**\n\nApache Kafka")
    with col4:
        st.markdown("<h3 style='text-align: center;'>➜</h3>", unsafe_allow_html=True)
    with col5:
        st.success("💾 **Storage**\n\nMongoDB Atlas")

    with st.expander("**Project Capabilities** (Click to Expand)", expanded=True):
        st.markdown("""
        This Big Data system is designed to monitor, store, and analyze air pollutants in real-time.
        
        * **⚡ Real-time Streaming:** Captures live sensor data via Apache Kafka.
        * **🗄️ NoSQL Storage:** Archives historical records in MongoDB Atlas.
        * **📊 Interactive Analytics:** Visualizes trends using Plotly & Pandas.
        * **💾 Data Export:** Download datasets in CSV, Excel, or JSON formats.
        """)
    st.markdown("---")

    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {'last_refresh': datetime.now(), 'auto_refresh': True}
    
    config = setup_sidebar()
    
    # Auto-refresh logic
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider("Refresh Rate (s)", 5, 60, 5)
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    else:
        refresh_interval = 0

    # Global Manual Refresh
    if st.sidebar.button("🔄 Manual Refresh"):
        st.rerun()
        
    # Main Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data","💨 Air quality Visuals", "💾 Export Data or Visuals"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)

    with tab3:
        display_air_visuals(config)

    with tab4:
        display_save_data(config)
if __name__ == "__main__":
    main()