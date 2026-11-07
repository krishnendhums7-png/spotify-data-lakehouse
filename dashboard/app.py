
import pandas as pd
import streamlit as st

# -----------------------------------
# 1. Dashboard configuration
# -----------------------------------

st.set_page_config(
    page_title="Global Spotify Analytics",
    page_icon="🎵",
    layout="wide"
)


# -----------------------------------
# Spotify-inspired dashboard styling
# -----------------------------------

st.markdown("""
<style>

/* Main background */
.stApp {
    background-color: #121212;
    color: #FFFFFF;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #181818;
}

/* Main title */
h1 {
    color: #1DB954 !important;
    font-weight: 800;
}

/* Section headings */
h2, h3 {
    color: #FFFFFF !important;
}

/* KPI cards */
[data-testid="stMetric"] {
    background-color: #202020;
    padding: 18px;
    border-radius: 12px;
    border: 1px solid #333333;
}

/* KPI values */
[data-testid="stMetricValue"] {
    color: #1DB954;
}

/* Select boxes */
[data-testid="stSelectbox"] {
    background-color: #202020;
}

/* Data table */
[data-testid="stDataFrame"] {
    border: 1px solid #333333;
    border-radius: 10px;
}

/* Captions */
.stCaption {
    color: #B3B3B3;
}

</style>
""", unsafe_allow_html=True)

# -----------------------------------
# 2. Load country profile data
# -----------------------------------

@st.cache_data
def load_data():
    file_path = "dbt_spotify/exports/analytics/mart_country_profile.parquet"
    data = pd.read_parquet(file_path)
    return data

df = load_data()

# Exclude global chart records for country comparisons
df = df[df["is_global"] == False].copy()

# -----------------------------------
# 3. Dashboard title
# -----------------------------------

st.title("🎵 Global Spotify Music Streaming Analytics")

st.markdown(
    "### Exploring global music popularity, "
    "country performance and audio characteristics"
)

st.caption("Data Engineering Capstone Project")

st.divider()

# -----------------------------------
# 4. Sidebar filters
# -----------------------------------

st.sidebar.header("Dashboard Filters")

continents = ["All"] + sorted(
    df["continent"].dropna().unique().tolist()
)

selected_continent = st.sidebar.selectbox(
    "Select Continent",
    continents,
    key="continent_filter"
)

chart_types = ["All"] + sorted(
    df["chart_type"].dropna().unique().tolist()
)

selected_chart = st.sidebar.selectbox(
    "Select Chart Type",
    chart_types,
    key="country_chart_filter"
)

filtered_df = df.copy()

if selected_continent != "All":
    filtered_df = filtered_df[
        filtered_df["continent"] == selected_continent
    ]

if selected_chart != "All":
    filtered_df = filtered_df[
        filtered_df["chart_type"] == selected_chart
    ]

st.sidebar.caption(
    "These filters currently control the Country Overview section."
)

# -----------------------------------
# 5. KPI cards
# -----------------------------------

st.subheader("Global Country Overview")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Countries",
    filtered_df["country_name"].nunique()
)

col2.metric(
    "Chart Entries",
    f"{filtered_df['chart_entries'].sum():,}"
)

col3.metric(
    "Distinct Tracks",
    f"{filtered_df['distinct_tracks'].sum():,}"
)

col4.metric(
    "Average Popularity",
    round(filtered_df["avg_track_popularity"].mean(), 2)
)

st.divider()

# -----------------------------------
# 6. Top countries by chart entries
# -----------------------------------

st.subheader("Top 10 Countries by Chart Entries")

top_countries = filtered_df.sort_values(
    by="chart_entries",
    ascending=False
).head(10)

st.bar_chart(
    top_countries.set_index("country_name")["chart_entries"]
)

# -----------------------------------
# 7. Country popularity comparison
# -----------------------------------

st.subheader("Country Music Popularity")

popularity_data = filtered_df.sort_values(
    by="avg_track_popularity",
    ascending=False
).head(10)

st.bar_chart(
    popularity_data.set_index("country_name")[
        "avg_track_popularity"
    ]
)

# -----------------------------------
# 8. Audio feature analysis
# -----------------------------------

st.divider()

st.subheader("Audio Feature Analysis")

st.caption(
    "Audio features are available only for a subset of chart entries. "
    "Interpret these averages with this coverage limitation in mind."
)

audio_features = {
    "Danceability": "avg_danceability",
    "Energy": "avg_energy",
    "Valence": "avg_valence",
    "Acousticness": "avg_acousticness",
    "Speechiness": "avg_speechiness",
    "Tempo": "avg_tempo"
}

selected_feature = st.selectbox(
    "Choose an Audio Feature",
    list(audio_features.keys())
)

feature_column = audio_features[selected_feature]

audio_data = filtered_df[
    ["country_name", feature_column, "chart_entries"]
].dropna()

audio_data = audio_data.sort_values(
    by="chart_entries",
    ascending=False
).head(10)

st.bar_chart(
    audio_data.set_index("country_name")[feature_column]
)

# -----------------------------------
# 9. Country data table
# -----------------------------------

st.divider()

st.subheader("Country Analytics Data")

display_columns = [
    "country_name",
    "continent",
    "chart_type",
    "chart_entries",
    "distinct_tracks",
    "total_streams",
    "avg_chart_rank",
    "avg_track_popularity"
]

st.dataframe(
    filtered_df[display_columns].sort_values(
        by="chart_entries",
        ascending=False
    ),
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Source: Member 3 analytical Parquet exports. "
    "Distinct-track counts are summed across country records "
    "in the KPI and may include the same track in multiple countries."
)


# -----------------------------------
# 10. Artist Leaderboard
# -----------------------------------

st.divider()

st.subheader("Artist Leaderboard")

@st.cache_data
def load_artist_data():
    file_path = "dbt_spotify/exports/analytics/mart_artist_leaderboard.parquet"
    artist_data = pd.read_parquet(file_path)
    return artist_data

artist_df = load_artist_data()

# Display top 10 artists by streams rank
top_artists = artist_df.sort_values(
    "streams_rank",
    ascending=True
).head(10)

st.subheader("Top 10 Artists by Streams")

st.dataframe(
    top_artists[
        [
            "artist_name",
            "chart_entries",
            "distinct_tracks",
            "distinct_countries",
            "attributed_streams",
            "streams_rank"
        ]
    ],
    use_container_width=True,
    hide_index=True
)
st.subheader("Top 10 Artists by Attributed Streams")

artist_chart = top_artists.set_index("artist_name")[
    "attributed_streams"
]

st.bar_chart(artist_chart)
# ---------------- GENRE PERFORMANCE ----------------

st.header("🎧 Genre Performance")

genre_df = pd.read_parquet(
    "dbt_spotify/exports/analytics/mart_genre_performance.parquet"
)

# Top 10 genres by weighted streams
top_genres = genre_df.sort_values(
    "weighted_streams",
    ascending=False
).head(10)

st.subheader("Top 10 Genres by Weighted Streams")

st.bar_chart(
    top_genres.set_index("genre_name")["weighted_streams"]
)

# Genre popularity
st.subheader("Average Track Popularity by Genre")

top_popular_genres = genre_df.sort_values(
    "avg_track_popularity",
    ascending=False
).head(10)

st.bar_chart(
    top_popular_genres.set_index("genre_name")["avg_track_popularity"]
)

# Genre details
st.subheader("Genre Details")

st.dataframe(
    genre_df[
        [
            "genre_name",
            "charted_tracks",
            "distinct_countries",
            "weighted_streams",
            "avg_chart_rank",
            "avg_track_popularity"
        ]
    ].sort_values("weighted_streams", ascending=False),
    use_container_width=True
)

# ---------------- PERIOD TRENDS ----------------

st.header("📅 Monthly and Seasonal Trends")

period_df = pd.read_parquet(
    "dbt_spotify/exports/analytics/mart_period_trends.parquet"
)

# Chart type filter
chart_types = ["All"] + sorted(period_df["chart_type"].dropna().unique().tolist())

selected_period_chart = st.sidebar.selectbox(
    "Select Period Chart Type",
    chart_types,
    key="period_chart_filter"
)

filtered_period = period_df.copy()

if selected_period_chart != "All":
    filtered_period = filtered_period[
        filtered_period["chart_type"] == selected_period_chart
    ]

# Monthly chart entries
st.subheader("Monthly Chart Entries")

monthly_df = filtered_period.sort_values("year_month")

st.line_chart(
    monthly_df.set_index("year_month")["chart_entries"]
)

# Monthly streaming trends
st.subheader("Monthly Total Streams")

stream_df = monthly_df.dropna(subset=["total_streams"])

if not stream_df.empty:
    st.bar_chart(
        stream_df.set_index("year_month")["total_streams"]
    )
else:
    st.info("No streaming data available for this selection.")

# Seasonal performance
st.subheader("Seasonal Chart Entries")

seasonal_df = (
    filtered_period.groupby("season", as_index=False)["chart_entries"]
    .sum()
)

st.bar_chart(
    seasonal_df.set_index("season")["chart_entries"]
)

# Period details
st.subheader("Period Details")

st.dataframe(
    filtered_period[
        [
            "year",
            "month",
            "season",
            "chart_type",
            "chart_entries",
            "distinct_tracks",
            "total_streams",
            "data_volume"
        ]
    ].sort_values(["year", "month"]),
    use_container_width=True
)

st.caption(
    "Note: The available period dataset contains only 8 records. "
    "These charts represent the available observations, not a complete "
    "continuous monthly history."
)

# ---------------- TOP TRACKS BY COUNTRY ----------------

st.header("🌍 Top Tracks by Country")

tracks_df = pd.read_parquet(
    "dbt_spotify/exports/analytics/mart_country_top_tracks.parquet"
)

# Apply the sidebar continent filter
filtered_tracks = tracks_df.copy()

if selected_continent != "All":
    filtered_tracks = filtered_tracks[
        filtered_tracks["continent"] == selected_continent
    ]

# Country selection
countries = sorted(
    filtered_tracks["country_name"].dropna().unique()
)

if countries:

    selected_country = st.selectbox(
        "Select a Country",
        countries,
        key="top_tracks_country"
    )

    country_tracks = filtered_tracks[
        filtered_tracks["country_name"] == selected_country
    ].sort_values("streams_rank")

    st.subheader("Top 10 Tracks")

    top_tracks = country_tracks.head(10)

    st.dataframe(
        top_tracks[
            [
                "streams_rank",
                "track_name",
                "artist_credit",
                "primary_genre",
                "total_streams",
                "best_rank",
                "chart_entries"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )

    st.subheader("Country Summary")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Tracks Listed", len(country_tracks))

    with col2:
        st.metric(
            "Total Streams",
            f"{country_tracks['total_streams'].sum():,.0f}"
        )

    with col3:
        st.metric(
            "Average Best Rank",
            round(country_tracks["best_rank"].mean(), 2)
        )

else:
    st.info("No country data available for this continent.")

st.caption(
    "Source: Member 3 country top-tracks analytics export. "
    "Streams and rankings reflect the available dataset."
)

# ---------------- TRACK LONGEVITY ANALYSIS ----------------

st.header("⏳ Track Longevity Analysis")

longevity_df = pd.read_parquet(
    "dbt_spotify/exports/analytics/mart_track_longevity.parquet"
)

# KPI summary
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Tracks Analysed", len(longevity_df))

with col2:
    longest_span = longevity_df["chart_span_days"].max()
    st.metric("Longest Chart Span", f"{longest_span} days")

with col3:
    number_one_tracks = (
        longevity_df[longevity_df["times_number_one"] > 0].shape[0]
    )
    st.metric("Tracks Reaching No. 1", number_one_tracks)

# Longest chart presence
st.subheader("Top 10 Tracks by Chart Longevity")

longest_tracks = longevity_df.sort_values(
    "chart_span_days",
    ascending=False
).head(10)

st.bar_chart(
    longest_tracks.set_index("track_name")["chart_span_days"]
)

st.dataframe(
    longest_tracks[
        [
            "track_name",
            "artist_credit",
            "chart_span_days",
            "distinct_chart_dates",
            "distinct_countries",
            "best_chart_rank"
        ]
    ],
    use_container_width=True,
    hide_index=True
)

# Most number-one appearances
st.subheader("Tracks with the Most Number-One Appearances")

number_one_df = longevity_df[
    longevity_df["times_number_one"] > 0
].sort_values("times_number_one", ascending=False).head(10)

if not number_one_df.empty:
    st.dataframe(
        number_one_df[
            [
                "track_name",
                "artist_credit",
                "times_number_one",
                "chart_entries",
                "best_chart_rank"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No number-one appearances found.")

st.caption(
    "Chart span is calculated from the first to the last recorded "
    "chart date. It does not necessarily mean continuous chart presence."
)