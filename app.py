"""
app.py
======
Streamlit front-end for the Canadian entry-level / visa-sponsored
Job Bank search tool.

Behavior
--------
- Automatically runs the live pipeline every time the page loads/refreshes.
- Displays only jobs first found today.
- Shows a prominent message if no qualifying jobs exist today.
- Provides:
    * Job title
    * Employer
    * Location
    * Direct Job Bank link
    * Google search shortcut:
        "<Employer Name> Canada careers"
"""

from __future__ import annotations

from datetime import date
from urllib.parse import quote_plus

import streamlit as st

from data_pipeline import run_pipeline, get_jobs_for_date


# -----------------------------------------------------------------------------
# Streamlit Page Configuration
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="Canada Entry-Level Job Finder",
    page_icon="🇨🇦",
    layout="wide",
)

TODAY = date.today().isoformat()


# -----------------------------------------------------------------------------
# Styling
# -----------------------------------------------------------------------------

st.markdown(
    """
<style>

.block-container{
    padding-top:1.2rem;
    padding-bottom:2rem;
    max-width:1100px;
}

.job-card{
    border:1px solid #dddddd;
    border-radius:10px;
    padding:16px;
    margin-bottom:12px;
    background:#ffffff;
}

.job-title{
    font-size:1.15rem;
    font-weight:700;
    margin-bottom:8px;
}

.job-meta{
    font-size:0.95rem;
    color:#444444;
    margin-bottom:4px;
}

.banner{
    padding:22px;
    border-radius:10px;
    background:#f8f9fa;
    border:2px solid #d9d9d9;
    text-align:center;
    font-size:1.35rem;
    font-weight:700;
    color:#444;
    margin-top:30px;
}

.small-note{
    color:#666;
    font-size:0.85rem;
}

</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------

st.title("🇨🇦 Canadian Entry-Level Job Finder")
st.caption("Automatically refreshes from the live Canada Job Bank feed each time the page opens.")

# -----------------------------------------------------------------------------
# Run Live Pipeline
# -----------------------------------------------------------------------------

with st.spinner("Checking the live Canada Job Bank feed..."):
    summary = run_pipeline()

jobs = get_jobs_for_date(TODAY)

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------

st.markdown(
    f"""
**Date:** {TODAY}

**Today's Matching Jobs:** {len(jobs)}
"""
)

# -----------------------------------------------------------------------------
# No Results
# -----------------------------------------------------------------------------

if not jobs:
    st.markdown(
        """
<div class="banner">
No new matching listings for today.
</div>
""",
        unsafe_allow_html=True,
    )
    st.stop()

# -----------------------------------------------------------------------------
# Results
# -----------------------------------------------------------------------------

for job in jobs:

    employer = job.get("employer") or "Unknown Employer"
    title = job.get("title") or "Untitled Position"
    location = job.get("location") or "Not specified"

    job_url = job.get("url") or "#"

    google_search = (
        "https://www.google.com/search?q="
        + quote_plus(f"{employer} Canada careers")
    )

    st.markdown(
        f"""
<div class="job-card">
<div class="job-title">{title}</div>

<div class="job-meta"><strong>Employer:</strong> {employer}</div>

<div class="job-meta"><strong>Location:</strong> {location}</div>

</div>
""",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.link_button(
            "View Job Posting",
            job_url,
            use_container_width=True,
        )

    with col2:
        st.link_button(
            "Employer Careers",
            google_search,
            use_container_width=True,
        )

st.markdown("---")
st.markdown(
    f"""
<div class="small-note">
Pipeline completed successfully.

Keywords processed: {summary["keywords_processed"]} |
New jobs cached: {summary["new_jobs_saved"]} |
Matching jobs shown today: {len(jobs)}
</div>
""",
    unsafe_allow_html=True,
)
