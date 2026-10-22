"""CapEx / OpEx classification review — Finance reviewer workspace.

Suggestions are advisory. Decisions are appended to REVIEW.DECISION_HISTORY through
REVIEW.RECORD_DECISION; nothing here writes back to Clarity or changes the recorded class.
"""

import json

import streamlit as st

st.set_page_config(page_title="CapEx/OpEx review", page_icon=":material/fact_check:", layout="wide")

conn = st.connection("snowflake")
session = conn.session()


@st.cache_data(ttl=3600)
def environment() -> str:
    return session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]


DB = environment()
ENV = DB.replace("CAPEX_", "")


def q(sql: str, params=None, ttl: int = 30):
    return conn.query(sql, params=params, ttl=ttl)


def reviewer_name() -> str:
    user = getattr(st, "user", None)
    for attr in ("email", "user_name", "name"):
        val = getattr(user, attr, None) if user is not None else None
        if val:
            return str(val)
    return session.sql("SELECT CURRENT_USER()").collect()[0][0]


st.title("CapEx / OpEx classification review")
st.caption(
    f"Environment **{ENV}** · AI suggestions are advisory only. Decisions are recorded with full "
    "history and never change the source system or the recorded accounting classification."
)

queue_tab, pipeline_tab, financials_tab, eval_tab, audit_tab = st.tabs(
    [
        ":material/inbox: Review queue",
        ":material/sync: Pipeline status",
        ":material/bar_chart: Project financials",
        ":material/science: AI evaluation",
        ":material/history: Decision history",
    ]
)

# ---------------------------------------------------------------- Review queue
with queue_tab:
    queue = q(f"SELECT * FROM {DB}.REVIEW.REVIEW_QUEUE ORDER BY AMOUNT DESC")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Open suggestions", int((queue["REVIEW_STATUS"].isin(["PENDING", "REQUEST_INFO"])).sum()))
    c2.metric("CapEx → OpEx", int(((queue["CURRENT_CLASSIFICATION"] == "CAPEX") & (queue["PROPOSED_CLASSIFICATION"] == "OPEX")).sum()))
    c3.metric("OpEx → CapEx", int(((queue["CURRENT_CLASSIFICATION"] == "OPEX") & (queue["PROPOSED_CLASSIFICATION"] == "CAPEX")).sum()))
    c4.metric("Needs evidence", int((queue["PROPOSED_CLASSIFICATION"] == "INSUFFICIENT EVIDENCE").sum()))

    status_filter = st.segmented_control(
        "Status",
        ["PENDING", "REQUEST_INFO", "ACCEPT", "REJECT"],
        selection_mode="multi",
        default=["PENDING", "REQUEST_INFO"],
    )
    view = queue[queue["REVIEW_STATUS"].isin(status_filter or [])] if status_filter else queue
    view = view.reset_index(drop=True)
    # Key the table on its contents so a stale selection is dropped when rows change
    # (after a decision or a filter change).
    table_key = "queue_table_" + str(abs(hash(tuple(view["SUGGESTION_ID"]) + tuple(view["REVIEW_STATUS"]))))

    event = st.dataframe(
        view[[
            "COST_LINE_ID", "PROJECT_NAME", "COST_TYPE", "AMOUNT", "CURRENT_CLASSIFICATION",
            "PROPOSED_CLASSIFICATION", "TRIGGERING_CRITERIA", "CONFIDENCE", "REVIEW_STATUS",
        ]],
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "AMOUNT": st.column_config.NumberColumn("Amount", format="$%,.0f"),
            "CONFIDENCE": st.column_config.ProgressColumn("AI confidence", min_value=0, max_value=1, format="%.2f"),
            "COST_LINE_ID": "Cost line",
            "PROJECT_NAME": "Project",
            "COST_TYPE": "Cost type",
            "CURRENT_CLASSIFICATION": "Recorded",
            "PROPOSED_CLASSIFICATION": "Proposed",
            "TRIGGERING_CRITERIA": "Triggering criteria",
            "REVIEW_STATUS": "Status",
        },
        key=table_key,
    )

    rows = [r for r in (event.selection.rows if event and event.selection else []) if r < len(view)]
    if not rows:
        st.info("Select a row to see the evidence and record a decision.", icon=":material/touch_app:")
    else:
        item = view.iloc[rows[0]]
        with st.container(border=True):
            st.subheader(f"{item['COST_LINE_ID']} · {item['PROJECT_NAME']}")
            left, right = st.columns([3, 2])
            with left:
                st.markdown(f"**Recorded description:** {item['DESCRIPTION']}")
                st.markdown(
                    f"**Recorded:** `{item['CURRENT_CLASSIFICATION']}` → **Proposed:** `{item['PROPOSED_CLASSIFICATION']}`"
                )
                st.markdown(f"**AI rationale:** {item['RATIONALE']}")
                quotes = item["EVIDENCE_QUOTES"]
                quotes = json.loads(quotes) if isinstance(quotes, str) else (quotes or [])
                if quotes:
                    st.markdown("**Evidence quoted by the model**")
                    for quote in quotes:
                        st.markdown(f"> {quote}")
                with st.expander("Full supporting evidence (Clarity, Azure DevOps, ServiceNow)"):
                    st.text(item["SUPPORTING_EVIDENCE"])
            with right:
                st.markdown("**Why it was flagged**")
                st.table(
                    {
                        "Rule": [f"{item['RULE_ID']} — {item['TRIGGERING_CRITERIA']}"],
                        "Rule outcome": [item["RULE_OUTCOME"]],
                        "Policy reference": [item["POLICY_REFERENCE"]],
                        "Rule implies": [item["RULE_IMPLIED_CLASS"]],
                        "AI_CLASSIFY": [item["AI_CLASSIFY_LABEL"]],
                        "AI_COMPLETE": [item["AI_COMPLETE_LABEL"]],
                        "Criteria / prompt / model": [
                            f"v{item['CRITERIA_VERSION']} / {item['PROMPT_VERSION']} / {item['MODEL']}"
                        ],
                    }
                )

            with st.form(f"decide_{item['SUGGESTION_ID']}", clear_on_submit=True):
                decision = st.segmented_control(
                    "Decision",
                    ["ACCEPT", "REJECT", "REQUEST_INFO"],
                    format_func=lambda d: {"ACCEPT": "Accept", "REJECT": "Reject", "REQUEST_INFO": "Request more information"}[d],
                    default="ACCEPT",
                )
                comment = st.text_area("Reviewer comment", placeholder="Required for reject or request more information")
                submitted = st.form_submit_button("Record decision", type="primary", icon=":material/gavel:")
            if submitted:
                result = session.call(
                    f"{DB}.REVIEW.RECORD_DECISION", item["SUGGESTION_ID"], decision, comment, reviewer_name()
                )
                if str(result).startswith("Recorded"):
                    st.success(f"{result} for {item['COST_LINE_ID']}. The source record is unchanged.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(result)

# ---------------------------------------------------------------- Pipeline status
with pipeline_tab:
    st.subheader("Latest pull per source")
    st.caption("Source timestamp = when the source last changed the record. Received = when Snowflake landed it.")
    st.dataframe(q(f"SELECT * FROM {DB}.CONTROL.INGEST_STATUS ORDER BY SOURCE_SYSTEM, ENTITY"), hide_index=True)

    st.subheader("Run history")
    st.dataframe(
        q(
            f"""SELECT SOURCE_SYSTEM, ENTITY, TRIGGERED_BY, STATUS, STARTED_TS, PAGES, API_RETRIES,
                       RECORDS_RECEIVED, RECORDS_NEW_OR_CHANGED, RECORDS_UNCHANGED, ERROR_MESSAGE
                FROM {DB}.CONTROL.INGEST_RUN_LOG ORDER BY STARTED_TS DESC LIMIT 50"""
        ),
        hide_index=True,
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Data-quality exceptions")
        st.dataframe(q(f"SELECT RECORD_ID, ISSUE, HANDLING, SOURCE_VALUES FROM {DB}.SILVER.DQ_EXCEPTIONS"), hide_index=True)
    with right:
        st.subheader("Source-to-report reconciliation")
        recon = q(f"SELECT * FROM {DB}.GOLD.RECONCILIATION")
        if not recon.empty and bool(recon.iloc[0]["IS_RECONCILED"]):
            st.success("Bronze = Silver = Gold + DQ exclusions", icon=":material/check_circle:")
        else:
            st.error("Reconciliation break", icon=":material/error:")
        st.dataframe(recon.T.rename(columns={0: "value"}))

# ---------------------------------------------------------------- Financials
with financials_tab:
    fin = q(f"SELECT * FROM {DB}.GOLD.PROJECT_FINANCIALS ORDER BY FISCAL_MONTH")
    totals = fin.groupby("PROJECT_NAME")[["RECORDED_CAPEX", "RECORDED_OPEX"]].sum().reset_index()
    st.subheader("Recorded CapEx vs OpEx by project")
    st.bar_chart(totals, x="PROJECT_NAME", y=["RECORDED_CAPEX", "RECORDED_OPEX"], stack=True)
    st.subheader("Monthly trend")
    trend = fin.groupby("FISCAL_MONTH")[["RECORDED_CAPEX", "RECORDED_OPEX"]].sum().reset_index()
    st.line_chart(trend, x="FISCAL_MONTH", y=["RECORDED_CAPEX", "RECORDED_OPEX"])
    st.caption("Figures are as recorded in Clarity. Accepted suggestions are recommendations, not postings.")

# ---------------------------------------------------------------- AI evaluation
with eval_tab:
    st.subheader("Which AI function fits? Accuracy on Finance-reviewed examples")
    ev = q(
        f"""WITH latest AS (SELECT MAX(EVAL_TS) TS FROM {DB}.CONTROL.AI_EVAL_RESULTS)
            SELECT METHOD, COUNT(*) EXAMPLES, COUNT_IF(IS_MATCH) CORRECT, ROUND(CORRECT / EXAMPLES, 2) ACCURACY
            FROM {DB}.CONTROL.AI_EVAL_RESULTS r, latest
            WHERE r.EVAL_TS >= DATEADD('minute', -30, latest.TS)
            GROUP BY METHOD ORDER BY ACCURACY DESC"""
    )
    st.dataframe(
        ev,
        hide_index=True,
        column_config={"ACCURACY": st.column_config.ProgressColumn("Accuracy", min_value=0, max_value=1, format="%.0f%%")},
    )
    st.caption(
        "Production path: AI_CLASSIFY with Finance label descriptions picks the label; AI_COMPLETE writes the "
        "rationale and quotes evidence. If they disagree the item is marked INSUFFICIENT EVIDENCE."
    )
    st.subheader("Active classification criteria")
    st.dataframe(
        q(f"""SELECT CRITERIA_VERSION, RULE_ID, RULE_NAME, PRIORITY, IMPLIED_CLASS, STATUS, APPROVED_BY, POLICY_REFERENCE
              FROM {DB}.CONTROL.CLASSIFICATION_CRITERIA ORDER BY CRITERIA_VERSION, PRIORITY"""),
        hide_index=True,
    )

# ---------------------------------------------------------------- Audit
with audit_tab:
    st.subheader("Decision history (append-only)")
    st.dataframe(
        q(
            f"""SELECT DECIDED_TS, COST_LINE_ID, DECISION, DECIDED_BY, REVIEWER_COMMENT, RECORDED_CLASS,
                       PROPOSED_CLASS, CRITERIA_VERSION, MODEL, PROMPT_VERSION, SOURCE_SNAPSHOT
                FROM {DB}.REVIEW.DECISION_HISTORY ORDER BY DECIDED_TS DESC"""
        ),
        hide_index=True,
    )
    st.subheader("Superseded suggestions")
    st.caption("When new evidence arrives the prior suggestion is retired, not edited.")
    st.dataframe(
        q(
            f"""SELECT COST_LINE_ID, PROPOSED_CLASS, RULE_NAME, CREATED_TS, SUPERSEDED_TS, LEFT(RATIONALE, 200) RATIONALE
                FROM {DB}.REVIEW.AI_SUGGESTION WHERE SUPERSEDED_TS IS NOT NULL ORDER BY SUPERSEDED_TS DESC"""
        ),
        hide_index=True,
    )
