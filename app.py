import streamlit as st
import pandas as pd
import numpy as np
import uuid
import snowflake.connector

st.set_page_config(page_title="Rally Racing Manager", page_icon="🏁", layout="centered")

# connecting SnowFlake

@st.cache_resource
def get_connection():
    cfg = st.secrets["snowflake"]
    cnx = snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        warehouse=cfg["warehouse"],
        role=cfg["role"],
    )
    
    cnx.cursor().execute(f"USE DATABASE {cfg.get('database','BOOTCAMP_RALLY')}")
    cnx.cursor().execute(f"USE SCHEMA {cfg.get('schema','CORE')}")
    return cnx

def run(sql: str, params=None, fetch: str | None = None):
    """Выполнить SQL; fetch='all' вернёт DataFrame."""
    cur = get_connection().cursor()
    try:
        cur.execute(sql, params or [])
        if fetch == "all":
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame()
        return None
    finally:
        cur.close()

def init_schema_if_missing():
    ddls = [
        "CREATE DATABASE IF NOT EXISTS BOOTCAMP_RALLY",
        "CREATE SCHEMA IF NOT EXISTS BOOTCAMP_RALLY.CORE",
        """
        CREATE TABLE IF NOT EXISTS BOOTCAMP_RALLY.CORE.TEAMS (
          TEAM_ID INTEGER AUTOINCREMENT PRIMARY KEY,
          TEAM_NAME VARCHAR NOT NULL UNIQUE,
          MEMBERS VARCHAR,
          BUDGET NUMBER(12,2) DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS BOOTCAMP_RALLY.CORE.CARS (
          CAR_ID INTEGER AUTOINCREMENT PRIMARY KEY,
          CAR_NAME VARCHAR NOT NULL,
          TOP_SPEED_KMH NUMBER(6,2) NOT NULL,
          ACCEL_0_100_S NUMBER(5,2) NOT NULL,
          RELIABILITY NUMBER(4,3) NOT NULL,
          HANDLING NUMBER(5,2) NOT NULL,
          WEIGHT_KG NUMBER(7,2) NOT NULL,
          TEAM_ID INTEGER NULL REFERENCES BOOTCAMP_RALLY.CORE.TEAMS(TEAM_ID)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS BOOTCAMP_RALLY.CORE.RACES (
          RACE_UID STRING PRIMARY KEY,
          TRACK_NAME VARCHAR,
          DISTANCE_KM NUMBER(6,2) NOT NULL,
          FEE_USD NUMBER(12,2) NOT NULL,
          PRIZE_POOL_USD NUMBER(12,2) NOT NULL,
          CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS BOOTCAMP_RALLY.CORE.RACE_RESULTS (
          RACE_UID STRING NOT NULL,
          CAR_ID INTEGER NOT NULL,
          TEAM_ID INTEGER NOT NULL,
          FINISH_TIME_MIN NUMBER(10,3) NOT NULL,
          AVG_SPEED_KMH  NUMBER(7,2) NOT NULL,
          POSITION INTEGER NOT NULL,
          PRIZE_USD NUMBER(12,2) NOT NULL,
          PRIMARY KEY (RACE_UID, CAR_ID)
        )
        """,
    ]
    for stmt in ddls:
        run(stmt)

def get_teams_df():
    return run("SELECT TEAM_ID, TEAM_NAME, MEMBERS, BUDGET FROM TEAMS ORDER BY TEAM_NAME", fetch="all")

def get_cars_df(include_unassigned=True):
    sql = """
    SELECT C.CAR_ID, C.CAR_NAME, C.TOP_SPEED_KMH, C.ACCEL_0_100_S, C.RELIABILITY,
           C.HANDLING, C.WEIGHT_KG, C.TEAM_ID, T.TEAM_NAME
      FROM CARS C
      LEFT JOIN TEAMS T ON T.TEAM_ID = C.TEAM_ID
      ORDER BY C.CAR_ID DESC
    """
    if not include_unassigned:
        sql = sql.replace("LEFT JOIN", "JOIN")
    return run(sql, fetch="all")

# UI part
st.title("🏁 Bootcamp Rally Racing Manager")
st.caption("Snowflake + Python + Streamlit — manage cars & teams, then run a 100 km race!")

init_schema_if_missing()

tabs = st.tabs(["Teams", "Cars", "Assign", "Start race!", "History"])

# TEAMS part
with tabs[0]:
    st.subheader("Manage Teams")
    with st.form("add_team"):
        team_name = st.text_input("Team name")
        members = st.text_area("Members (comma-separated)", placeholder="Alice,Bob")
        budget = st.number_input("Initial budget (USD)", min_value=0.0, value=10000.0, step=500.0)
        if st.form_submit_button("➕ Add team"):
            if not team_name.strip():
                st.error("Team name is required.")
            else:
                try:
                    run("INSERT INTO TEAMS (TEAM_NAME, MEMBERS, BUDGET) VALUES (%s, %s, %s)",
                        [team_name.strip(), members.strip(), budget])
                    st.success(f"Team '{team_name}' added.")
                except Exception as e:
                    st.error(f"Failed to add team: {e}")
    st.dataframe(get_teams_df(), use_container_width=True)

# CARS
with tabs[1]:
    st.subheader("Manage Cars")
    teams_df = get_teams_df()
    team_options = {row["TEAM_NAME"]: int(row["TEAM_ID"]) for _, row in teams_df.iterrows()} if not teams_df.empty else {}

    with st.form("add_car"):
        c_name = st.text_input("Car name", placeholder="Lightning X")
        c_top = st.number_input("Top speed (km/h)", min_value=100.0, max_value=500.0, value=280.0, step=1.0)
        c_acc = st.number_input("0-100 km/h (s)", min_value=1.0, max_value=10.0, value=3.6, step=0.1)
        c_rel = st.slider("Reliability (0..1)", min_value=0.5, max_value=1.0, value=0.92, step=0.01)
        c_hand = st.slider("Handling (0..100)", min_value=50, max_value=100, value=88, step=1)
        c_weight = st.number_input("Weight (kg)", min_value=600.0, max_value=2000.0, value=1200.0, step=10.0)
        chosen_team = st.selectbox("Assign to team (optional)", ["— Unassigned —"] + list(team_options.keys()))
        if st.form_submit_button("➕ Add car"):
            try:
                team_id = None if chosen_team == "— Unassigned —" else team_options[chosen_team]
                run(
                    "INSERT INTO CARS (CAR_NAME, TOP_SPEED_KMH, ACCEL_0_100_S, RELIABILITY, HANDLING, WEIGHT_KG, TEAM_ID)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [c_name.strip(), c_top, c_acc, c_rel, c_hand, c_weight, team_id]
                )
                st.success(f"Car '{c_name}' added.")
            except Exception as e:
                st.error(f"Failed to add car: {e}")
    st.dataframe(get_cars_df(True), use_container_width=True)


with tabs[2]:
    st.subheader("Assign Car to Team")
    cars_df = get_cars_df(True)
    teams_df = get_teams_df()

    if cars_df.empty or teams_df.empty:
        st.info("Add at least one car and one team first.")
    else:
        car_map = {f"[#{row['CAR_ID']}] {row['CAR_NAME']}": int(row["CAR_ID"]) for _, row in cars_df.iterrows()}
        team_map = {row["TEAM_NAME"]: int(row["TEAM_ID"]) for _, row in teams_df.iterrows()}
        sel_car = st.selectbox("Car", list(car_map.keys()))
        sel_team = st.selectbox("Team", list(team_map.keys()))
        if st.button("🔗 Assign"):
            try:
                run("UPDATE CARS SET TEAM_ID = %s WHERE CAR_ID = %s", [team_map[sel_team], car_map[sel_car]])
                st.success("Assignment updated.")
            except Exception as e:
                st.error(f"Failed to assign: {e}")
        st.dataframe(get_cars_df(True), use_container_width=True)

with tabs[3]:
    st.subheader("Start race! 🏎️")
    st.caption("All cars **with a team** participate. Each entry pays a fee; prizes go to top finishers.")

    track = st.text_input("Track name", "Riga Street Circuit")
    distance = st.number_input("Distance (km)", min_value=10.0, max_value=1000.0, value=100.0, step=10.0)
    fee = st.number_input("Participation fee per car (USD)", min_value=0.0, max_value=100000.0, value=1000.0, step=100.0)
    prize_pool = st.number_input("Prize pool (USD)", min_value=0.0, max_value=1000000.0, value=10000.0, step=500.0)
    prize_split = [0.6, 0.3, 0.1]

    if st.button("🏁 Start race!"):
        cars = run(
            """
            SELECT C.CAR_ID, C.CAR_NAME, C.TOP_SPEED_KMH, C.ACCEL_0_100_S, C.RELIABILITY,
                   C.HANDLING, C.WEIGHT_KG, T.TEAM_ID, T.TEAM_NAME
              FROM CARS C
              JOIN TEAMS T ON T.TEAM_ID = C.TEAM_ID
            """,
            fetch="all"
        )
        if cars.empty:
            st.error("No cars assigned to teams.")
        else:
            race_uid = str(uuid.uuid4())
            results = []
            for _, row in cars.iterrows():
                top = float(row["TOP_SPEED_KMH"])
                accel = float(row["ACCEL_0_100_S"])
                rel = float(row["RELIABILITY"])
                hand = float(row["HANDLING"])

                accel_factor = max(0.8, min(1.2, 1.4 - 0.1 * accel)) 
                handling_factor = 0.9 + (hand / 1000.0)              
                randomness = np.random.normal(1.0, 0.07)  

                effective_speed = 0.7 * top * accel_factor * handling_factor * randomness
                if np.random.rand() > rel:                          
                    effective_speed *= np.random.uniform(0.7, 0.9)

                effective_speed = max(60.0, min(effective_speed, top))
                time_min = (float(distance) / effective_speed) * 60.0
                results.append({
                    "CAR_ID": int(row["CAR_ID"]),
                    "CAR_NAME": row["CAR_NAME"],
                    "TEAM_ID": int(row["TEAM_ID"]),
                    "TEAM_NAME": row["TEAM_NAME"],
                    "AVG_SPEED_KMH": round(effective_speed, 2),
                    "FINISH_TIME_MIN": round(time_min, 3),
                })

            results.sort(key=lambda x: x["FINISH_TIME_MIN"])
            for i, r in enumerate(results, start=1):
                r["POSITION"] = i

            prizes = [round(prize_pool * p, 2) for p in prize_split]
            for r in results:
                r["PRIZE_USD"] = prizes[r["POSITION"] - 1] if r["POSITION"] <= len(prizes) else 0.0

            try:
                run("BEGIN")
                run(
                    "INSERT INTO RACES (RACE_UID, TRACK_NAME, DISTANCE_KM, FEE_USD, PRIZE_POOL_USD) VALUES (%s, %s, %s, %s, %s)",
                    [race_uid, track, distance, fee, prize_pool]
                )
                for r in results:
                    run(
                        "INSERT INTO RACE_RESULTS (RACE_UID, CAR_ID, TEAM_ID, FINISH_TIME_MIN, AVG_SPEED_KMH, POSITION, PRIZE_USD)"
                        " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        [race_uid, r["CAR_ID"], r["TEAM_ID"], r["FINISH_TIME_MIN"], r["AVG_SPEED_KMH"], r["POSITION"], r["PRIZE_USD"]]
                    )

                df = pd.DataFrame(results)
                fees = df.groupby("TEAM_ID")["CAR_ID"].count().rename("entries").reset_index()
                fees["fee_total"] = fees["entries"] * fee
                prizes_team = df.groupby("TEAM_ID")["PRIZE_USD"].sum().reset_index()

                merged = pd.merge(fees, prizes_team, on="TEAM_ID", how="left").fillna({"PRIZE_USD": 0.0})
                for _, row in merged.iterrows():
                    delta = float(row["PRIZE_USD"] - row["fee_total"])
                    run("UPDATE TEAMS SET BUDGET = BUDGET + %s WHERE TEAM_ID = %s", [delta, int(row["TEAM_ID"])])

                run("COMMIT")
            except Exception as e:
                run("ROLLBACK")
                st.error(f"Failed to persist race: {e}")
            else:
                st.success(f"Race completed: {track} ({distance} km). RACE_UID: {race_uid}")
                st.write("### Results")
                res_df = pd.DataFrame(results)[["POSITION", "TEAM_NAME", "CAR_NAME", "AVG_SPEED_KMH", "FINISH_TIME_MIN", "PRIZE_USD"]]
                st.dataframe(res_df, use_container_width=True)

                st.write("### Updated Team Budgets")
                st.dataframe(get_teams_df(), use_container_width=True)

with tabs[4]:
    st.subheader("Race History")
    races = run(
        "SELECT RACE_UID, TRACK_NAME, DISTANCE_KM, FEE_USD, PRIZE_POOL_USD, CREATED_AT FROM RACES ORDER BY CREATED_AT DESC",
        fetch="all"
    )
    st.dataframe(races, use_container_width=True)

    sel = st.selectbox("Show results for RACE_UID", races["RACE_UID"].tolist() if not races.empty else [])
    if sel:
        rr = run(
            """
            SELECT RR.POSITION, T.TEAM_NAME, C.CAR_NAME, RR.AVG_SPEED_KMH, RR.FINISH_TIME_MIN, RR.PRIZE_USD
              FROM RACE_RESULTS RR
              JOIN CARS C  ON C.CAR_ID  = RR.CAR_ID
              JOIN TEAMS T ON T.TEAM_ID = RR.TEAM_ID
             WHERE RR.RACE_UID = %s
             ORDER BY RR.POSITION
            """,
            [sel], fetch="all"
        )
        st.dataframe(rr, use_container_width=True)

st.caption("Add cars via UI; only cars with a team will race.")
