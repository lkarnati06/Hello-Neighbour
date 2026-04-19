import pandas as pd
import numpy as np
import os

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename


folder = "./events"
os.makedirs(folder, exist_ok=True)

REQUIRED_COLUMNS = {"Name", "Email", "Number"}


def load_volunteers():
    rows = []
    for filename in os.listdir(folder):
        if not filename.endswith(".csv"):
            continue

        event_name = filename.replace(".csv", "").replace("_", " ")
        path = os.path.join(folder, filename)

        # Try to read the file — skip it if pandas fails for any reason
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"[skip] {filename}: could not read ({e})")
            continue

        # Skip files that don't have the columns we need
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            print(f"[skip] {filename}: missing columns {missing} — has {list(df.columns)}")
            continue

        for _, person in df.iterrows():
            rows.append({
                "Name": person["Name"],
                "Email": person["Email"],
                "Number": person["Number"],
                "Event": event_name
            })

    return pd.DataFrame(rows)


# Your analytics functions — unchanged
def repeat_events(df):
    counts = df.groupby('user_id')['Event'].nunique()
    return [counts >= 2].mean()

def avg_events_per_user(df):
    counts = df.groupby('user_id')['Event'].count()
    return counts.mean()

def total_volunteers(df):
    grouped = df.groupby('Event')["Name"].apply(set)
    sum = 0
    for _ in grouped:
        sum += len(_)
    return sum

def sum_per_event(df):
    counts = df.groupby('user_id')['Event'].count()
    return counts


# Flask setup
app = Flask(__name__)
CORS(app)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


@app.route("/api/dashboard")
def dashboard():
    df = load_volunteers()
    events = [{"Name": e, "Date": "", "Status": "done", "DocURL": "#"}
              for e in df["Event"].unique()] if len(df) else []
    return jsonify({
        "volunteers": df.to_dict(orient="records"),
        "events": events,
    })


@app.route("/api/upload", methods=["POST"])
def upload():
    saved = []
    for key in request.files:
        for file in request.files.getlist(key):
            if not file.filename:
                continue
            safe_name = secure_filename(os.path.basename(file.filename))
            if not safe_name.endswith(".csv"):
                continue
            file.save(os.path.join(folder, safe_name))
            saved.append(safe_name)

    df = load_volunteers()
    events = [{"Name": e, "Date": "", "Status": "done", "DocURL": "#"}
              for e in df["Event"].unique()] if len(df) else []
    return jsonify({
        "saved": saved,
        "volunteers": df.to_dict(orient="records"),
        "events": events,
    })


@app.route("/api/clear", methods=["POST"])
def clear():
    removed = 0
    for f in os.listdir(folder):
        if f.endswith(".csv"):
            os.remove(os.path.join(folder, f))
            removed += 1
    return jsonify({"removed": removed})


if __name__ == "__main__":
    app.run(port=5000, debug=True)