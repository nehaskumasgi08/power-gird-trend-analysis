# PowerGrid Trend Analysis

A local academic demonstration platform for power-grid time-series analytics. The browser dashboard uses generated/sample telemetry. Actual distributed Spark RDD execution is available through the optional Flask + PySpark backend and is clearly identified separately.

## 1. Run the dashboard with VS Code Live Server (frontend)

1. Extract `power-grid-trend-analysis.zip`.
2. Open the extracted `power-grid-trend-analysis` folder in VS Code (**File → Open Folder**).
3. Install the **Live Server** extension by Ritwick Dey.
4. In Explorer, open `frontend/index.html`.
5. Right-click the file and choose **Open with Live Server**.
6. The dashboard opens in your browser (usually `http://127.0.0.1:5500/frontend/`).

The frontend works in demonstration mode without the backend. Internet access is required for the Chart.js and Google Fonts CDN resources.

## 2. Run the Python API and actual PySpark (optional but recommended)

Live Server only serves the frontend; it does not run Python or Spark. Keep Live Server open, then open a second VS Code terminal:

```bash
# from the project root
python3 -m venv .venv
source .venv/bin/activate          # macOS / Linux
# Windows PowerShell: .venv\\Scripts\\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
python backend/app.py
```

The API runs at `http://127.0.0.1:5000`. Open the Spark Performance page and click **Run Spark benchmark**. The API executes real PySpark RDD `map`, `filter`, `reduceByKey`, `aggregateByKey`, `join`, partition-count tests and lineage inspection. Timings are measurements from your machine and will vary.

**Java prerequisite:** PySpark requires a compatible Java runtime. Install a supported JDK (Java 17 is a practical choice for current Spark 3.5 releases), set `JAVA_HOME`, then reopen the terminal. Verify with `java -version`.

If PySpark/Java is not installed, the rest of the frontend still runs with sample data; the Spark page reports that the backend is unavailable instead of inventing Spark results.

## 3. Data

`data/grid_load.csv` contains 35,042 rows: 35,040 hourly zone readings plus two intentionally invalid records. Schema: `timestamp, zone_id, load_mw, voltage_kv`. The browser validates uploaded CSVs and reports rejected rows. Sample telemetry is not a real utility feed.

## 4. Project structure

```text
power-grid-trend-analysis/
├── frontend/
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── backend/
│   └── app.py
├── data/
│   └── grid_load.csv
├── requirements.txt
└── README.md
```

## Troubleshooting

- **Live Server option missing:** confirm the Live Server extension is installed, then right-click `frontend/index.html`.
- **Dashboard charts blank:** check internet access because Chart.js is loaded from a CDN.
- **Spark benchmark says backend unavailable:** make sure `python backend/app.py` is still running in the second terminal.
- **CORS/network error:** use the provided Flask backend on `127.0.0.1:5000`; do not open the HTML by double-clicking it.
- **JAVA_GATEWAY_EXITED:** install/configure Java and verify `JAVA_HOME` and `java -version`.
