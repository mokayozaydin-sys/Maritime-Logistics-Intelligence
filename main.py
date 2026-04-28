import os
import sys
import sqlite3
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import random
import matplotlib.ticker as ticker

# ==========================================
# MODULE 1: CONFIGURATION & LOGGING
# ==========================================
DB_NAME = 'ocean_logistics_dw.db'
CSV_FILE = 'cleaned_maritime_data_v4_realistic.csv'

# 5 Different Output Report Files
IMG_COST = '01_origin_cost_report.png'
IMG_TIME = '02_origin_time_report.png'
IMG_DEST = '03_destination_density_report.png'
IMG_ROUTE = '04_popular_routes_report.png'
IMG_PIE = '05_cost_breakdown_pie_chart.png'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


# ==========================================
# MODULE 2 & 3: DATABASE SETUP & ETL
# ==========================================
def setup_and_load_data():
    """Sets up the Star Schema and loads cleaned data from CSV to SQLite."""
    if not os.path.exists(CSV_FILE):
        logger.error(f"Error: '{CSV_FILE}' not found. Please check the file path.")
        sys.exit(1)

    # Read and drop rows with missing essential data
    df = pd.read_csv(CSV_FILE).dropna(subset=['Origin_Port', 'Destination_Port', 'Date', 'Predicted_ETA_Date'])

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        # Initialize Tables
        cursor.executescript('''
            DROP TABLE IF EXISTS Costs; DROP TABLE IF EXISTS Shipments; DROP TABLE IF EXISTS Ports;
            CREATE TABLE Ports (port_id INTEGER PRIMARY KEY AUTOINCREMENT, port_name TEXT NOT NULL UNIQUE);
            CREATE TABLE Shipments (
                shipment_id INTEGER PRIMARY KEY AUTOINCREMENT, tracking_number TEXT,
                origin_port_id INTEGER, dest_port_id INTEGER, departure_date TEXT, arrival_date TEXT,
                FOREIGN KEY (origin_port_id) REFERENCES Ports (port_id), FOREIGN KEY (dest_port_id) REFERENCES Ports (port_id)
            );
            CREATE TABLE Costs (
                cost_id INTEGER PRIMARY KEY AUTOINCREMENT, shipment_id INTEGER,
                freight_cost REAL, custom_fee REAL, inland_transport REAL,
                FOREIGN KEY (shipment_id) REFERENCES Shipments (shipment_id)
            );
        ''')

        # Insert Unique Ports
        all_ports = pd.concat([df['Origin_Port'], df['Destination_Port']]).unique()
        cursor.executemany("INSERT OR IGNORE INTO Ports (port_name) VALUES (?)", [(p,) for p in all_ports])

        cursor.execute("SELECT port_name, port_id FROM Ports")
        port_map = dict(cursor.fetchall())

        # Insert Shipments
        shipments = [(str(row['Shipment_ID']), port_map.get(row['Origin_Port']), port_map.get(row['Destination_Port']),
                      str(row['Date']), str(row['Predicted_ETA_Date']))
                     for _, row in df.iterrows() if
                     port_map.get(row['Origin_Port']) and port_map.get(row['Destination_Port'])]
        cursor.executemany(
            'INSERT INTO Shipments (tracking_number, origin_port_id, dest_port_id, departure_date, arrival_date) VALUES (?, ?, ?, ?, ?)',
            shipments)

        # Insert Synthetic Costs
        cursor.execute("SELECT shipment_id FROM Shipments")
        costs = [(s_id[0], round(random.uniform(2500, 7500), 2), round(random.uniform(150, 800), 2),
                  round(random.uniform(400, 1200), 2)) for s_id in cursor.fetchall()]
        cursor.executemany(
            'INSERT INTO Costs (shipment_id, freight_cost, custom_fee, inland_transport) VALUES (?, ?, ?, ?)', costs)


# ==========================================
# MODULE 4: SQL ANALYTICS (DATA FETCHING)
# ==========================================
def fetch_all_reports():
    """Executes 4 different SQL queries to generate DataFrames for KPI Dashboards."""
    with sqlite3.connect(DB_NAME) as conn:
        # Report 1 & 2: Origin Port (Cost and Lead Time)
        query_origin = """
            SELECT p.port_name as Origin_Port, COUNT(s.shipment_id) as Total_Shipments,
                   ROUND(AVG(ABS(JULIANDAY(s.arrival_date) - JULIANDAY(s.departure_date))), 1) as Avg_Lead_Time_Days,
                   ROUND(AVG(c.freight_cost + c.custom_fee + c.inland_transport), 0) as Avg_Total_Cost_USD
            FROM Shipments s JOIN Ports p ON s.origin_port_id = p.port_id
            JOIN Costs c ON s.shipment_id = c.shipment_id 
            GROUP BY p.port_name HAVING Total_Shipments > 0
        """
        df_origin = pd.read_sql_query(query_origin, conn)

        # Report 3: Destination Port Density
        query_dest = """
            SELECT p.port_name as Destination_Port, COUNT(s.shipment_id) as Inbound_Shipments
            FROM Shipments s JOIN Ports p ON s.dest_port_id = p.port_id
            GROUP BY p.port_name ORDER BY Inbound_Shipments DESC LIMIT 10
        """
        df_dest = pd.read_sql_query(query_dest, conn)

        # Report 4: Most Popular Routes (Origin -> Destination)
        query_routes = """
            SELECT po.port_name || ' ➔ ' || pd.port_name as Route, COUNT(s.shipment_id) as Total_Shipments
            FROM Shipments s
            JOIN Ports po ON s.origin_port_id = po.port_id
            JOIN Ports pd ON s.dest_port_id = pd.port_id
            GROUP BY Route ORDER BY Total_Shipments DESC LIMIT 10
        """
        df_routes = pd.read_sql_query(query_routes, conn)

        # Report 5: Overall Average Cost Breakdown
        query_costs = """
            SELECT 
                ROUND(AVG(freight_cost), 2) as Avg_Freight_Cost, 
                ROUND(AVG(custom_fee), 2) as Avg_Customs_Fee, 
                ROUND(AVG(inland_transport), 2) as Avg_Inland_Transport 
            FROM Costs
        """
        df_costs = pd.read_sql_query(query_costs, conn)

    return df_origin, df_dest, df_routes, df_costs


# ==========================================
# MODULE 5: TERMINAL OUTPUTS & VISUALIZATION
# ==========================================
def generate_reports(df_origin, df_dest, df_routes, df_costs):
    """Generates terminal tables and saves 5 distinct charts as PNG files."""
    sns.set_theme(style="whitegrid", rc={"axes.edgecolor": "#cccccc"})

    # --- REPORT 1: ORIGIN PORT COST ---
    df_cost = df_origin.sort_values(by='Avg_Total_Cost_USD', ascending=False).head(10)
    print("\n[TABLE 1] TOP 10 HIGHEST COST ORIGIN PORTS\n" + "-" * 50)
    print(df_cost[['Origin_Port', 'Total_Shipments', 'Avg_Total_Cost_USD']].to_string(index=False))

    plt.figure(figsize=(10, 5))
    ax1 = sns.barplot(x='Origin_Port', y='Avg_Total_Cost_USD', data=df_cost, palette='Blues_r')

    for container in ax1.containers:
        ax1.bar_label(container, fmt='$%d', padding=3, fontweight='bold')

    plt.title('💸 Average Cost by Origin Port (USD)', fontweight='bold', fontsize=13)
    plt.ylim(0, df_cost['Avg_Total_Cost_USD'].max() * 1.2)
    plt.xlabel('Origin Port', fontweight='bold')
    plt.ylabel('Average Cost (USD)', fontweight='bold')
    plt.tight_layout()
    plt.savefig(IMG_COST, dpi=300)
    plt.close()

    # --- REPORT 2: ORIGIN PORT LEAD TIME ---
    df_time = df_origin.sort_values(by='Avg_Lead_Time_Days', ascending=False).head(10)
    print("\n[TABLE 2] TOP 10 SLOWEST ORIGIN PORTS\n" + "-" * 50)
    print(df_time[['Origin_Port', 'Total_Shipments', 'Avg_Lead_Time_Days']].to_string(index=False))

    plt.figure(figsize=(10, 5))
    ax2 = sns.barplot(x='Origin_Port', y='Avg_Lead_Time_Days', data=df_time, palette='Oranges_r')

    for container in ax2.containers:
        ax2.bar_label(container, fmt='%.1f', padding=3, fontweight='bold')

    plt.title('⏳ Average Lead Time by Origin Port (Days)', fontweight='bold', fontsize=13)
    plt.ylim(0, df_time['Avg_Lead_Time_Days'].max() * 1.2)
    plt.xlabel('Origin Port', fontweight='bold')
    plt.ylabel('Average Lead Time (Days)', fontweight='bold')
    plt.tight_layout()
    plt.savefig(IMG_TIME, dpi=300)
    plt.close()

    # --- REPORT 3: DESTINATION PORT DENSITY ---
    print("\n[TABLE 3] TOP 10 BUSIEST DESTINATION PORTS\n" + "-" * 50)
    print(df_dest.to_string(index=False))

    plt.figure(figsize=(10, 5))
    ax3 = sns.barplot(x='Destination_Port', y='Inbound_Shipments', data=df_dest, palette='Greens_r')

    for container in ax3.containers:
        ax3.bar_label(container, padding=3, fontweight='bold')

    plt.title('⚓ Destination Port Operation Density (Shipment Count)', fontweight='bold', fontsize=13)
    plt.ylim(0, df_dest['Inbound_Shipments'].max() * 1.2)
    plt.xlabel('Destination Port', fontweight='bold')
    plt.ylabel('Total Inbound Shipments', fontweight='bold')
    plt.tight_layout()
    plt.savefig(IMG_DEST, dpi=300)
    plt.close()

    # --- REPORT 4: MOST POPULAR ROUTES (Horizontal Bar) ---
    print("\n[TABLE 4] TOP 10 MOST FREQUENT ROUTES\n" + "-" * 50)
    print(df_routes.to_string(index=False))

    plt.figure(figsize=(10, 6))
    ax4 = sns.barplot(x='Total_Shipments', y='Route', data=df_routes, palette='Purples_r')

    for container in ax4.containers:
        ax4.bar_label(container, padding=3, fontweight='bold')

    plt.title('🚢 Most Active Logistics Routes (Origin ➔ Dest)', fontweight='bold', fontsize=13)
    plt.xlim(0, df_routes['Total_Shipments'].max() * 1.2)
    plt.xlabel('Total Shipments', fontweight='bold')
    plt.ylabel('Route', fontweight='bold')
    plt.tight_layout()
    plt.savefig(IMG_ROUTE, dpi=300)
    plt.close()

    # --- REPORT 5: COST BREAKDOWN (Pie Chart) ---
    print("\n[TABLE 5] OVERALL AVERAGE COST BREAKDOWN (USD)\n" + "-" * 50)
    print(df_costs.to_string(index=False))

    plt.figure(figsize=(7, 7))
    labels = ['Freight Cost', 'Customs Fee', 'Inland Transport']
    sizes = [df_costs['Avg_Freight_Cost'][0], df_costs['Avg_Customs_Fee'][0], df_costs['Avg_Inland_Transport'][0]]
    colors = ['#3498db', '#e74c3c', '#f1c40f']

    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140,
            wedgeprops={'edgecolor': 'white', 'linewidth': 2}, textprops={'fontweight': 'bold', 'fontsize': 11})
    plt.title('💰 Logistics Operation Cost Breakdown', fontweight='bold', pad=20, fontsize=14)
    plt.tight_layout()
    plt.savefig(IMG_PIE, dpi=300)
    plt.close()


# ==========================================
# MAIN EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    logger.info("Initializing Logistics Data Warehouse ETL Process...")
    setup_and_load_data()

    logger.info("Calculating KPIs and querying analytical data...")
    df_org, df_dst, df_rt, df_cst = fetch_all_reports()

    logger.info("Generating dashboard reports and visualizations...")
    generate_reports(df_org, df_dst, df_rt, df_cst)

    logger.info("Process Completed Successfully! 5 analytical reports have been generated in your folder.")
