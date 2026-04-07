import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from faker import Faker
import random
from datetime import timedelta

# 1. AYARLAR VE BAĞLANTI
fake = Faker()
DB_NAME = 'logistics_analysis.db'
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

print("--- Lojistik Veri Ambarı Projesi Başlatıldı ---")

# 2. VERİ TABANI ŞEMASININ OLUŞTURULMASI
# Endüstri Mühendisi Bakış Açısı: İlişkisel tablo yapısı (Normalization)
cursor.executescript('''
DROP TABLE IF EXISTS Costs;
DROP TABLE IF EXISTS Shipments;
DROP TABLE IF EXISTS Ports;

CREATE TABLE Ports (
    port_id INTEGER PRIMARY KEY,
    port_name TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE Shipments (
    shipment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    origin_port_id INTEGER,
    dest_port_id INTEGER,
    departure_date TEXT, -- Python 3.12 uyumu için TEXT (ISO format)
    arrival_date TEXT,
    status TEXT,
    FOREIGN KEY (origin_port_id) REFERENCES Ports (port_id),
    FOREIGN KEY (dest_port_id) REFERENCES Ports (port_id)
);

CREATE TABLE Costs (
    cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
    shipment_id INTEGER,
    freight_cost REAL,
    custom_fee REAL,
    inland_transport REAL,
    FOREIGN KEY (shipment_id) REFERENCES Shipments (shipment_id)
);
''')

# 3. STATİK VERİLERİN (LİMANLAR) EKLENMESİ
ports_data = [
    (1, 'Shanghai', 'China'), (2, 'Istanbul', 'Turkey'),
    (3, 'Rotterdam', 'Netherlands'), (4, 'Los Angeles', 'USA'),
    (5, 'Hamburg', 'Germany'), (6, 'Singapore', 'Singapore')
]
cursor.executemany('INSERT INTO Ports VALUES (?,?,?)', ports_data)

# 4. SENTETİK VERİ ÜRETİMİ (DATA GENERATION)
# Python 3.12 DeprecationWarning hatası .isoformat() ile giderildi.
print("Veri üretiliyor (1000 kayıt)...")
for _ in range(1000):
    dep_obj = fake.date_between(start_date='-1y', end_date='today')
    arr_obj = dep_obj + timedelta(days=random.randint(15, 45))

    # Tarihleri string (YYYY-MM-DD) olarak saklıyoruz
    dep_date = dep_obj.isoformat()
    arr_date = arr_obj.isoformat()

    cursor.execute('''
    INSERT INTO Shipments (customer_name, origin_port_id, dest_port_id, departure_date, arrival_date, status)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (fake.company(), random.randint(1, 6), random.randint(1, 6), dep_date, arr_date, 'Delivered'))

    ship_id = cursor.lastrowid

    # Maliyetler: Navlun, Gümrük ve İç Taşıma
    cursor.execute('''
    INSERT INTO Costs (shipment_id, freight_cost, custom_fee, inland_transport)
    VALUES (?, ?, ?, ?)
    ''', (ship_id, random.uniform(2500, 7500), random.uniform(150, 800), random.uniform(400, 1200)))

conn.commit()

# 5. ANALİZ VE KPI HESAPLAMA (SQL + PANDAS)
# KPI: Ortalama Teslimat Süresi (Lead Time) ve Toplam Maliyet
query = """
SELECT 
    p.port_name as Origin,
    COUNT(s.shipment_id) as Total_Shipments,
    ROUND(AVG(JULIANDAY(s.arrival_date) - JULIANDAY(s.departure_date)), 2) as Avg_Lead_Time,
    ROUND(AVG(c.freight_cost + c.custom_fee + c.inland_transport), 2) as Avg_Total_Cost
FROM Shipments s
JOIN Ports p ON s.origin_port_id = p.port_id
JOIN Costs c ON s.shipment_id = c.shipment_id
GROUP BY p.port_name
ORDER BY Avg_Total_Cost DESC;
"""

df = pd.read_sql_query(query, conn)
print("\n--- ANALİZ SONUÇLARI (İLK 5 SATIR) ---")
print(df.head())

# 6. GÖRSELLEŞTİRME VE ÇIKTI ALMA
plt.figure(figsize=(12, 6))
sns.set_theme(style="whitegrid")

# İki eksenli (Dual-Axis) grafik: Hem Maliyet hem Süre
ax = sns.barplot(x='Origin', y='Avg_Total_Cost', data=df, hue='Origin', palette='magma', alpha=0.8, legend=False)
ax2 = ax.twinx()
sns.lineplot(x='Origin', y='Avg_Lead_Time', data=df, marker='o', color='red', ax=ax2, label='Avg Lead Time (Days)')

ax.set_title('Liman Bazlı Maliyet ve Teslimat Süresi Analizi', fontsize=15)
ax.set_ylabel('Ortalama Toplam Maliyet (USD)', fontsize=12)
ax2.set_ylabel('Ortalama Teslimat Süresi (Gün)', fontsize=12)

# Grafiği kaydet
plt.savefig('logistic_kpi_visual.png', dpi=300, bbox_inches='tight')
print("\nGrafik 'logistic_kpi_visual.png' adıyla kaydedildi.")

conn.close()
print("İşlem başarıyla tamamlandı.")