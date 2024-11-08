import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import Label
from tkinter import ttk
import pyodbc

# Connexion à la base de données SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=172.17.235.170;'
    'DATABASE=SMT_MONITORING;'
    'UID=SMT_SVMLogin;'
    'PWD=SVMLogin+2025*;'
)

# Requête SQL pour récupérer les données
query = """
    SELECT UDATE, TIME, RESP, TRANSX_NUMBER
    FROM [SMT_MONITORING].[dbo].[SVISTA_Monitoring_Hist]
"""
cursor = conn.cursor()
cursor.execute(query)

# Récupérer les résultats dans un DataFrame
rows = cursor.fetchall()
df = pd.DataFrame([tuple(row) for row in rows], columns=[col[0] for col in cursor.description])
conn.close()
df['RESP'] = pd.to_numeric(df['RESP'], errors='coerce')

# Préparation des données
df['UDATE'] = df['UDATE'].astype(str)
df['TIME'] = df['TIME'].apply(lambda x: x.zfill(6))
df['DATETIME'] = pd.to_datetime(df['UDATE'] + df['TIME'], format='%Y%m%d%H%M%S', errors='coerce')

df['SUCCESS'] = df['RESP'].apply(lambda x: 1 if x == -1 or x == 0 else 0)

# Filtrer pour la dernière journée disponible
latest_date = df['DATETIME'].dt.date.max()
df_last_day = df[df['DATETIME'].dt.date == latest_date]

# Regrouper les données par heure
df_hourly = df_last_day.set_index('DATETIME').resample('H').agg(
    total_transactions=('SUCCESS', 'size'),
    successful_transactions=('SUCCESS', 'sum'),
    refused_transactions=('SUCCESS', lambda x: x.size - x.sum())
)
df_hourly['Success Rate'] = (df_hourly['successful_transactions'] / df_hourly['total_transactions']) * 100
df_hourly['Refusal Rate'] = (df_hourly['refused_transactions'] / df_hourly['total_transactions']) * 100
df_hourly = df_hourly.reset_index()

# Création de l'application Tkinter
root = tk.Tk()
root.title("Dashboard de Monitoring SVFE - Dernier Jour")

# Frame pour les informations KPI
info_frame = ttk.Frame(root, padding="10")
info_frame.grid(row=0, column=0, sticky="nsew")

# Ajouter les informations KPI
total_transactions = len(df_last_day)
successful_transactions = df_last_day['SUCCESS'].sum()
refused_transactions = len(df_last_day[df_last_day['RESP'] != -1])
success_rate = (successful_transactions / total_transactions) * 100 if total_transactions else 0
refusal_rate = (refused_transactions / total_transactions) * 100 if total_transactions else 0

Label(info_frame, text="Total Transactions:", font=("Arial", 12, "bold"), foreground="#5dade2").grid(row=0, column=0, sticky="w")
Label(info_frame, text=f"{total_transactions}", font=("Arial", 12)).grid(row=0, column=1, sticky="w")

Label(info_frame, text="Transactions Réussies:", font=("Arial", 12, "bold"), foreground="#5dade2").grid(row=1, column=0, sticky="w")
Label(info_frame, text=f"{successful_transactions}", font=("Arial", 12)).grid(row=1, column=1, sticky="w")

Label(info_frame, text="Taux de Réussite:", font=("Arial", 12, "bold"), foreground="#5dade2").grid(row=2, column=0, sticky="w")
Label(info_frame, text=f"{success_rate:.2f}%", font=("Arial", 12)).grid(row=2, column=1, sticky="w")

Label(info_frame, text="Transactions Refusées:", font=("Arial", 12, "bold"), foreground="#5dade2").grid(row=3, column=0, sticky="w")
Label(info_frame, text=f"{refused_transactions}", font=("Arial", 12)).grid(row=3, column=1, sticky="w")

Label(info_frame, text="Taux de Refus:", font=("Arial", 12, "bold"), foreground="#5dade2").grid(row=4, column=0, sticky="w")
Label(info_frame, text=f"{refusal_rate:.2f}%", font=("Arial", 12)).grid(row=4, column=1, sticky="w")

# Frame pour les graphiques
graph_frame = ttk.Frame(root, padding="10")
graph_frame.grid(row=1, column=0, sticky="nsew")

# Fonction pour tracer le graphique line chart
def plot_line_chart(df, x_column, y_columns, title, ylabel, colors):
    plt.figure(figsize=(12, 6))
    for y_column, color in zip(y_columns, colors):
        plt.plot(df[x_column], df[y_column], marker='o', label=y_column, color=color)
    plt.title(title)
    plt.xlabel('Heure')
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    
    canvas = FigureCanvasTkAgg(plt.gcf(), master=graph_frame)
    canvas.draw()
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)

# Tracer le graphique pour le taux de réussite et de refus par heure
plot_line_chart(
    df_hourly, 
    'DATETIME', 
    ['Success Rate', 'Refusal Rate'], 
    'Taux de Réussite et de Refus par Heure - Dernier Jour', 
    'Taux (%)', 
    ['blue', 'red']
)

# Lancer l'application Tkinter
root.mainloop()
