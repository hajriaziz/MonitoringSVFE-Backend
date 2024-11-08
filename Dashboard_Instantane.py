from numpy import roots
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import Label
from tkinter import ttk
from tkinter import messagebox

try:
    conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=172.17.235.170;'  # Remplace par le nom de ton serveur SQL
    'DATABASE=SMT_MONITORING;'
    'UID=SMT_SVMRLogin;'
    'PWD=Monetique2026*;'
)
except pyodbc.Error as e:
    messagebox.showerror("Erreur", f"Erreur de connexion : {str(e)}")
    roots.destroy()  # Fermer l'application si la connexion échoue





# Connexion à la base de données SQL Server
#conn = pyodbc.connect(
    #'DRIVER={ODBC Driver 17 for SQL Server};'
    #'SERVER=172.17.235.170;'  # Remplace par le nom de ton serveur SQL
    #'DATABASE=SMT_MONITORING;'
    #'UID=SMT_SVMRLogin;'
    #'PWD=Monetique2026*;'
#)

# Requête SQL pour récupérer les données
query = """
    SELECT UDATE, TIME, ISS_INST, ACQ_INST, TERMINAL_TYPE, RESP, TRANSX_NUMBER
    FROM [SMT_MONITORING].[dbo].[SVISTA_Monitoring]
"""
# Exécution de la requête avec pyodbc
cursor = conn.cursor()
cursor.execute(query)

# Récupérer toutes les lignes
rows = cursor.fetchall()

# Créer un DataFrame manuellement
df = pd.DataFrame([tuple(row) for row in rows], columns=[col[0] for col in cursor.description])
# Fermer la connexion
conn.close()
#change data type objet to int 
df['RESP'] = pd.to_numeric(df['RESP'], errors='coerce')
df['TERMINAL_TYPE'] = pd.to_numeric(df['TERMINAL_TYPE'], errors='coerce')

# Extraire la date et l'heure du fichier Excel (par exemple, la première date)
first_date_time = pd.to_datetime(df['UDATE'].astype(str) + df['TIME'].astype(str), format='%Y%m%d%H%M%S').min()

# Convertir les colonnes 'UDATE' et 'TIME' en datetime
df['DATETIME'] = pd.to_datetime(df['UDATE'].astype(str) + df['TIME'].astype(str), format='%Y%m%d%H%M%S')

# Extraire uniquement l'heure et la minute de la colonne 'DATETIME'
df['TIME_ONLY'] = df['DATETIME'].dt.strftime('%H:%M')

# Calculer la colonne SUCCESS
df['SUCCESS'] = df['RESP'].apply(lambda x: 1 if x == -1 or x == 0 else 0)

# Calculer les KPI
total_transactions = len(df)
successful_transactions = df['SUCCESS'].sum()
#print(successful_transactions)
success_rate = (successful_transactions / total_transactions) * 100
refused_transactions = len(df[df['RESP'] != -1])
refusal_rate = (refused_transactions / total_transactions) * 100

# Distribution des Codes de Réponse de Refus
refused_transactions_df = df[(df['RESP'] != -1) & (df['RESP'] != 0)]
response_distribution = refused_transactions_df['RESP'].value_counts()
most_frequent_refusal_code = response_distribution.idxmax()
most_frequent_refusal_count = response_distribution.max()

# Transactions par Type de Terminal
terminal_distribution = df['TERMINAL_TYPE'].value_counts()

# Transactions par Institution Émettrice
total_transactions_per_issuer = df['ISS_INST'].value_counts()
refused_transactions_df = df[(df['RESP'] != -1) & (df['RESP'] != 0)]
refused_transactions_per_issuer = refused_transactions_df['ISS_INST'].value_counts()
refusal_rate_per_issuer = (refused_transactions_per_issuer / total_transactions_per_issuer) * 100
refusal_rate_per_issuer = refusal_rate_per_issuer.sort_values(ascending=False)
refusal_rate_per_issuer.index = refusal_rate_per_issuer.index.astype(int)
refusal_rate_per_issuer = refusal_rate_per_issuer.round(2)

# Stabilité du système
df = df.sort_values(by='DATETIME')
df['TIME_DIFF'] = df['DATETIME'].diff().dt.total_seconds()
problematic_intervals = df[(df['TIME_DIFF'] == 30) | (df['TIME_DIFF'] == 60)]
system_status = "Il y a un problème dans le système." if not problematic_intervals.empty else "Le système est disponible."

# Grouper les données par période de temps (par minute) et calculer le taux de réussite et le taux de refus
time_success_rate = df.groupby('TIME_ONLY').agg(
    total_transactions=('SUCCESS', 'size'),
    successful_transactions=('SUCCESS', 'sum'),
    refused_transactions=('SUCCESS', lambda x: x.size - x.sum())
)
time_success_rate['Success Rate'] = (time_success_rate['successful_transactions'] / time_success_rate['total_transactions']) * 100
time_success_rate['Refusal Rate'] = (time_success_rate['refused_transactions'] / time_success_rate['total_transactions']) * 100
time_success_rate = time_success_rate.reset_index()

# Création de l'application Tkinter
root = tk.Tk()
root.title("Dashboard de Monitoring des Transactions")

# Définir la taille de la fenêtre
root.geometry("820x880")  # Par exemple, largeur 1200 pixels et hauteur 800 pixels

# Création d'un Canvas avec une Scrollbar
main_frame = tk.Frame(root)
main_frame.pack(fill=tk.BOTH, expand=1)

canvas = tk.Canvas(main_frame)
scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=canvas.yview)
scrollable_frame = ttk.Frame(canvas)

scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(
        scrollregion=canvas.bbox("all")
    )
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

# Pack canvas and scrollbar
canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=1)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

# Ajouter la date et l'heure du fichier Excel dans l'en-tête
header_label = ttk.Label(scrollable_frame, text=f"Date et Heure du Fichier: {first_date_time.strftime('%Y-%m-%d %H:%M:%S')}", font=("Arial", 12, "bold"), foreground="#E11306")
header_label.pack(pady=10)

# Ajout des labels avec titres colorés et valeurs séparées
Label(scrollable_frame, text="Total des Transactions:", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{total_transactions}", font=("Arial", 12), foreground="black").pack(anchor="w")

Label(scrollable_frame, text="Transactions Réussies:", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{successful_transactions}", font=("Arial", 12), foreground="black").pack(anchor="w")

Label(scrollable_frame, text="Taux de Réussite:", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{success_rate:.2f}%", font=("Arial", 12), foreground="black").pack(anchor="w")

Label(scrollable_frame, text="Transactions Refusées:", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{refused_transactions}", font=("Arial", 12), foreground="black").pack(anchor="w")

Label(scrollable_frame, text="Taux de Refus:", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{refusal_rate:.2f}%", font=("Arial", 12), foreground="black").pack(anchor="w")

Label(scrollable_frame, text="Code de Réponse de Refus le Plus Fréquent:", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{most_frequent_refusal_code} (Nombre: {most_frequent_refusal_count})", font=("Arial", 12), foreground="black").pack(anchor="w")

Label(scrollable_frame, text="Disponibilité du système durant les 5 dernières minutes :", font=("Arial", 12, "bold"), foreground="#5dade2").pack(anchor="w")
Label(scrollable_frame, text=f"{system_status}", font=("Arial", 12), foreground="black").pack(anchor="w")


# Fonction pour créer un graphique matplotlib et l'ajouter à la frame Tkinter
def plot_donut_chart(df, title):
 # Define the label mapping
    label_mapping = {
        1: 'DAB',
        2: 'TPE',
        8: 'E-Commerce'
    }
    
    # Replace numeric index with labels
    df.index = df.index.map(label_mapping)

    fig, ax = plt.subplots(figsize=(6, 3))
    wedges, texts, autotexts = ax.pie(df.values, labels=df.index, autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
    
    # Draw a white circle at the center to make it a donut chart
    center_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(center_circle)
    
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    ax.set_title(title)
    
    fig_canvas = FigureCanvasTkAgg(fig, master=scrollable_frame)
    fig_canvas.draw()
    fig_canvas.get_tk_widget().pack(pady=10)

# Graphique pour les Transactions par Type de Terminal (Donut Chart)
plot_donut_chart(terminal_distribution, 'Transactions par Type de Terminal')


# Fonction pour créer un graphique matplotlib et l'ajouter à la frame Tkinter
def plot_to_frame(df, column_name, title, ylabel, kind='bar'):
    plt.figure(figsize=(6, 3))
    if kind == 'bar':
        df.plot(kind='bar', legend=False)
    elif kind == 'line':
        df.plot(kind='line', x=column_name, legend=False)
    plt.title(title)
    plt.xlabel('')
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    fig_canvas = FigureCanvasTkAgg(plt.gcf(), master=scrollable_frame)
    fig_canvas.draw()
    fig_canvas.get_tk_widget().pack(pady=10)


def plot_top_5_bar_chart(df, column_name, title, ylabel):
    # Filter to get the top 5 issuers with the highest refusal rate
    top_5_df = df.head(5)
    
    # Plot the bar chart
    plt.figure(figsize=(6, 3))
    top_5_df.plot(kind='bar', legend=False)
    plt.title(title)
    plt.xlabel(column_name)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    fig_canvas = FigureCanvasTkAgg(plt.gcf(), master=scrollable_frame)
    fig_canvas.draw()
    fig_canvas.get_tk_widget().pack(pady=10)

# Ensure that the DataFrame is sorted by refusal rate before plotting
refusal_rate_per_issuer = refusal_rate_per_issuer.sort_values(ascending=False)

# Graphique pour le Taux de Refus par Institution Émettrice (Top 5)
plot_top_5_bar_chart(refusal_rate_per_issuer, 'ISS_INST', 'Taux de Refus par Institution Émettrice (Top 5)', 'Taux de Refus (%)')


# Graphique pour le Taux de Refus par Institution Émettrice
#plot_to_frame(refusal_rate_per_issuer, 'ISS_INST', 'Taux de Refus par Institution Émettrice', 'Taux de Refus (%)')

# Graphique pour la Distribution des Codes de Réponse de Refus
plot_to_frame(response_distribution, 'RESP', 'Distribution des Codes de Réponse de Refus', 'Nombre de Transactions')

# Graphique pour le Taux de Réussite et Taux de Refus par Période de Temps (Line Chart Only)
plt.figure(figsize=(8, 4))

# Plot the Success Rate with a line
plt.plot(time_success_rate['TIME_ONLY'], time_success_rate['Success Rate'], marker='o', color='skyblue', label='Taux de Réussite')

# Plot the Refusal Rate with a line
plt.plot(time_success_rate['TIME_ONLY'], time_success_rate['Refusal Rate'], marker='o', color='salmon', label='Taux de Refus')

plt.title('Taux de Réussite et Taux de Refus par Période de Temps')
plt.xlabel('Temps (HH:MM)')
plt.ylabel('Taux (%)')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()

canvas_chart = FigureCanvasTkAgg(plt.gcf(), master=scrollable_frame)
canvas_chart.draw()
canvas_chart.get_tk_widget().pack(pady=10)

# Define a list of critical response codes
critical_response_codes = [801, 802]  # Add more critical codes if needed

# Function to show the alert after the dashboard is displayed
def show_alert(success_rate,refusal_rate,most_frequent_refusal_code,most_frequent_refusal_count):
    if success_rate < 70:
        messagebox.showwarning("Alerte Critique Taux de Réussite", f"Le taux de réussite est de {success_rate:.2f}%, ce qui est inférieur à 70%!")
    
    # Alert for taux de refus
    if refusal_rate > 30:
        messagebox.showwarning("Alerte Taux de Refus", f"Le taux de refus est de {refusal_rate:.2f}%, ce qui est trop élevé!")
     # Alert for critical response codes
    if most_frequent_refusal_code in critical_response_codes:
        messagebox.showwarning("Alerte Code Critique", 
                               f"Le code de refus le plus fréquent est {most_frequent_refusal_code} avec {most_frequent_refusal_count} occurrences, ce qui est critique!")

def refresh_data():
    # Reconnecter à la base et recharger les données
    # Mettre à jour les KPI et les graphiques
    messagebox.showinfo("Rafraîchissement", "Les données ont été mises à jour.")

quit_button = tk.Button(scrollable_frame, text="Quitter", command=root.quit)
quit_button.pack(pady=10)

refresh_button = tk.Button(scrollable_frame, text="Rafraîchir", command=refresh_data)
refresh_button.pack(pady=10)


# Schedule the alert to display after the UI is rendered (500ms delay)
root.after(500, show_alert(success_rate,refusal_rate,most_frequent_refusal_code,most_frequent_refusal_count))

# Lancement de l'application
root.mainloop()

