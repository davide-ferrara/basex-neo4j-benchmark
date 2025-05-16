# Importa le librerie necessarie
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import re
import numpy as np

# Funzione per estrarre i valori di intervallo di confidenza dai dati
def extract_confidence_values(confidence_interval_str):
    if pd.isna(confidence_interval_str):
        return np.nan, np.nan
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Definisce i percorsi ai file CSV per i dati di BaseX e Neo4j
basex_csv_paths = [
    "./basex/res/basex_response_times_first_execution.csv",
    "./basex/res/basex_response_times_average_30.csv",
]

neo4j_csv_paths = [
    "./neo4j/res/neo4j_response_times_first_execution.csv",
    "./neo4j/res/neo4j_response_times_average_30.csv",
]

# Carica i dati dai file CSV per BaseX
data_basex_first_execution = pd.read_csv(basex_csv_paths[0], sep=',', dtype={'Confidence Interval (Min, Max)': str})
data_basex_avg_30 = pd.read_csv(basex_csv_paths[1], sep=',', dtype={'Confidence Interval (Min, Max)': str})

# Carica i dati dai file CSV per Neo4j
data_neo4j_first_execution = pd.read_csv(neo4j_csv_paths[0], sep=',', dtype={'Confidence Interval (Min, Max)': str})
data_neo4j_avg_30 = pd.read_csv(neo4j_csv_paths[1], sep=',', dtype={'Confidence Interval (Min, Max)': str})

# Definisce le dimensioni del dataset e le query da analizzare
dataset_sizes = ['100%', '75%', '50%', '25%']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5']

matplotlib.use('TkAgg')

# Definisce i colori per i grafici
color_basex = '#AA0000'
color_neo4j = '#00EE55'
color_dataset_size = '#00CCFF'

# Cicla attraverso ogni query per generare i grafici
for query in queries:
    # Come seguito dalla guida su StackOverflow
    # Filtra i dati per la query corrente
    data_basex_query_first_execution = data_basex_first_execution[data_basex_first_execution['Query'] == query]
    data_basex_query_avg_30 = data_basex_avg_30[data_basex_avg_30['Query'] == query]
    data_neo4j_query_first_execution = data_neo4j_first_execution[data_neo4j_first_execution['Query'] == query]
    data_neo4j_query_avg_30 = data_neo4j_avg_30[data_neo4j_avg_30['Query'] == query]

    # Crea il primo grafico: Tempo di esecuzione per la prima esecuzione
    plt.figure(figsize=(12, 9))
    bar_width = 0.35
    index = np.arange(len(dataset_sizes))

    # Estrai i valori di tempo di esecuzione per Basex e Neo4j
    values_basex_first_execution = [data_basex_query_first_execution[data_basex_query_first_execution['Dataset'] == size]['Milliseconds'].values[0] for size in dataset_sizes]
    values_neo4j_first_execution = [data_neo4j_query_first_execution[data_neo4j_query_first_execution['Dataset'] == size]['Milliseconds'].values[0] for size in dataset_sizes]

    # Crea i barplot per Basex e Neo4j
    plt.bar(index - bar_width / 2, values_basex_first_execution, bar_width, label='BaseX', color=color_basex)
    plt.bar(index + bar_width / 2, values_neo4j_first_execution, bar_width, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dataset Size')
    plt.ylabel('Execution Time (ms)')
    plt.title(f'Histogram - First Execution Time for {query}')
    plt.xticks(index, dataset_sizes)
    plt.legend()
    plt.tight_layout()

    # Imposta la scala logaritmica
    plt.yscale('log')

    # Aggiungi la tabella dei risultati
    table_data = []
    for size, basex_time, neo4j_time in zip(dataset_sizes, values_basex_first_execution, values_neo4j_first_execution):
        table_data.append([size, f"{basex_time:.2f}", f"{neo4j_time:.2f}"])

    # Rendi i nomi delle colonne in grassetto utilizzando il parametro 'fontweight'
    column_labels = ['Dataset Size', 'BaseX', 'Neo4j']
    table = plt.table(cellText=table_data, colLabels=column_labels, cellLoc='center', loc='bottom', bbox=[0.0, -0.4, 1, 0.3], colColours=[color_dataset_size, color_basex, color_neo4j])
    
    # Imposta il font in grassetto per le etichette delle colonne
    for key, cell in table.get_celld().items():
        if key[0] == 0:  # Prima riga, le etichette delle colonne
            cell.set_text_props(fontweight='bold')
    
    plt.subplots_adjust(left=0.2, bottom=0.3)

    # Salva e mostra il grafico
    filename = f'./histograms/Histogram_Time_Before_Execution_{query}.png'
    plt.savefig(filename)
    plt.show()
    plt.close()

    # Crea il secondo grafico: Tempo di esecuzione medio con intervallo di confidenza
    plt.figure(figsize=(12, 9))
    bar_width = 0.35
    index = np.arange(len(dataset_sizes))

    # Estrai i valori di tempo di esecuzione medio per Basex e Neo4j
    values_basex_avg_30 = [data_basex_query_avg_30[data_basex_query_avg_30['Dataset'] == size]['Average'].values[0] for size in dataset_sizes]
    values_neo4j_avg_30 = [data_neo4j_query_avg_30[data_neo4j_query_avg_30['Dataset'] == size]['Average'].values[0] for size in dataset_sizes]

    # Estrai gli intervalli di confidenza per Basex e Neo4j
    conf_intervals_basex = [extract_confidence_values(data_basex_query_avg_30[data_basex_query_avg_30['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) for size in dataset_sizes]
    conf_intervals_neo4j = [extract_confidence_values(data_neo4j_query_avg_30[data_neo4j_query_avg_30['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) for size in dataset_sizes]

    # Estrai i valori minimi e massimi degli intervalli di confidenza
    conf_basex_min = [conf[0] for conf in conf_intervals_basex]
    conf_basex_max = [conf[1] for conf in conf_intervals_basex]
    conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
    conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]

    # Calcola gli errori per le barre di errore
    basex_yerr = [np.array([values_basex_avg_30[i] - conf_basex_min[i], conf_basex_max[i] - values_basex_avg_30[i]]) for i in range(len(dataset_sizes))]
    neo4j_yerr = [np.array([values_neo4j_avg_30[i] - conf_neo4j_min[i], conf_neo4j_max[i] - values_neo4j_avg_30[i]]) for i in range(len(dataset_sizes))]

    # Crea i barplot con barre di errore per Basex e Neo4j
    plt.bar(index - bar_width / 2, values_basex_avg_30, bar_width, yerr=np.array(basex_yerr).T, capsize=5, label='BaseX', color=color_basex)
    plt.bar(index + bar_width / 2, values_neo4j_avg_30, bar_width, yerr=np.array(neo4j_yerr).T, capsize=5, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dataset Size')
    plt.ylabel('Average Execution Time (ms)')
    plt.title(f'Histogram - Average Execution Time for {query}')
    plt.xticks(index, dataset_sizes)
    plt.legend()
    plt.tight_layout()

    plt.yscale('log')

    # Aggiungi la tabella dei risultati
    table_data = []
    for size, basex_avg, neo4j_avg in zip(dataset_sizes, values_basex_avg_30, values_neo4j_avg_30):
        table_data.append([size, f"{basex_avg:.2f}", f"{neo4j_avg:.2f}"])

    column_labels = ['Dataset Size', 'BaseX', 'Neo4j']
    table = plt.table(cellText=table_data, colLabels=column_labels, cellLoc='center', loc='bottom', bbox=[0.0, -0.4, 1, 0.3], colColours=[color_dataset_size, color_basex, color_neo4j])

    # Imposta il font in grassetto per le etichette delle colonne
    for key, cell in table.get_celld().items():
        if key[0] == 0:  # Prima riga, le etichette delle colonne
            cell.set_text_props(fontweight='bold')

    plt.subplots_adjust(left=0.2, bottom=0.3)

    # Salva e mostra il grafico
    filename = f'./histograms/Histogram_Average_Execution_Time_{query}.png'
    plt.savefig(filename)
    plt.show()
    plt.close()
