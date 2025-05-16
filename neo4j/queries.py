from py2neo import Graph, Node
import time
import csv
import scipy.stats as stats
import numpy as np
import json

address = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

company_id = 1914
currency = "EUR"
ubo_percentage = 25
start_date = "2016-07-01"
end_date = "2024-07-01"

# Connessione ai database neo4j
db_name = "database"
graph100 = Graph(address, auth=(username, password), name=f"{db_name}100")
graph75 = Graph(address, auth=(username, password), name=f"{db_name}75")
graph50 = Graph(address, auth=(username, password), name=f"{db_name}50")
graph25 = Graph(address, auth=(username, password), name=f"{db_name}25")

def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)  # Numero di osservazioni
    average_value = np.average(data)  # Valore medio
    stderr = stats.sem(data)  # Errore standard della media
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)  # Margine di errore
    return average_value, margin_of_error

# Funzione per misurare le performance di una query
def run_benchmark(graph, query_func, percentage, iterations, query_num):
    if iterations == 1:
        start_time = time.time()
        query_result = query_func(graph)  # Esecuzione della query
        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)  # Tempo di esecuzione della prima query

        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)  # Conversione in JSON
    
            print(f"Response time (first execution - Query {query_num}): {time_first_execution} ms")
            print(f"Query {query_num} Result: \n{json_result}\n")
        else:
            print(f"No results founds!\n")

        return time_first_execution
    
    subsequent_times = []
    for _ in range(iterations):
        start_time = time.time()  # Tempo di inizio
        query_func(graph)  # Esecuzione della query
        end_time = time.time()  # Tempo di fine
        execution_time = (end_time - start_time) * 1000  # Tempo di esecuzione in millisecondi
        subsequent_times.append(execution_time)
    
    # Calcolo dell'intervallo di confidenza per i tempi di esecuzione
    average, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_subsequent_time = round(sum(subsequent_times) / len(subsequent_times), 2)
    
    return average_subsequent_time, average, margin_of_error

def query1(graph):
    query = f"""
    MATCH (c:Companies {{id: '{company_id}'}})
    RETURN c as company
    """
    result = graph.run(query).data()
    return result

def query2(graph):
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    RETURN c as company, collect(a) as administrators
    """
    result = graph.run(query).data()
    return result

def query3(graph):
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > {ubo_percentage}
    RETURN c as company, collect(DISTINCT a) as administrators, collect(DISTINCT u) as ubos
    """

    result = graph.run(query).data()
    return result

def query4(graph):
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})

    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    WITH c, collect(DISTINCT a) as administrators

    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WITH c, administrators, collect(DISTINCT u) as ubos

    OPTIONAL MATCH (c)-[:COMPANY_HAS_TRANSACTION]->(t:Transactions)
    WHERE t.date >= '{start_date}' AND t.date <= '{end_date}'

    RETURN c as company, 
           [ubo IN ubos WHERE ubo.ownership_percentage > {ubo_percentage}] as ubos,
           administrators, 
           sum(t.amount) as total_amount

    """
    result = graph.run(query).data()
    return result

def query5(graph):
    # Query per recuperare l'azienda e i dettagli associati
    query = f"""
        MATCH (c:Companies {{id: {company_id}}})

         OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
         WITH c, collect(DISTINCT a) as administrators

         OPTIONAL MATCH (c)-[:COMPANY_HAS_SHAREHOLDER]->(s:Shareholders)
         WITH c, administrators, collect(DISTINCT s) as shareholders

         OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
         WITH c, administrators, shareholders, collect(DISTINCT u) as ubos

         OPTIONAL MATCH (c)-[:COMPANY_HAS_TRANSACTION]->(t:Transactions)
         WHERE t.date >= '{start_date}' AND t.date <= '{end_date}' AND t.currency = "{currency}"
         WITH c, administrators, shareholders, ubos, collect(DISTINCT t) as transactions, sum(t.amount) as total_amount

         RETURN c as company, 
                administrators, 
                [ubo IN ubos WHERE ubo.ownership_percentage > {ubo_percentage}] as ubos,
                shareholders,
                transactions,
                total_amount
    """

    # Esecuzione della query
    result = graph.run(query).data()

    return result


def main():
    # Definizione dei grafi da analizzare
    graphs = {
        '100%': graph100,
        '75%': graph75,
        '50%': graph50,
        '25%': graph25
    }
    
    response_times_first_execution = {}  # Dizionario per i tempi di risposta della prima esecuzione
    average_response_times = {}  # Dizionario per i tempi di risposta medi

    for percentage, graph in graphs.items():
        print(f"\nRunning queries for percentage: {percentage}\n")

        # Query 1 first time execution
        time_first_execution = run_benchmark(graph, query1, percentage, 1, "1")
        response_times_first_execution[f"{percentage} - Query 1"] = time_first_execution
        # Query 1 - 30 iterations
        average_subsequent_time, average, margin_of_error = run_benchmark(graph, query1, percentage, 30, "1")

        print(f"Average time of 30 subsequent executions (Query 1): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 1): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 1"] = (average_subsequent_time, average, margin_of_error)

        # Query 2 first time execution
        time_first_execution = run_benchmark(graph, query2, percentage, 1, "2")
        response_times_first_execution[f"{percentage} - Query 2"] = time_first_execution
        # Query 2 - 30 iterations
        average_subsequent_time, average, margin_of_error = run_benchmark(graph, query2, percentage, 30, "2")

        print(f"Average time of 30 subsequent executions (Query 2): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 2): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 2"] = (average_subsequent_time, average, margin_of_error)

        # Query 3 first time execution
        time_first_execution = run_benchmark(graph, query3, percentage, 1, "3")
        response_times_first_execution[f"{percentage} - Query 3"] = time_first_execution
        # Query 3 - 30 iterations
        average_subsequent_time, average, margin_of_error = run_benchmark(graph, query3, percentage, 30, "3")

        print(f"Average time of 30 subsequent executions (Query 3): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 3): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 3"] = (average_subsequent_time, average, margin_of_error)

        # Query 4 first time execution
        time_first_execution = run_benchmark(graph, query4, percentage, 1, "4")
        response_times_first_execution[f"{percentage} - Query 4"] = time_first_execution
        # Query 4 - 30 iterations
        average_subsequent_time, average, margin_of_error = run_benchmark(graph, query4, percentage, 30, "4")

        print(f"Average time of 30 subsequent executions (Query 4): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 4): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 4"] = (average_subsequent_time, average, margin_of_error)

        # Query 5 first time execution
        time_first_execution = run_benchmark(graph, query5, percentage, 1, "5")
        response_times_first_execution[f"{percentage} - Query 5"] = time_first_execution
        # Query 5 - 30 iterations
        average_subsequent_time, average, margin_of_error = run_benchmark(graph, query5, percentage, 30, "5")

        print(f"Average time of 30 subsequent executions (Query 5): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 5): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 5"] = (average_subsequent_time, average, margin_of_error)

    # Salvataggio dati nei CSV
    with open('neo4j/res/neo4j_response_times_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds"])
        for key, value in response_times_first_execution.items():
            percentage, query = key.split(' - ')
            writer.writerow([percentage, query, value])

    with open('neo4j/res/neo4j_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds", "Average", "Confidence Interval (Min, Max)"])
        for key, (average_time, average, margin_of_error) in average_response_times.items():
            percentage, query = key.split(' - ')
            writer.writerow([percentage, query, average_time, round(average, 2), f"[{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}]"])

if __name__ == "__main__":
    main()
