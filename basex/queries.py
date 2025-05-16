from BaseXClient import BaseXClient
import time
import csv
import scipy.stats as stats
import numpy as np

company_id = 1914
ubo_percentage = 25
currency = "SEK"
start_date = "2016-07-01"
end_date = "2024-07-01"

# Funzione per calcolare l'intervallo di confidenza per una media
def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)  # Numero di osservazioni
    mean_value = np.mean(data)  # Media dei dati
    stderr = stats.sem(data)  # Errore standard della media
    margin_of_error = stderr * stats.t.ppf(
        (1 + confidence) / 2, n - 1
    )  # Margine d'errore
    return mean_value, margin_of_error


# Funzione per misurare la performance delle query
def measure_query_performance(session, query_function, percentage, iterations=30):
    subsequent_times = []  # Lista per memorizzare i tempi di esecuzione

    for _ in range(iterations):
        start_time = time.time()  # Inizio misurazione tempo
        query_function(session, percentage)
        end_time = time.time()  # Fine misurazione tempo
        execution_time = (
            end_time - start_time
        ) * 1000  # Tempo di esecuzione in millisecondi
        subsequent_times.append(execution_time)

    # Calcola il tempo medio e l'intervallo di confidenza
    mean, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_time_subsequent = round(sum(subsequent_times) / len(subsequent_times), 2)

    return average_time_subsequent, mean, margin_of_error


# Query testate anche su BaseXGUI
# Query 1: Recupera un'azienda per id
def query1(session, percentage, entity_type="companies"):
    query = f"""
        declare option output:indent "yes";
        let $company := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='companies' and id={company_id}]

        return 
        <result>
        {{ 
          $company
        }}
        </result>

        """

    query_obj = session.query(query)
    result = query_obj.execute()

    return company_id, result


# Query 2: Recupera i dettagli di un'azienda e dei suoi amministratori
def query2(session, percentage):
    query = f"""
        declare option output:indent "yes";
        let $company := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='companies' and id={company_id}]
        let $admin_ids_str := $company//administrators/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $admin_ids_clean := replace($admin_ids_str, "[\[\]]", "")
        let $admin_ids := 
        for $id in tokenize($admin_ids_clean, ",\s*")
        return number($id)  (: Converte le stringhe in numeri :)

        let $admins := 
        for $id in $admin_ids
        return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=$id]

        return 
            <result>
                {{ 
                    $company,
                    $admins
                }}
             </result>

        """

    result = session.query(query).execute()
    return company_id, result


# Query 3: Recupera i dettagli dell'azienda, dei suoi amministratori e degli UBO con più del 25%
def query3(session, percentage):
    query = f"""
        declare option output:indent "yes";
        let $company := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='companies' and id={company_id}]
        let $admin_ids_str := $company//administrators/data()
        
        (: Rimuove le parentesi quadre e divide per virgola :)
        let $admin_ids_clean := replace($admin_ids_str, "[\[\]]", "")
        let $admin_ids := 
          for $id in tokenize($admin_ids_clean, ",\s*")
          return number($id)  (: Converte le stringhe in numeri :)
        
        let $admins := 
          for $id in $admin_ids
          return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=$id]
         
        let $ubo_ids_str := $company//ubo/data()
        
        (: Rimuove le parentesi quadre e divide per virgola :)
        let $ubo_ids_clean := replace($ubo_ids_str, "[\[\]]", "")
        let $ubo_ids := 
          for $id in tokenize($ubo_ids_clean, ",\s*")
          return number($id)  (: Converte le stringhe in numeri :)
        
        let $ubos := 
          for $id in $ubo_ids
          let $ubo_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='ubos' 
            and id=$id] where xs:decimal($ubo_record/ownership_percentage) > {ubo_percentage} return $ubo_record
          
        return 
        <result>
          {{
            <company>
                {{ $company }}
            </company>
          }}
          {{
            <administrators>
              {{ $admins }}
            </administrators>
          }}
          {{
            <ubos>
            {{ $ubos }}
            </ubos>
          }}
        </result>

    """

    result = session.query(query).execute()
    return company_id, result


# Query 4: Recupera i dettagli dell'azienda, dei suoi amministratori, UBO e la somma delle transazioni in un periodo
def query4(session, percentage):
    query = f"""
        declare option output:method "xml";
        declare option output:indent "yes";

        let $collection := concat("UBO_", '{percentage}')
        let $company := collection($collection)//ubo_record[@entity_type='companies' and id={company_id}]

        let $admin_ids_str := $company//administrators/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $admin_ids_clean := replace($admin_ids_str, "[\[\]]", "")
        let $admin_ids := 
          for $id in tokenize($admin_ids_clean, ",\s*")
          return number($id)  (: Converte le stringhe in numeri :)

        let $admins := 
          for $id in $admin_ids
          return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=$id]

        let $ubo_ids_str := $company//ubo/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $ubo_ids_clean := replace($ubo_ids_str, "[\[\]]", "")
        let $ubo_ids := 
          for $id in tokenize($ubo_ids_clean, ",\s*")
          return number($id)  (: Converte le stringhe in numeri :)

        let $ubos := 
          for $id in $ubo_ids
          let $ubo_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='ubos'
            and id=$id] where xs:decimal($ubo_record/ownership_percentage) > {ubo_percentage} return $ubo_record

        (: Transazioni nel range temporale :)
        let $start_date := xs:date("{start_date}")
        let $end_date := xs:date("{end_date}")

        let $tx_ids_str := $company//transactions/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $tx_ids_clean := replace($tx_ids_str, "[\[\]]", "")
        let $tx_ids := 
          for $id in tokenize($tx_ids_clean, ",\s*")
          return number($id)  (: Converte le stringhe in numeri :)

        let $tx := 
          for $id in $tx_ids
          let $tx_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='transactions' 
            and id=$id] where xs:date($tx_record/date) >= $start_date and xs:date($tx_record/date) <= $end_date

          return $tx_record

        let $tx_amount := sum($tx/amount)


        return 
        <result>
            {{
                $company,

                <administrators>
                {{
                    $admins
                }}
                </administrators>,

                <ubos>
                {{
                    $ubos
                }}
                </ubos>,

                <total_transaction_amount>
                {{ 
                    $tx_amount
                }}
                </total_transaction_amount>
            }}
        </result>
    """

    result = session.query(query).execute()
    return company_id, result


# Query 5: Recupera i dettagli dell'azienda, dei suoi amministratori, UBO e la somma delle transazioni in una valuta specifica e risultati KYC/AML recenti
def query5(session, percentage):
    query = f"""
        declare option output:method "xml";
        declare option output:indent "yes";

        let $collection := concat("UBO_", '{percentage}')
        let $company := collection($collection)//ubo_record[@entity_type='companies' and id={company_id}]

        let $admin_ids_str := $company//administrators/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $admin_ids_clean := replace($admin_ids_str, "[\[\]]", "")
        let $admin_ids := 
        for $id in tokenize($admin_ids_clean, ",\s*")
        return number($id)  (: Converte le stringhe in numeri :)

        let $admins := 
        for $id in $admin_ids
        return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='administrators' and id=$id]

        let $ubo_ids_str := $company//ubo/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $ubo_ids_clean := replace($ubo_ids_str, "[\[\]]", "")
        let $ubo_ids := 
        for $id in tokenize($ubo_ids_clean, ",\s*")
        return number($id)  (: Converte le stringhe in numeri :)

        let $ubos := 
        for $id in $ubo_ids
        let $ubo_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='ubos' 
            and id=$id] where xs:decimal($ubo_record/ownership_percentage) > {ubo_percentage} return $ubo_record

        (: Shareholders :)
        let $sh_ids_str := $company//shareholders/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $sh_ids_clean := replace($sh_ids_str, "[\[\]]", "")
        let $sh_ids := 
        for $id in tokenize($sh_ids_clean, ",\s*")
        return number($id)  (: Converte le stringhe in numeri :)

        let $sh := 
        for $id in $sh_ids
        return collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='shareholders' and id=$id]


        (: Transazioni nel range temporale :)
        let $start_date := xs:date("{start_date}")
        let $end_date := xs:date("{end_date}")

        let $tx_ids_str := $company//transactions/data()

        (: Rimuove le parentesi quadre e divide per virgola :)
        let $tx_ids_clean := replace($tx_ids_str, "[\[\]]", "")
        let $tx_ids := 
        for $id in tokenize($tx_ids_clean, ",\s*")
        return number($id)  (: Converte le stringhe in numeri :)

        let $tx := 
        for $id in $tx_ids
        let $tx_record := collection(concat("UBO_", '{percentage}'))//ubo_record[@entity_type='transactions' and id=$id]
        where xs:date($tx_record/date) >= $start_date and xs:date($tx_record/date) <= $end_date 
        and xs:string($tx_record/currency) = "{currency}"

        return $tx_record

        let $tx_amount := sum($tx/amount)

        return 
            <result>
            {{
            $company,
    
            <administrators>
            {{
                $admins
            }}
            </administrators>,
    
            <ubos>
            {{
                $ubos
            }}
            </ubos>,
    
            <shareholders>
            {{
                $sh
            }}
            </shareholders>,
    
            <transactions>
            {{
                $tx
            }}
            </transactions>,
    
            <total_transaction_amount>
            {{ 
                $tx_amount
            }}
            </total_transaction_amount>
            }}
            </result>
        """

    result = session.query(query).execute()
    return company_id, result

# Funzione principale per eseguire le query e misurare le performance
def main():
    session = BaseXClient.Session("localhost", 1984, "admin", "admin")

    percentages = ["100", "75", "50", "25"]  # Percentuali del dataset
    # Dizionario per la mappatura diretta dei dataset ai valori con il simbolo della percentuale nei file di ResponseTime
    dataset_mapping = {"100": "100%", "75": "75%", "50": "50%", "25": "25%"}
    first_execution_response_times = {}
    average_response_times = {}

    for percentage in percentages:
        print(f"\nAnalysis by percentage: {percentage}%\n")

        # Query 1: Recupera il nome dell'azienda con il nome specificato
        start_time = time.time()
        company_name, company = query1(session, percentage)
        end_time = time.time()
        if company:
            print(f"Company name with the specified name: \n{company}\n")
        else:
            print(f"No companies found with the specified name: {company_name}\n")

        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first run - Query 1): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 1"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(
            session, query1, percentage
        )
        print(
            f"Average time of 30 successive executions (Query 1): {average_time_subsequent} ms"
        )
        print(
            f"Confidence interval (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n"
        )
        average_response_times[f"{percentage} - Query 1"] = (
            average_time_subsequent,
            mean,
            margin_of_error,
        )

        # Query 2: Recupera i dettagli dell'azienda e dei suoi amministratori
        start_time = time.time()
        company_id, company = query2(session, percentage)
        end_time = time.time()
        if company:
            print(
                f"Company details with ID {company_id} and administrators: \n{company}\n"
            )
            # print(f"Dettagli degli amministratori dell'azienda con ID {company_id}: \n{administrators}\n")
        else:
            print(f"No companies found with ID {company_id}\n")

        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response Time (First Run - Query 2): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 2"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(
            session, query2, percentage
        )
        print(
            f"Average time of 30 successive executions (Query 2): {average_time_subsequent} ms"
        )
        print(
            f"Confidence Interval (Query 2): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n"
        )
        average_response_times[f"{percentage} - Query 2"] = (
            average_time_subsequent,
            mean,
            margin_of_error,
        )

        # Query 3: Recupera i dettagli dell'azienda, dei suoi amministratori e degli UBO con più del 25%
        start_time = time.time()
        company_id, company = query3(session, percentage)
        end_time = time.time()
        if company:
            print(
                f"Company details with ID {company_id}, administrators and UBO: \n{company}\n"
            )
        else:
            print(f"No companies found with ID {company_id}\n")

        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response Time (First Run - Query 3): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 3"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(
            session, query3, percentage
        )
        print(
            f"Average time of 30 successive executions (Query 3): {average_time_subsequent} ms"
        )
        print(
            f"Confidence Interval (Query 3): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n"
        )
        average_response_times[f"{percentage} - Query 3"] = (
            average_time_subsequent,
            mean,
            margin_of_error,
        )

        # Query 4: Recupera i dettagli dell'azienda, dei suoi amministratori, UBO e la somma delle transazioni in un periodo
        start_time = time.time()
        company_id, company = query4(session, percentage)
        end_time = time.time()
        if company:
            print(
                f"Company details with ID {company_id}, administrators, UBO and transactions: \n{company}\n"
            )
        else:
            print(f"No companies found with ID {company_id}\n")

        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response Time (First Run - Query 4): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 4"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(
            session, query4, percentage
        )
        print(
            f"Average time of 30 successive executions (Query 4): {average_time_subsequent} ms"
        )
        print(
            f"Confidence Interval (Query 4): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n"
        )
        average_response_times[f"{percentage} - Query 4"] = (
            average_time_subsequent,
            mean,
            margin_of_error,
        )

        # Query 5: Recupera i dettagli dell'azienda, dei suoi amministratori, UBO e la somma delle transazioni in una valuta specifica e risultati KYC/AML recenti
        start_time = time.time()
        company_id, company = query5(session, percentage)
        end_time = time.time()
        if company:
            print(f"Company details with ID {company_id} Query 5: \n{company}\n")
        else:
            print(f"No companies found with ID {company_id}\n")

        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response Time (First Run - Query 5): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 5"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(
            session, query5, percentage
        )
        print(
            f"Average time of 30 successive executions (Query 5): {average_time_subsequent} ms"
        )
        print(
            f"Confidence Interval (Query 5): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n"
        )
        average_response_times[f"{percentage} - Query 5"] = (
            average_time_subsequent,
            mean,
            margin_of_error,
        )

        print("-" * 100)  # Separatore tra le diverse percentuali

    # Salva i risultati
    with open(
        "./res/basex_response_times_first_execution.csv",
        mode="w",
        newline="",
    ) as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds"])
        for query, first_execution_time in first_execution_response_times.items():
            dataset, query = query.split(" - ")
            dataset = dataset_mapping.get(dataset, dataset)
            writer.writerow([dataset, query, first_execution_time])

    with open(
        "./res/basex_response_times_average_30.csv", mode="w", newline=""
    ) as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "Dataset",
                "Query",
                "Milliseconds",
                "Average",
                "Confidence Interval (Min, Max)",
            ]
        )
        for query, (
            average_time_subsequent,
            mean_value,
            margin_of_error,
        ) in average_response_times.items():
            dataset, query_name = query.split(" - ")
            dataset = dataset_mapping.get(dataset, dataset)
            min_interval = round(mean_value - margin_of_error, 2)
            max_interval = round(mean_value + margin_of_error, 2)
            writer.writerow(
                [
                    dataset,
                    query_name,
                    average_time_subsequent,
                    mean_value,
                    f"[{min_interval}, {max_interval}]",
                ]
            )
        print(
            "Average response times were written in 'basex_response_times_first_execution.csv' and 'basex_response_times_average_30.csv'."
        )

    session.close()


if __name__ == "__main__":
    main()
