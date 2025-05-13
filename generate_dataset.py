import random
import csv
import json
from faker import Faker


# Scrive i dati generati in un file CSV
def write_csv(dataset_name: str, fieldnames: list, buffer: list):
    try:
        with open(f"dataset/{dataset_name}.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(buffer)
        print(f"CSV file '{dataset_name}.csv' successfully created.")
    except Exception as e:
        print(f"Error occured while writing CSV file: {e}")


fake = Faker()

legal_forms = [
    "S.r.l.",  # Società a responsabilità limitata
    "S.p.A.",  # Società per azioni
    "S.a.S.",  # Società in accomandita semplice
    "S.n.C.",  # Società in nome collettivo
    "S.r.l. a socio unico",  # Società a responsabilità limitata a socio unico
    "Coop",  # Cooperativa
    "Onlus",  # Organizzazione non lucrativa di utilità sociale
    "Fond.",  # Fondazione
    "Trust",  # Trust
    "Fil.",  # Filiale
    "S.a.p.A.",  # Società in accomandita per azioni
]


currencies = [
    "EUR",  # Euro
    "USD",  # Dollaro statunitense
    "GBP",  # Sterlina britannica
    "JPY",  # Yen giapponese
    "AUD",  # Dollaro australiano
    "CAD",  # Dollaro canadese
    "CHF",  # Franco svizzero
    "CNY",  # Yuan cinese
    "NZD",  # Dollaro neozelandese
    "SEK",  # Corona svedese
    "NOK",  # Corona norvegese
    "MXN",  # Peso messicano
    "SGD",  # Dollaro di Singapore
    "HKD",  # Dollaro di Hong Kong
    "RUB",  # Rublo russo
]

# Numero record per ogni entità
ADMIN_NUM = 5000
COMPANIES_NUM = 50000
UBO_NUM = 10000
TRANSACTIONS_NUM = 400000
KYC_AML_CHECKS_NUM = 30000
SHAREHOLDERS_NUM = 5000

# Generazione dati per gli amministratori
admins = []
fieldnames = ["id", "name", "address", "birthdate", "nationality"]
for id in range(1, ADMIN_NUM + 1):
    name = fake.name()
    address = fake.address()
    birthdate = fake.date_of_birth(minimum_age=22, maximum_age=74).strftime("%Y-%m-%d")
    nationality = fake.country()
    admins.append(
        {
            "id": id,
            "name": name,
            "address": address,
            "birthdate": birthdate,
            "nationality": nationality,
        }
    )
write_csv("admins", fieldnames, admins)

# Generazione dati per gli azionisti
shareholders = []
fieldnames = [
    "id",
    "name",
    "type",
    "ownership_percentage",
    "address",
    "birthdate",
    "nationality",
]
for shareholder_id in range(1, SHAREHOLDERS_NUM + 1):
    name = fake.name()
    shareholder_type = random.choice(["Person", "Company"])
    ownership_percentage = round(random.uniform(0.1, 100), 2)
    address = fake.address()
    birthdate = (
        fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d")
        # if shareholder_type == "Person"
        # else ""
    )
    #    nationality = fake.country() if shareholder_type == "Person" else ""

    shareholders.append(
        {
            "id": shareholder_id,
            "name": name,
            "type": shareholder_type,
            "ownership_percentage": ownership_percentage,
            "address": address,
            "birthdate": birthdate,
            "nationality": nationality,
        }
    )
write_csv("shareholders", fieldnames, shareholders)


# Generazione dati per i beneficiari effettivi (UBO)
ubos = []
fieldnames = [
    "id",
    "name",
    "address",
    "birthdate",
    "nationality",
    "ownership_percentage",
    "type",
]
for ubo_id in range(1, UBO_NUM + 1):
    name = fake.name()
    address = fake.address()
    birthdate = fake.date_of_birth().strftime("%Y-%m-%d")
    nationality = fake.country()
    ownership_percentage = round(random.uniform(0.1, 100.0), 2)
    ubo_type = random.choice(["Person", "Company"])

    ubos.append(
        {
            "id": ubo_id,
            "name": name,
            "address": address,
            "birthdate": birthdate,
            "nationality": nationality,
            "ownership_percentage": ownership_percentage,
            "type": ubo_type,
        }
    )
write_csv("ubos", fieldnames, ubos)


# Generazione dati per le transazioni
transactions = []
fieldnames = ["id", "type", "amount", "date", "currency"]
for transaction_id in range(1, TRANSACTIONS_NUM + 1):
    transaction_type = random.choice(["Purchase", "Sale", "Payment", "Refund"])
    amount = round(random.uniform(10.0, 10000.0), 2)
    date = fake.date_between(start_date="-8y", end_date="today").strftime("%Y-%m-%d")
    currency = random.choice(currencies)

    transactions.append(
        {
            "id": transaction_id,
            "type": transaction_type,
            "amount": amount,
            "date": date,
            "currency": currency,
        }
    )
write_csv("transactions", fieldnames, transactions)

# Generazione dati per i controlli KYC/AML
kyc_aml_checks = []
fieldnames = ["id", "ubo_id", "type", "result", "date", "notes"]
for kyc_aml_id in range(1, KYC_AML_CHECKS_NUM + 1):
    ubo_id = random.randint(1, UBO_NUM)
    check_type = random.choice(
        [
            "Identity Verification",  # Verifica dell'identità
            "Sanctions Check",  # Controllo delle sanzioni
            "Transaction Monitoring",  # Monitoraggio delle transazioni
            "Source of Funds Verification",  # Verifica della fonte dei fondi
            "Enhanced Due Diligence",  # Due diligence rafforzata
            "Politically Exposed Persons Check",  # Controllo delle persone politicamente esposte
            "Adverse Media Check",  # Controllo dei media avversi
            "Risk Assessment",  # Valutazione del rischio
            "Customer Due Diligence",  # Due diligence del cliente
            "Ongoing Monitoring",  # Monitoraggio continuo
        ]
    )
    result = random.choice(["Passed", "Failed"])
    date = fake.date_between(start_date="-8y", end_date="today").strftime("%Y-%m-%d")
    notes = fake.text(max_nb_chars=200)

    kyc_aml_checks.append(
        {
            "id": kyc_aml_id,
            "ubo_id": ubo_id,
            "type": check_type,
            "result": result,
            "date": date,
            "notes": notes,
        }
    )
write_csv("kyc_aml_checks", fieldnames, kyc_aml_checks)

# Generazione dati per le aziende
companies = []
fieldnames = [
    "id",
    "name",
    "address",
    "legal_form",
    "registration_details",
    "financial_data",
    "administrators",
    "shareholders",
    "ubo",
    "transactions",
]
for company_id in range(1, COMPANIES_NUM + 1):
    name = fake.company()
    address = fake.address()
    legal_form = random.choice(legal_forms)
    registration_details = fake.ssn()
    financial_data = [
        {
            "year": random.randint(2015, 2023),
            "revenue": round(random.uniform(10000, 1000000), 2),
            "profit": round(random.uniform(1000, 500000), 2),
        }
        for _ in range(random.randint(1, 5))
    ]

    num_administrators = random.randint(1, 3)
    administrators_ids = random.sample(range(1, ADMIN_NUM + 1), num_administrators)

    num_shareholders = random.randint(1, 10)
    shareholders_ids = random.sample(range(1, SHAREHOLDERS_NUM + 1), num_shareholders)

    num_ubos = random.randint(1, 3)
    ubos_ids = random.sample(range(1, UBO_NUM + 1), num_ubos)

    num_transactions = random.randint(1, 5)
    transactions_ids = random.sample(range(1, TRANSACTIONS_NUM + 1), num_transactions)

    companies.append(
        {
            "id": company_id,
            "name": name,
            "address": address,
            "legal_form": legal_form,
            "registration_details": registration_details,
            "financial_data": json.dumps(financial_data),
            "administrators": administrators_ids,
            "shareholders": shareholders_ids,
            "ubo": ubos_ids,
            "transactions": transactions_ids,
        }
    )
write_csv("companies", fieldnames, companies)
