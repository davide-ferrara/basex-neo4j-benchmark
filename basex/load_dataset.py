import pandas as pd
from BaseXClient import BaseXClient


# In XML caraterri come >< non sono ammessi e vanno sostituiti con &lt o &gt ad esempio
def escape_xml_chars(text):
    if isinstance(text, str):
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )
    return text


# Funzione per convertire un DataFrame in XML, omettendo i campi non presenti o NaN e aggiungendo l'attributo entity_type
def data_frame_to_xml(df):
    xml = ["<ubo_records>"]
    for _, row in df.iterrows():
        entity_type = row["entity_type"]
        xml.append(
            f'  <ubo_record entity_type="{entity_type}">'
        )  # Aggiunge attributo entity_type
        for field in df.columns:
            if field != "entity_type":  # Non aggiunge il campo 'entity_type' come tag
                value = row[field]
                if pd.notna(value):  # Aggiunge solo i campi non NaN
                    escaped_value = escape_xml_chars(
                        str(value)
                    )  # Escapa i caratteri speciali
                    xml.append(f"    <{field}>{escaped_value}</{field}>")
        xml.append("  </ubo_record>")
    xml.append("</ubo_records>")
    return "\n".join(xml)

# Ritorna i dati XML frazionati partendo dai data frames
def fraction_data_frames(data_frame):
    # 100% dei dati
    data_frame_100 = data_frame
    xml_100 = data_frame_to_xml(data_frame_100)

    # 75% dei dati
    data_frame_75 = data_frame_100.sample(
        frac=0.75, random_state=1
    )  # Campiona il 75% del dataset originale
    xml_75 = data_frame_to_xml(data_frame_75)

    # 50% dei dati
    data_frame_50 = data_frame_75.sample(
        frac=0.6667, random_state=1
    )  # 50% di 75% = 66.67% del totale originale
    xml_50 = data_frame_to_xml(data_frame_50)

    # 25% dei dati
    data_frame_25 = data_frame_50.sample(
        frac=0.5, random_state=1
    )  # 50% di 50% = 50% del totale originale
    xml_25 = data_frame_to_xml(data_frame_25)

    return xml_100, xml_75, xml_50, xml_25


def xml_data_to_basex(db_name, xml_data):
    try:
        print("Trying to connect to BaseX Server...")
        session = BaseXClient.Session("localhost", 1984, "admin", "admin")
        print("Connected to BaseX!")
        try:
            print(f"Creating database: {db_name}")
            session.execute(f"CREATE DB {db_name}")
            session.add(f"{db_name}.xml", xml_data)
            print(f"Data successfully loaded into {db_name} in BaseX.")
        except Exception as e:
            print(f"An error occurred during data insertion: {e}")
        finally:
            session.close()
    except Exception as e:
        print(f"Connection error: {e}")
        exit(-1)


# Nome entità e path CSV 
csv_files = {
    "administrators": "dataset/admins.csv",
    "companies": "dataset/companies.csv",
    "shareholders": "dataset/shareholders.csv",
    "transactions": "dataset/transactions.csv",
    "kyc_aml_checks": "dataset/kyc_aml_checks.csv",
    "ubos": "dataset/ubos.csv",
}

def load_dataset():
    # Legge tutti i file CSV in un unico DataFrame pandas, aggiungendo una colonna 'entity_type' per indicare l'entità di origine
    data_frames = []
    for entity_type, file_path in csv_files.items():
        data_frame = pd.read_csv(file_path, encoding="ISO-8859-1")
        data_frame["entity_type"] = entity_type
        # print(data_frame)
        data_frames.append(data_frame)
    
    # print(data_frame)
    data_frame = pd.concat(data_frames, ignore_index=True)
    
    xml_data_100, xml_data_75, xml_data_50, xml_data_25 = fraction_data_frames(data_frame)
    db_prefix = "UBO"
    db_names = {
        f"{db_prefix}_100": xml_data_100,
        f"{db_prefix}_75": xml_data_75,
        f"{db_prefix}_50": xml_data_50,
        f"{db_prefix}_25": xml_data_25,
    }
    
    for db_name, xml_data in db_names.items():
        print(f"Length of XML data for {db_name}: {len(xml_data)} characters")
        xml_data_to_basex(db_name, xml_data)

# Carico i dati su basex
load_dataset()
