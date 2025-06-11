# BaseX and Neo4j Benchmark

## Obiettivo del Progetto
Questo progetto si propone di analizzare e confrontare le funzionalità di due sistemi di gestione dei
database, Neo4j e BaseX, focalizzandosi sulla loro efficacia nell’elaborazione di informazioni
relative al Beneficiario Effettivo Ultimo (UBO). Pur non essendo finalizzato alla reale
identificazione degli UBO secondo quanto previsto dai criteri del progetto, lo studio si basa sulla
creazione di un dataset che rappresenta scenari realistici di strutture societarie complesse. A partire
da questo dataset simulato, il progetto intende eseguire cinque interrogazioni con livelli crescenti di
complessità su entrambi i database, al fine di confrontarne le prestazioni in termini di velocità di
esecuzione e capacità di gestione di dati articolati.
Tecnologie Utilizzate

Per affrontare la sfida della gestione e interrogazione dei dati legati ai Beneficiari Effettivi Ultimi
(UBO), sono stati selezionati due database che incarnano approcci differenti nella modellazione e
nel trattamento delle informazioni:
Neo4j: Si tratta di un database a grafo che organizza i dati sotto forma di nodi, relazioni e attributi.
Questo modello si presta particolarmente bene alla rappresentazione di reti complesse e intrecciate,
come quelle riscontrabili nelle strutture di proprietà aziendale multilivello tipiche dell'analisi UBO.
Grazie alla sua architettura orientata ai grafi, Neo4j consente una navigazione efficiente tra entità
interconnesse.

BaseX: È un database nativo per XML e utilizza XQuery come linguaggio di interrogazione. È
progettato per gestire dati altamente strutturati e gerarchici, qualità che lo rendono adatto alla
modellazione delle catene di proprietà in formato XML. In questo progetto, BaseX offre la
possibilità di rappresentare strutture societarie complesse e di esplorarle attraverso interrogazioni
avanzate basate su XQuery.

Per automatizzare le operazioni di caricamento, interrogazione e analisi dei dati su entrambe le
piattaforme, è stato impiegato il linguaggio Python. Grazie a librerie dedicate come py2neo per
Neo4j e BaseXClient.py per BaseX, è stato possibile gestire in modo agevole la connessione ai
database, l’esecuzione di query e la raccolta dei risultati per confrontarne l’efficacia.
