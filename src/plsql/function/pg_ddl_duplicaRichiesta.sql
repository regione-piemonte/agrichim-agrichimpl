CREATE OR REPLACE FUNCTION DUPLICA_ETICHETTA_CAMPIONE(idRichiesta numeric,
                                             anagraficaUtente numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaEtichettaCampione$
BEGIN

  INSERT INTO ETICHETTA_CAMPIONE ( ID_RICHIESTA,
    DATA_INSERIMENTO_RICHIESTA,
    CODICE_MATERIALE, DESCRIZIONE_ETICHETTA,
    ANAGRAFICA_UTENTE,
    ANAGRAFICA_TECNICO, ANAGRAFICA_PROPRIETARIO,
    LABORATORIO_CONSEGNA, CODICE_MODALITA,
    STATO_ATTUALE, PAGAMENTO, NOTE_CLIENTE,CODICE_MISURA_PSR,NOTE_MISURA_PSR )
  SELECT idRichiestaNuovo,
    current_date,
    CODICE_MATERIALE, DESCRIZIONE_ETICHETTA,
    anagraficaUtente,
    ANAGRAFICA_TECNICO, ANAGRAFICA_PROPRIETARIO,
    LABORATORIO_CONSEGNA, CODICE_MODALITA,
    '00', 'N', NOTE_CLIENTE,CODICE_MISURA_PSR,NOTE_MISURA_PSR
  FROM ETICHETTA_CAMPIONE
  WHERE ID_RICHIESTA = idRichiesta;

END;
$duplicaEtichettaCampione$  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION DUPLICA_FASI_RICHIESTA(idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaFasiRichiesta$
BEGIN

  INSERT INTO FASI_RICHIESTA ( ID_RICHIESTA, NUMERO_FASE )
  VALUES ( idRichiestaNuovo, 6 );

END;
$duplicaFasiRichiesta$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DUPLICA_DATI_APPEZZAMENTO(idRichiesta numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaDatiAppezzamento$
BEGIN
  INSERT INTO DATI_APPEZZAMENTO ( ID_RICHIESTA,
    LOCALITA_APPEZZAMENTO,
    COMUNE_APPEZZAMENTO)
  SELECT idRichiestaNuovo,
    LOCALITA_APPEZZAMENTO,
    COMUNE_APPEZZAMENTO
  FROM DATI_APPEZZAMENTO
  WHERE ID_RICHIESTA = idRichiesta;
END;
$duplicaDatiAppezzamento$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DUPLICA_ANALISI_RICHIESTE(idRichiesta numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaAnalisiRichieste$
DECLARE
  vParMTNM  INTEGER;
  vParMTPR	INTEGER;
  nRec      INTEGER;
  rid       record;
  nUpdate   INTEGER;
BEGIN

  INSERT INTO ANALISI_RICHIESTE ( ID_RICHIESTA,
    CODICE_ANALISI, COSTO_ANALISI )
  SELECT idRichiestaNuovo,
    ar.CODICE_ANALISI, ar.COSTO_ANALISI
  FROM ANALISI_RICHIESTE ar,tipi_di_analisi tda
  WHERE ar.ID_RICHIESTA = idRichiesta
  and tda.CODICE_ANALISI = ar.CODICE_ANALISI
  and tda.data_cessazione is null;
  
  nUpdate := 0;
  
  for rid in (select CASE WHEN ec.TARIFFA_APPLICATA = 1 THEN T.FASCIA_RIDUZIONE_1
						  WHEN ec.TARIFFA_APPLICATA = 2 THEN T.FASCIA_RIDUZIONE_2
						  WHEN ec.TARIFFA_APPLICATA = 3 THEN T.FASCIA_RIDUZIONE_3 END tariffa,t.codice_analisi
			  from   ANALISI_RICHIESTE ar,TARIFFE t,ETICHETTA_CAMPIONE ec
			  where  ar.id_richiesta   = idRichiesta
			  and    ar.codice_analisi = t.codice_analisi
			  and    ar.id_richiesta   = ec.id_richiesta
			  and    ec.TARIFFA_APPLICATA is not null) loop
			  
    UPDATE ANALISI_RICHIESTE
    SET    COSTO_ANALISI  = rid.tariffa
	where  id_richiesta   = idRichiestaNuovo
	and    codice_analisi = rid.codice_analisi;
	
	nUpdate := 1;
  END LOOP;
  
  IF nUpdate = 1 THEN
    SELECT CAST(VALORE AS integer)
    INTO   vParMTNM
	FROM   PARAMETRO
    WHERE  ID_PARAMETRO = 'MTNM';
  
    SELECT COUNT(*)
    INTO   nRec
    FROM   ANALISI_RICHIESTE ar,TIPI_DI_ANALISI tda
    WHERE  ar.ID_RICHIESTA    = idRichiestaNuovo
    and    tda.CODICE_ANALISI = ar.CODICE_ANALISI
    and    tda.data_cessazione is null
    AND    tda.FLAG_METALLI_PESANTI  = 'S';
  
    IF nRec > vParMTNM THEN
      SELECT CAST(VALORE AS integer)
      INTO   vParMTPR
	  FROM   PARAMETRO
      WHERE  ID_PARAMETRO = 'MTPR';
	
	  update ANALISI_RICHIESTE ar
	  set    COSTO_ANALISI = COSTO_ANALISI - ((COSTO_ANALISI*vParMTPR)/100)
	  where  ar.ID_RICHIESTA    = idRichiestaNuovo
	  and    ar.CODICE_ANALISI  in (SELECT tda.CODICE_ANALISI
			  					    FROM   TIPI_DI_ANALISI tda
								    WHERE  tda.data_cessazione is null
								    AND    tda.FLAG_METALLI_PESANTI  = 'S');
    END IF;
  END IF;
    

END;
$duplicaAnalisiRichieste$  LANGUAGE plpgsql;

/*
CREATE OR REPLACE FUNCTION DUPLICA_ANALISI_DATI(idRichiesta numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaAnalisiDati$
BEGIN

  INSERT INTO ANALISI_DATI ( ID_RICHIESTA, NOTE )
  SELECT idRichiestaNuovo, NOTE
  FROM ANALISI_DATI
  WHERE ID_RICHIESTA = idRichiesta;

END;
$duplicaAnalisiDati$  LANGUAGE plpgsql;
*/

CREATE OR REPLACE FUNCTION DUPLICA_DATI_CAMPIONE_TERRENO(idRichiesta numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaDatiCampioneTerreno$
BEGIN

  INSERT INTO DATI_CAMPIONE_TERRENO ( ID_RICHIESTA,
    ID_PROFONDITA, NUOVO_IMPIANTO, COLTURA_ATTUALE,
    COLTURA_PREVISTA, ID_VARIETA, ID_INNESTO,
    ANNO_IMPIANTO, ID_SISTEMA_ALLEVAMENTO,
    PRODUZIONE_Q_HA, SUPERFICIE_APPEZZAMENTO,
    GIACITURA, ID_ESPOSIZIONE, SCHELETRO,
    PERCENTUALE_PIETRE, STOPPIE, TIPO_CONCIMAZIONE,
    ID_CONCIME, ID_LAVORAZIONE_TERRENO,
    ID_IRRIGAZIONE, CODICE_MODALITA_COLTIVAZIONE )
  SELECT idRichiestaNuovo,
    ID_PROFONDITA, NUOVO_IMPIANTO, COLTURA_ATTUALE,
    COLTURA_PREVISTA, ID_VARIETA, ID_INNESTO,
    ANNO_IMPIANTO, ID_SISTEMA_ALLEVAMENTO,
    PRODUZIONE_Q_HA, SUPERFICIE_APPEZZAMENTO,
    GIACITURA, ID_ESPOSIZIONE, SCHELETRO,
    PERCENTUALE_PIETRE, STOPPIE, TIPO_CONCIMAZIONE,
    ID_CONCIME, ID_LAVORAZIONE_TERRENO,
    ID_IRRIGAZIONE, CODICE_MODALITA_COLTIVAZIONE
  FROM DATI_CAMPIONE_TERRENO
  WHERE ID_RICHIESTA = idRichiesta;

END;
$duplicaDatiCampioneTerreno$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DUPLICA_CAMPIONE_VEG_ERB(idRichiesta numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaCampioneVegErb$
BEGIN
  INSERT INTO CAMPIONE_VEGETALI_ERBACEE ( ID_RICHIESTA,
    DATA_CAMPIONAMENTO, ID_SPECIE, CAMPIONE_TERRENO )
  SELECT idRichiestaNuovo,
    DATA_CAMPIONAMENTO, ID_SPECIE, CAMPIONE_TERRENO
  FROM CAMPIONE_VEGETALI_ERBACEE
  WHERE ID_RICHIESTA = idRichiesta;

END;
$duplicaCampioneVegErb$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DUPLICA_CAMPIONE_VEG_FOGFRU(idRichiesta numeric,
                                             idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaCampioneVegFogfru$
BEGIN

  INSERT INTO CAMPIONE_VEGETALI_FOGLIEFRUTTA ( ID_RICHIESTA,
    DATA_CAMPIONAMENTO, GIACITURA, SUPERFICIE_APPEZZAMENTO,
	ID_ESPOSIZIONE, SCHELETRO, ALTITUDINE_SLM, ETA_IMPIANTO,
	ID_COLTURA, ID_SPECIE, ALTRA_SPECIE, ID_VARIETA, ID_INNESTO,
	ID_SISTEMA_ALLEVAMENTO, ALTRO_ALLEVAMENTO, SESTO_IMPIANTO_1,
	SESTO_IMPIANTO_2, UNITA_N, UNITA_P2O5, UNITA_K2O, UNITA_MG,
	LETAMAZIONE_ANNO, TIPO_CONCIMAZIONE, ID_CONCIME, ID_STADIO_FENOLOGICO,
	CODICE_PRODUTTIVITA, CAMPIONE_TERRENO )
  SELECT idRichiestaNuovo,
    DATA_CAMPIONAMENTO, GIACITURA, SUPERFICIE_APPEZZAMENTO,
	ID_ESPOSIZIONE, SCHELETRO, ALTITUDINE_SLM, ETA_IMPIANTO,
	ID_COLTURA, ID_SPECIE, ALTRA_SPECIE, ID_VARIETA, ID_INNESTO,
	ID_SISTEMA_ALLEVAMENTO, ALTRO_ALLEVAMENTO, SESTO_IMPIANTO_1,
	SESTO_IMPIANTO_2, UNITA_N, UNITA_P2O5, UNITA_K2O, UNITA_MG,
	LETAMAZIONE_ANNO, TIPO_CONCIMAZIONE, ID_CONCIME, ID_STADIO_FENOLOGICO,
	CODICE_PRODUTTIVITA, CAMPIONE_TERRENO
  FROM CAMPIONE_VEGETALI_FOGLIEFRUTTA
  WHERE ID_RICHIESTA = idRichiesta;

END;
$duplicaCampioneVegFogfru$  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DUPLICA_DATI_FATTURA(idRichiesta numeric,
                                                idRichiestaNuovo numeric)
RETURNS VOID AS $duplicaDatiFattura$
BEGIN

  INSERT INTO DATI_FATTURA
  (id_richiesta,fattura_sn,spedizione,importo_spedizione,fatturare,cf_partita_iva,ragione_sociale,indirizzo,cap,comune,codice_destinatario,PEC)
  SELECT idrichiestanuovo,fattura_sn,spedizione,importo_spedizione,fatturare,cf_partita_iva,ragione_sociale,indirizzo,cap,comune,codice_destinatario,PEC
  FROM   DATI_FATTURA
  WHERE  ID_RICHIESTA = idRichiesta;

END;
$duplicaDatiFattura$  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION duplica_richiesta(idRichiesta numeric,
                                             anagraficaUtente numeric)
RETURNS numeric AS $duplicaRichiesta$
DECLARE
    idRichiestaNuovo numeric;
BEGIN
  SELECT INTO idRichiestaNuovo nextval('ID_RICHIESTA');

  EXECUTE DUPLICA_ETICHETTA_CAMPIONE(idRichiesta, anagraficaUtente, idRichiestaNuovo);
  EXECUTE DUPLICA_FASI_RICHIESTA(idRichiestaNuovo);
  EXECUTE DUPLICA_DATI_APPEZZAMENTO(idRichiesta, idRichiestaNuovo);
  EXECUTE DUPLICA_ANALISI_RICHIESTE(idRichiesta, idRichiestaNuovo);
  EXECUTE DUPLICA_DATI_CAMPIONE_TERRENO(idRichiesta, idRichiestaNuovo);
  EXECUTE DUPLICA_CAMPIONE_VEG_ERB(idRichiesta, idRichiestaNuovo);
  EXECUTE DUPLICA_CAMPIONE_VEG_FOGFRU(idRichiesta, idRichiestaNuovo);
  EXECUTE DUPLICA_DATI_FATTURA(idRichiesta, idRichiestaNuovo);

  return idRichiestaNuovo;

END;
$duplicaRichiesta$  LANGUAGE plpgsql;

GRANT EXECUTE
ON FUNCTION duplica_richiesta(idRichiesta numeric, anagraficaUtente numeric)
TO agrichim_rw;