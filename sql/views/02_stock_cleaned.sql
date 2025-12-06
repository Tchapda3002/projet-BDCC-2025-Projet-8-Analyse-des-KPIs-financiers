-- Vue : Nettoyage du stock des unités légales

  -- Vue : Nettoyage du stock des unités légales
CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_stock_cleaned` 
AS
WITH
  first_element AS (
    SELECT
      sl.siren,
      sl.nomUniteLegale,
      sl.categorieEntreprise,
      sl.etatAdministratifUniteLegale,
      sl.categorieJuridiqueUniteLegale,
      sl.activitePrincipaleUniteLegale,
      sl.nomenclatureActivitePrincipaleUniteLegale,
      sl.economieSocialeSolidaireUniteLegale,
      sl.trancheEffectifsUniteLegale,
      sl.extraction_timestamp,
      sl.extraction_date
      
    FROM
       `{project_id}.{dataset}.stock_entreprises_raw` AS sl
    WHERE
      sl.siren IS NOT NULL
      AND sl.trancheEffectifsUniteLegale IS NOT NULL
      AND sl.categorieEntreprise IS NOT NULL
      AND sl.etatAdministratifUniteLegale IS NOT NULL
      AND sl.categorieJuridiqueUniteLegale IS NOT NULL
      AND sl.activitePrincipaleUniteLegale IS NOT NULL
      AND sl.nomenclatureActivitePrincipaleUniteLegale IS NOT NULL
      AND sl.economieSocialeSolidaireUniteLegale IS NOT NULL
      {timestamp_filter}
  ),
  second_element AS (
    SELECT
      t.*,
      t.categorieJuridiqueUniteLegale AS categorie_juridique_niveau_III,
      COALESCE(
        cj.libelle_niveau_I, CAST(t.categorieJuridiqueUniteLegale AS STRING))
        AS categorie_juridique_niveau_I
    FROM `first_element` AS t
    LEFT JOIN
      UNNEST(
        [
          STRUCT(
            "0" AS code_niveau_I,
            "Organisme de placement collectif en valeurs mobilières sans personnalité morale"
              AS libelle_niveau_I),
          STRUCT("1", "Entrepreneur individuel"),
          STRUCT(
            "2",
            "Groupement de droit privé non doté de la personnalité morale"),
          STRUCT("3", "Personne morale de droit étranger"),
          STRUCT(
            "4", "Personne morale de droit public soumise au droit commercial"),
          STRUCT("5", "Société commerciale"),
          STRUCT("6", "Autre personne morale immatriculée au RCS"),
          STRUCT(
            "7", "Personne morale et organisme soumis au droit administratif"),
          STRUCT("8", "Organisme privé spécialisé"),
          STRUCT("9", "Groupement de droit privé")])
        AS cj
      ON
        SUBSTR(CAST(t.categorieJuridiqueUniteLegale AS STRING), 1, 1)
        = cj.code_niveau_I
  ),
  third_element AS (
    SELECT
      t.*,
      t.activitePrincipaleUniteLegale
        AS original_activite_principale_unite_legale,
      COALESCE(am.description, t.activitePrincipaleUniteLegale)
        AS recoded_activite_principale_unite_legale
    FROM `second_element` AS t
    LEFT JOIN
      UNNEST(
        [
          STRUCT(
            "01" AS code,
            "A" AS section,
            "Culture et production animale, chasse et services annexes"
              AS description),
          STRUCT("02", "A", "Sylviculture et exploitation forestière"),
          STRUCT("03", "A", "Pêche et aquaculture"),
          STRUCT("05", "B", "Extraction de charbon"),
          STRUCT("06", "B", "Extraction de pétrole brut et de gaz naturel"),
          STRUCT("07", "B", "Extraction de minerais métalliques"),
          STRUCT("08", "B", "Autres activités extractives"),
          STRUCT("09", "B", "Services de soutien aux industries extractives"),
          STRUCT("10", "C", "Industries alimentaires"),
          STRUCT("11", "C", "Fabrication de boissons"),
          STRUCT("12", "C", "Fabrication de produits à base de tabac"),
          STRUCT("13", "C", "Fabrication de textiles"),
          STRUCT("14", "C", "Industrie de l''habillement"),
          STRUCT("15", "C", "Industrie du cuir et de la chaussure"),
          STRUCT(
            "16",
            "C",
            "Travail du bois et fabrication d'articles en bois et en liège"),
          STRUCT("17", "C", "Industrie du papier et du carton"),
          STRUCT("18", "C", "Imprimerie et reproduction d''enregistrements"),
          STRUCT("19", "C", "Cokéfaction et raffinage"),
          STRUCT("20", "C", "Industrie chimique"),
          STRUCT("21", "C", "Industrie pharmaceutique"),
          STRUCT(
            "22", "C", "Fabrication de produits en caoutchouc et en plastique"),
          STRUCT(
            "23",
            "C",
            "Fabrication d''autres produits minéraux non métalliques"),
          STRUCT("24", "C", "Métallurgie"),
          STRUCT("25", "C", "Fabrication de produits métalliques"),
          STRUCT(
            "26",
            "C",
            "Fabrication de produits informatiques, électroniques et optiques"),
          STRUCT("27", "C", "Fabrication d''équipements électriques"),
          STRUCT("28", "C", "Fabrication de machines et équipements n.c.a."),
          STRUCT("29", "C", "Industrie automobile"),
          STRUCT("30", "C", "Fabrication d''autres matériels de transport"),
          STRUCT("31", "C", "Fabrication de meubles"),
          STRUCT("32", "C", "Autres industries manufacturières"),
          STRUCT(
            "33",
            "C",
            "Réparation et installation de machines et d''équipements"),
          STRUCT(
            "35",
            "D",
            "Production et distribution d''électricité, de gaz, de vapeur et d''air conditionné"),
          STRUCT("36", "E", "Captage, traitement et distribution d''eau"),
          STRUCT("37", "E", "Collecte et traitement des eaux usées"),
          STRUCT(
            "38",
            "E",
            "Collecte, traitement et élimination des déchets ; récupération"),
          STRUCT(
            "39",
            "E",
            "Dépollution et autres services de gestion des déchets"),
          STRUCT("41", "F", "Construction de bâtiments"),
          STRUCT("42", "F", "Génie civil"),
          STRUCT("43", "F", "Travaux de construction spécialisés"),
          STRUCT(
            "45",
            "G",
            "Commerce et réparation d''automobiles et de motocycles"),
          STRUCT("46", "G", "Commerce de gros"),
          STRUCT("47", "G", "Commerce de détail"),
          STRUCT("49", "H", "Transports terrestres et transport par conduites"),
          STRUCT("50", "H", "Transports par eau"),
          STRUCT("51", "H", "Transports aériens"),
          STRUCT(
            "52", "H", "Entreposage et services auxiliaires des transports"),
          STRUCT("53", "H", "Activités de poste et de courrier"),
          STRUCT("55", "I", "Hébergement"),
          STRUCT("56", "I", "Restauration"),
          STRUCT("58", "J", "Édition"),
          STRUCT(
            "59", "J", "Production de films, de vidéo et de télévision"),
          STRUCT("60", "J", "Programmation et diffusion"),
          STRUCT("61", "J", "Télécommunications"),
          STRUCT(
            "62",
            "J",
            "Programmation, conseil et autres activités informatiques"),
          STRUCT("63", "J", "Services d''information"),
          STRUCT("64", "K", "Activités des services financiers"),
          STRUCT("65", "K", "Assurance"),
          STRUCT(
            "66",
            "K",
            "Activités auxiliaires de services financiers et d''assurance"),
          STRUCT("68", "L", "Activités immobilières"),
          STRUCT("69", "M", "Activités juridiques et comptables"),
          STRUCT(
            "70", "M", "Activités des sièges sociaux ; conseil de gestion"),
          STRUCT("71", "M", "Activités d''architecture et d''ingénierie"),
          STRUCT("72", "M", "Recherche-développement scientifique"),
          STRUCT("73", "M", "Publicité et études de marché"),
          STRUCT(
            "74",
            "M",
            "Autres activités spécialisées, scientifiques et techniques"),
          STRUCT("75", "M", "Activités vétérinaires"),
          STRUCT("77", "N", "Activités de location et location-bail"),
          STRUCT("78", "N", "Activités liées à l''emploi"),
          STRUCT("79", "N", "Activités des agences de voyage"),
          STRUCT("80", "N", "Enquêtes et sécurité"),
          STRUCT(
            "81",
            "N",
            "Services relatifs aux bâtiments et aménagement paysager"),
          STRUCT(
            "82",
            "N",
            "Activités administratives et autres activités de soutien aux entreprises"),
          STRUCT(
            "84",
            "O",
            "Administration publique et défense ; sécurité sociale obligatoire"),
          STRUCT("85", "P", "Enseignement"),
          STRUCT("86", "Q", "Activités pour la santé humaine"),
          STRUCT("87", "Q", "Hébergement médico-social et social"),
          STRUCT("88", "Q", "Action sociale sans hébergement"),
          STRUCT(
            "90", "R", "Activités créatives, artistiques et de spectacle"),
          STRUCT(
            "91",
            "R",
            "Bibliothèques, archives, musées et autres activités culturelles"),
          STRUCT("92", "R", "Organisation de jeux de hasard et d''argent"),
          STRUCT(
            "93", "R", "Activités sportives, récréatives et de loisirs"),
          STRUCT("94", "S", "Activités des organisations associatives"),
          STRUCT(
            "95",
            "S",
            "Réparation d''ordinateurs et de biens personnels et domestiques"),
          STRUCT("96", "S", "Autres services personnels"),
          STRUCT(
            "97",
            "T",
            "Activités des ménages employeurs de personnel domestique"),
          STRUCT("98", "T", "Activités indifférenciées des ménages"),
          STRUCT("99", "U", "Activités des organisations extraterritoriales")])
        AS am
      ON SUBSTR(t.activitePrincipaleUniteLegale, 1, 2) = am.code
  ),
  fourth_element AS (
    SELECT
      t.*,
      CASE
        WHEN t.trancheEffectifsUniteLegale IN ("00") THEN "Sans salarié"
        WHEN t.trancheEffectifsUniteLegale IN ("01", "02", "03")
          THEN "1 à 9 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("11", "12")
          THEN "10 à 49 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("21", "22")
          THEN "50 à 199 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("31", "32")
          THEN "200 à 499 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("41", "42")
          THEN "500 à 1 999 salariés"
        WHEN t.trancheEffectifsUniteLegale IN ("51", "52", "53")
          THEN "2000 salariés et plus"
        ELSE "Non renseigné"
        END
        AS categorie_effectif
    FROM `third_element` AS t
  ),
  fifth_element AS (
    SELECT
      t.*,
      CASE t.categorieEntreprise
        WHEN "PME" THEN "Petites et Moyennes Entreprises"
        WHEN "ETI" THEN "Entreprises de Taille Intermédiaire"
        WHEN "GE" THEN "Grandes Entreprises"
        ELSE t.categorieEntreprise
        END
        AS categorie_entreprise_recodee,
      CASE t.economieSocialeSolidaireUniteLegale
        WHEN "N" THEN "NON"
        WHEN "O" THEN "OUI"
        ELSE t.economieSocialeSolidaireUniteLegale
        END
        AS ess_recodee,
      CASE t.etatAdministratifUniteLegale
        WHEN "A" THEN "ACTIF"
        WHEN "C" THEN "CESSE"
        ELSE t.etatAdministratifUniteLegale
        END
        AS etat_administratif_recoded
    FROM `fourth_element` AS t
  )
SELECT
  sl.siren,
  sl.nomUniteLegale,
  sl.categorie_juridique_niveau_I,
  sl.recoded_activite_principale_unite_legale,
  sl.original_activite_principale_unite_legale,
  sl.categorie_effectif,
  sl.categorie_entreprise_recodee,
  sl.ess_recodee,
  sl.etat_administratif_recoded,
  sl.extraction_timestamp,
  sl.extraction_date
FROM `fifth_element` AS sl
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY siren ORDER BY extraction_timestamp DESC) = 1;
