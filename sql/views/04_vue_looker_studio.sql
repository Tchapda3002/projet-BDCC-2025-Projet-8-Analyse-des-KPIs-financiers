CREATE OR REPLACE VIEW `{project_id}.{dataset}.v_looker_studio` AS
SELECT
    s.siren,
    s.nomUniteLegale,
    s.categorie_juridique_niveau_I,
    s.recoded_activite_principale_unite_legale,
    s.original_activite_principale_unite_legale,
    s.categorie_effectif,
    s.categorie_entreprise_recodee,
    s.ess_recodee,
    s.etat_administratif_recoded,
    s.extraction_date,

    r.date_cloture_exercice,
    r.chiffre_d_affaires,
    r.marge_brute,
    r.ebe,
    r.ebit,
    r.resultat_net,
    r.taux_d_endettement,
    r.ratio_de_liquidite,
    r.ratio_de_vetuste,
    r.autonomie_financiere,
    r.poids_bfr_exploitation_sur_ca,
    r.couverture_des_interets,
    r.caf_sur_ca,
    r.capacite_de_remboursement,
    r.marge_ebe,
    r.resultat_courant_avant_impots_sur_ca,
    r.poids_bfr_exploitation_sur_ca_jours,
    r.rotation_des_stocks_jours,
    r.credit_clients_jours,
    r.credit_fournisseurs_jours,
    r.type_bilan,
    r.confidentiality,
    r.extraction_timestamp

FROM `{project_id}.{dataset}.v_ratios_cleaned` AS r
INNER JOIN `{project_id}.{dataset}.v_stock_cleaned` AS s
    ON s.siren = r.siren;
