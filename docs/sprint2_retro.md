# Sprint 2 — retrospective

2026-10-19. Each figure comes from the repository or a dated run.

## Ce que j'ai appris (10 points)

1. **Un nom de fichier n'est pas une preuve de date.** Trois snapshots du 2026-10-11 portaient l'heure
   prévue du run, mais contenaient le flux capturé au réveil du PC, 3 à 14 h plus tard. Réponse en trois
   couches : quarantaine des fichiers et rechargement du jour, garde-fou à la capture (skip au-delà de
   240 s), test dbt `assert_snapshot_matches_feed_time` (erreur au-delà de 300 s), validé par une
   contre-épreuve sur les fichiers en quarantaine (20 861 s, 10 765 s, 50 455 s).
2. **Un test qui passe doit savoir échouer.** Le test unitaire de `hour_of_day_local` passe avec
   `Europe/Paris` et échoue avec `Europe/London` ; le skip du garde-fou a été prouvé par un vrai run au
   `run_after` ancien, pas seulement par des tests unitaires.
3. **L'état d'un run Airflow vient de ses tâches finales.** Avec `trigger_rule="all_done"`, une fraîcheur
   en erreur suivie d'un build réussi laissait le run vert. Le watcher (`one_failed`) rend l'échec
   visible sans bloquer les marts.
4. **Une option par défaut peut casser un run réussi.** Le 2026-10-19, un `dbt build` réussi a été marqué
   en échec : écriture XCom lente (8,8 s), renvoyée par le client, doublon (409). La sortie n'étant pas
   utilisée, `do_xcom_push=False` supprime le problème à la racine.
5. **DuckDB n'accepte qu'un écrivain, et refuse même un lecteur pendant une écriture.** Le pool
   `duckdb_writer` (1 slot) sérialise chargements et dbt ; prouvé le 2026-10-19 : 8 tâches, 0
   chevauchement.
6. **Stocker en UTC, lire en heure locale.** `hour_ts` reste UTC ; `hour_of_day_local` suit l'heure de
   Paris, vérifié sur le passage à l'heure d'hiver du 2026-10-25 (00:00 et 01:00 UTC donnent tous deux 2).
7. **Le grain se déclare et se teste.** `fct_daily_coverage` a pour grain (`coverage_date`, `source`) ;
   l'hypothèse « une seule source par jour » est un test séparé (`assert_sources_do_not_overlap`) au lieu
   d'être cachée dans la clé.
8. **Garder l'historique d'une référence rend les changements visibles.** `station_information` est
   chargée par jour : la station 40010 est passée d'une capacité de 21 à 0 le 2026-10-13.
9. **Un pourcentage ne se ré-agrège pas.** Le fait horaire garde `bins_empty` / `bins_full` à côté des
   pourcentages ; les créneaux 0 vélo / 0 place sont comptés à part (`bins_closed`).
10. **Documenter la cause d'un trou avec une preuve.** Chaque trou de collecte du README a une source :
    journal Windows (veille, batterie, redémarrage) ou log Airflow ; la coupure du 2026-10-18 (14:25–14:30)
    est une erreur DNS sur secteur, pas une veille.
