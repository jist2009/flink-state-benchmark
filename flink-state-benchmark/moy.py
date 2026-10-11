import glob
import os
import pandas as pd


def agreger_et_nettoyer(mode="STATELESS"):
    fichiers = sorted(glob.glob(f"metrics_{mode}_subtask_*.csv"))

    if not fichiers:
        print(f"Aucun fichier trouvé pour {mode}.")
        return None

    # Lecture et agrégation
    df = pd.concat((pd.read_csv(f) for f in fichiers), ignore_index=True)
    resume = (
        df.groupby("seconde")
        .agg(
            debit_total_rec_s=("debit_rec_s", "sum"),
            p50_moyen_ms=("p50_ms", "mean"),
            p99_moyen_ms=("p99_ms", "mean"),
            heap_moyen_mo=("heap_mo", "mean"),
        )
        .reset_index()
    )

    # Sauvegarde du fichier consolidé
    sortie = f"summary_{mode}.csv"
    resume.to_csv(sortie, index=False)
    print(f"Fichier créé : {sortie}")

    # Suppression des 16 fichiers sources
    for f in fichiers:
        os.remove(f)
    print(f"{len(fichiers)} fichiers intermédiaires supprimés pour {mode}.")

    return resume


# Exécution
agreger_et_nettoyer("QUERY_2_SELECTION")
#agreger_et_nettoyer("STATELESS")
#agreger_et_nettoyer("STATEFUL")
