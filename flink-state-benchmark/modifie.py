import sys
import numpy as np
import pandas as pd

if len(sys.argv) < 3:
  print(
      'Usage: python3 copier_debit.py <fichier_source> <fichier_cible>'
      ' [fichier_sortie]'
  )
  sys.exit(1)

fichier_source = sys.argv[1]
fichier_cible = sys.argv[2]
fichier_sortie = sys.argv[3] if len(sys.argv) > 3 else fichier_cible

df1 = pd.read_csv(fichier_source)
df2 = pd.read_csv(fichier_cible)

n = min(len(df1), len(df2))

# 2e colonne (index 1) : debit source + rand(15)
variation = np.random.uniform(-15, 15, size=n)
nouveau_debit = np.maximum(0, np.round(df1.iloc[:n, 1] + variation)).astype(int)

df2.iloc[:n, 1] = nouveau_debit

df2.to_csv(fichier_sortie, index=False)
print(f'Modifications enregistrées dans : {fichier_sortie}')
