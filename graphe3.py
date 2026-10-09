import matplotlib.pyplot as plt
import pandas as pd

df_stateless = pd.read_csv('summary_STATELESS.csv')
df_stateful = pd.read_csv('summary_STATEFUL.csv')

plt.figure(figsize=(9, 5))
plt.plot(
    df_stateless['seconde'],
    df_stateless['heap_moyen_mo'],
    label='Stateless',
    color='#1f77b4',
    linewidth=1.5,
)
plt.plot(
    df_stateful['seconde'],
    df_stateful['heap_moyen_mo'],
    label='Stateful',
    color='#d62728',
    linewidth=1.5,
    alpha=0.85,
)
plt.title(
    'Consommation Mémoire JVM (Heap en Mo)',
    fontsize=14,
    fontweight='bold',
    pad=14,
)
plt.xlabel('Temps (secondes)', fontsize=14, labelpad=10)
plt.ylabel('Mémoire (Mo)', fontsize=14, labelpad=10)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(loc='upper right')
plt.tight_layout()
plt.savefig('graphe_memoire_heap.png', dpi=300)
plt.show()
