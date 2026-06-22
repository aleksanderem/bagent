"""Silnik cenowy oparty na podobieństwie embeddingów (similarity-first pricing).

Zamiast klasyfikować usługi do taksonomii (podatne na błędy aliasów),
wyszukujemy bliźniaków przez cosinus embeddingów i z ich cen obliczamy
cenę rynkową. Moduł jest budowany warstwowo:

  layer_dedup   — deduplikacja klastra per salon (ta biblioteka)
  layer_stats   — statystyki cenowe na zdeduplikowanym klastrze (TODO)
  layer_context — budowanie market_context dla raportu (TODO)
"""
